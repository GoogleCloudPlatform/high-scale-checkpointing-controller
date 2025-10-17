// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deploy_test

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	informerv1 "k8s.io/client-go/informers/core/v1"
	lister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	sliceLabel = "kind"
	sliceValue = "checkpoint"
	sliceTaint = "checkpoint"
)

// getTestSlices returns a map of node pool name to nodes in that pool, for nodes
// matching the slice test label.
func getTestSlices(ctx context.Context, t *testing.T) map[string][]string {
	t.Helper()
	nodes, err := K8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("couldn't list nodes: %v", err)
	}
	pools := map[string][]string{}
	for _, node := range nodes.Items {
		labels := node.GetLabels()
		if labels == nil {
			continue
		}
		val, found := labels[sliceLabel]
		if found && val == sliceValue {
			poolName, found := labels[nodePoolLabel]
			if !found {
				t.Fatalf("node missing node pool label: %s", node.GetName())
			}
			pool, found := pools[poolName]
			if !found {
				pool = []string{}
			}
			pool = append(pool, node.GetName())
			pools[poolName] = pool
		}
	}
	return pools
}

// createTestPool creates a node pool that simulates an e2e slice. The pool uses small
// disks and max pods per node counts in order to be easily used for scale tests. The
// small disk size makes the test easier on PD quota, and low max pods per node is
// necessary to prevent IP address exhaustion.
func createTestPool(ctx context.Context, t *testing.T, sliceSize int) string {
	t.Helper()
	return createUniqueNodePool(ctx, MachineType("e2-small"), NumNodes(sliceSize), NodePoolLabel(fmt.Sprintf("%s=%s", sliceLabel, sliceValue)), NodePoolTaint(sliceTaint), DiskSizeGb(20), MaxPodsPerNode(12))
}

type scaleTestParams struct {
	slices        int
	nodesPerSlice int
	workerImage   string
}

type scaleTestServer struct {
	informer  informerv1.ConfigMapInformer
	lister    lister.ConfigMapLister
	closer    chan struct{}
	available chan struct{}
}

func getScaleTestParams(t *testing.T) *scaleTestParams {
	t.Helper()
	params := func() *scaleTestParams {
		env := os.Getenv("SCALE_TEST")
		if env == "" {
			return nil
		}
		img := os.Getenv("SCALE_TEST_IMAGE")
		if img == "" {
			t.Fatalf("Missing env SCALE_TEST_IMAGE")
		}
		parts := strings.Split(env, "x")
		if len(parts) != 2 {
			t.Logf("Unparsable SCALE_TEST=%s", env)
			return nil
		}
		slices, err := strconv.Atoi(parts[0])
		if err != nil {
			t.Logf("Unparsable SCALE_TEST=%s: %v", env, err)
			return nil
		}
		nodesPerSlice, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Logf("Unparsable SCALE_TEST=%s: %v", env, err)
			return nil
		}
		params := scaleTestParams{
			slices:        slices,
			nodesPerSlice: nodesPerSlice,
			workerImage:   img,
		}
		t.Logf("Using scale test %+v", params)
		verifyTimeout = 30 * time.Minute
		waitTimeout = 15 * time.Minute // deleting lots of pods can be slow
		return &params
	}()
	if params == nil {
		t.Skip("Skipping scale test. Pass env vars SCALE_TEST=${slices}x${size} SCALE_TEST_IMAGE=image")
	}
	return params
}

func startScaleTestServer(t *testing.T) *scaleTestServer {
	t.Helper()
	factory := informers.NewSharedInformerFactoryWithOptions(K8sClient, 60*time.Minute, informers.WithNamespace(testNamespace))
	informer := factory.Core().V1().ConfigMaps()
	svr := &scaleTestServer{
		informer:  informer,
		lister:    informer.Lister(),
		closer:    make(chan struct{}),
		available: make(chan struct{}, 1),
	}
	_, err := informer.Informer().AddEventHandler(&cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o interface{}) { svr.setAvailable() },
		UpdateFunc: func(_, _ interface{}) { svr.setAvailable() },
	})
	assert.NilError(t, err)

	stopCh := make(chan struct{})
	factory.Start(stopCh)
	t.Logf("starting configmap cache sync at %v", time.Now())
	if !cache.WaitForCacheSync(stopCh, informer.Informer().HasSynced) {
		t.Fatal("Cannot sync caches")
	}
	t.Logf("Cache synced at %v", time.Now())

	svr.setAvailable()
	return svr
}

func (svr *scaleTestServer) close() {
	close(svr.closer)
}

func (svr *scaleTestServer) setAvailable() {
	select {
	case svr.available <- struct{}{}:
	default:
	}
}

func (svr *scaleTestServer) hasAvailable(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-svr.available:
	}
	return nil
}

// verify checks that process ranks are consistent. The kubectl exec used in the
// deploy_tests takes a very long time to run at scale, so instead the job runs
// the scale test workers that write information to configmaps that can all be
// pulled at once. The scale test worker will also allow instrumentation to get
// better performance tracking. For now performance is tracked by the worker
// putting timestamps into its configmap.
//
// The function returns a mapping node -> rank.
func (svr *scaleTestServer) verify(ctx context.Context, t *testing.T, numWorkers int) map[string]int {
	t.Helper()
	t.Logf("%v: starting verification", time.Now())
	var nodeToIndex map[string]int
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 120*time.Minute, true, func(ctx context.Context) (bool, error) {
		now := time.Now()
		if err := svr.hasAvailable(ctx); err != nil {
			return false, err
		}

		maps, err := svr.lister.ConfigMaps(testNamespace).List(labels.Everything())
		if err != nil {
			return false, err
		}
		indexToCoord := map[int]string{}
		indexToPod := map[int]string{}
		nodeToIndex = map[string]int{}
		coordinator := ""
		var firstIdx, lastIdx time.Time
		var firstStart, lastStart time.Time
		var minDelta, maxDelta time.Duration
		errors := []string{}
		for _, m := range maps {
			if !strings.HasPrefix(m.GetName(), "worker-") {
				continue
			}
			if m.Data == nil {
				t.Logf("%v: missing data for %s", now, m.GetName())
				return false, nil // retry
			}
			idx, err := strconv.Atoi(m.Data["index"])
			if err != nil {
				errors = append(errors, fmt.Sprintf("Bad index in %s: %s", m.GetName(), m.Data["index"]))
				continue
			}
			coord := m.Data["coordinator"]
			ip := m.Data["ip"]
			startTs, err := time.Parse(time.RFC3339Nano, m.Data["start-time"])
			if err != nil {
				errors = append(errors, fmt.Sprintf("Bad timestamp in %s: %s", m.GetName(), m.Data["start-time"]))
				continue
			}
			idxTs, err := time.Parse(time.RFC3339Nano, m.Data["idx-time"])
			if err != nil {
				errors = append(errors, fmt.Sprintf("Bad timestamp in %s: %s", m.GetName(), m.Data["idx-time"]))
				continue
			}
			if firstStart.IsZero() || startTs.Before(firstStart) {
				firstStart = startTs
			}
			if lastStart.IsZero() || startTs.After(lastStart) {
				lastStart = startTs
			}
			if firstIdx.IsZero() || idxTs.Before(firstIdx) {
				firstIdx = idxTs
			}
			if lastIdx.IsZero() || idxTs.After(lastIdx) {
				lastIdx = idxTs
			}

			delta := idxTs.Sub(startTs)
			if minDelta == time.Duration(0) || delta < minDelta {
				minDelta = delta
			}
			if maxDelta == time.Duration(0) || delta > maxDelta {
				maxDelta = delta
			}

			if _, found := indexToCoord[idx]; found != false {
				errors = append(errors, fmt.Sprintf("Duplicate index for %d in %s by %s", idx, m.GetName(), indexToPod[idx]))
				continue
			}
			indexToCoord[idx] = coord
			indexToPod[idx] = m.GetName()
			nodeToIndex[m.Data["node"]] = idx
			if idx == 0 {
				coordinator = ip
			}
		}
		t.Logf("%v: first start: %v", now, firstStart)
		t.Logf("%v: last start: %v", now, lastStart)
		t.Logf("%v: first idx: %v", now, firstIdx)
		t.Logf("%v: last idx: %v", now, lastIdx)
		t.Logf("%v: min/max delta: %v %v", now, minDelta, maxDelta)
		t.Logf("%v: last idx - last start: %v", now, lastIdx.Sub(lastStart))
		if len(errors) > 0 {
			t.Logf("%v: errors in collecting configmaps", now)
			for _, err := range errors {
				t.Log(err)
				return false, nil // retry
			}
		}
		if len(indexToPod) != numWorkers {
			t.Logf("%v: Found %d of %d unique indicies", now, len(indexToCoord), numWorkers)
			return false, nil // retry
		}
		if coordinator == "" {
			t.Logf("%v: missing coordinator", now)
			return false, nil // retry
		}
		for i := 0; i < numWorkers; i++ {
			coord, found := indexToCoord[i]
			if !found {
				t.Logf("%v: missing index %d", time.Now(), i)
				return false, nil // retry
			}
			if coord != coordinator+":8476" {
				t.Logf("%v: coordinator mismatch %d (%s vs %s)", time.Now(), i, coord, coordinator)
				return false, nil // retry
			}
		}

		t.Logf("%v: verification succeeded!", now)
		return true, nil
	})
	assert.NilError(t, err)
	return nodeToIndex
}

func (svr *scaleTestServer) deleteMaps(ctx context.Context, t *testing.T) {
	t.Logf("%v: deleting worker configmaps", time.Now())
	maps, err := svr.lister.ConfigMaps(testNamespace).List(labels.Everything())
	assert.NilError(t, err)
	cnt := 0
	for _, m := range maps {
		if strings.HasPrefix(m.GetName(), "worker-") {
			cnt++
			err := wait.PollUntilContextTimeout(ctx, time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
				err := K8sClient.CoreV1().ConfigMaps(testNamespace).Delete(ctx, m.GetName(), metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					t.Logf("retrying configmap delete: %v", err)
					return false, nil
				}
				return true, nil
			})
			assert.NilError(t, err)
		}
	}
	t.Logf("%v: %d maps deleted", time.Now(), cnt)
}

// initializeTestSlices ensures the cluster has test slice node pools that exactly match
// numSlices and sliceSize. It parallelizes what it can to be efficient for scale testing.
func initializeTestSlices(ctx context.Context, t *testing.T, numSlices, sliceSize int) {
	t.Helper()

	pools := getTestSlices(ctx, t)
	deleted := []string{}
	var wg sync.WaitGroup
	for pool, nodes := range pools {
		if len(nodes) != sliceSize {
			deleted = append(deleted, pool)
			poolToDelete := pool
			wg.Add(1)
			go func() {
				defer wg.Done()
				deleteNodePool(ctx, poolToDelete)
			}()
		}
	}
	for _, p := range deleted {
		delete(pools, p)
	}
	wg.Wait()

	t.Logf("initial valid pools: %v", slices.Collect(maps.Keys(pools)))
	if len(pools) < numSlices {
		t.Logf("Creating %d new pools", numSlices-len(pools))
		wg = sync.WaitGroup{}
		for i := 0; i < numSlices-len(pools); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				createTestPool(ctx, t, sliceSize)
			}()
		}
		wg.Wait()
	} else if numSlices < len(pools) {
		cnt := len(pools) - numSlices
		t.Logf("Deleting %d unneeded pools", cnt)
		wg = sync.WaitGroup{}
		for pool := range pools {
			poolToDelete := pool
			wg.Add(1)
			go func() {
				defer wg.Done()
				deleteNodePool(ctx, poolToDelete)
			}()
			cnt--
			if cnt == 0 {
				break
			}
		}
		wg.Wait()
	}
}
