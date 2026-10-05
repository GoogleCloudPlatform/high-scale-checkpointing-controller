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
	"io"
	"log"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	informerv1 "k8s.io/client-go/informers/core/v1"
	lister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	sliceLabel = "kind"
	sliceValue = "checkpoint"
	sliceTaint = "checkpoint"

	// useDataParallelism and following are options for setEmulatorConfig.
	useDataParallelism = "data-parallel"
	useCubePeers       = "cube-peers"
	waitForDisruption  = "wait-for-disruption"

	mtcCsiDriverName = "multitier-checkpoint.csi.storage.gke.io"
)

// getTestSlices returns a map of node pool name to nodes in that pool, for nodes
// matching the slice test label.
func getTestSlices(ctx context.Context, t *testing.T, opts ...NodePoolOption) map[string][]string {
	t.Helper()

	var options NodePoolOptions
	for _, o := range opts {
		o.Update(&options)
	}
	targetLabel := sliceLabel
	targetValue := sliceValue
	if options.label != "" {
		parts := strings.Split(options.label, "=")
		if len(parts) != 2 {
			t.Fatalf("bad node pool label %s", options.label)
		}
		targetLabel = parts[0]
		targetValue = parts[1]
	}

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
		val, found := labels[targetLabel]
		if found && val == targetValue {
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
		} else {
			t.Logf("skipping %s as %s=%s doesn't match %s", node.GetName(), targetLabel, val, targetValue)
		}
	}
	return pools
}

// createTestPool creates a node pool that simulates an e2e slice. The pool uses small
// disks and max pods per node counts in order to be easily used for scale tests. The
// small disk size makes the test easier on PD quota, and low max pods per node is
// necessary to prevent IP address exhaustion.
func createTestPool(ctx context.Context, t *testing.T, sliceSize int, opts ...NodePoolOption) string {
	t.Helper()
	// Note opts will override any of the defaults set here.
	pool_opts := []NodePoolOption{MachineType("e2-small"), NumNodes(sliceSize), NodePoolLabel(fmt.Sprintf("%s=%s", sliceLabel, sliceValue)), NodePoolTaint(sliceTaint), DiskSizeGb(20), MaxPodsPerNode(14)}
	pool_opts = append(pool_opts, opts...)
	return createUniqueNodePool(ctx, pool_opts...)
}

type scaleTestParams struct {
	slices        int
	nodesPerSlice int
	workerImage   string
	replicaSize   int // Used for superslice only.
}

type scaleTestServer struct {
	informer  informerv1.ConfigMapInformer
	lister    lister.ConfigMapLister
	closer    chan struct{}
	available chan struct{}
}

func getScaleTestParams(t *testing.T) *scaleTestParams {
	t.Helper()
	t.Logf("%v: getting scale test params", time.Now())
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

func getSupersliceTestParams(t *testing.T) *scaleTestParams {
	t.Helper()
	t.Logf("%v: getting superslice params", time.Now())
	params := func() *scaleTestParams {
		topology := os.Getenv("SUPERSLICE_TOPOLOGY")
		if topology == "" {
			return nil
		}
		img := os.Getenv("SCALE_TEST_IMAGE")
		if img == "" {
			t.Fatal("Missing env SCALE_TEST_IMAGE")
		}
		parts := strings.Split(topology, "x")
		if len(parts) != 3 {
			t.Logf("Unparsable SUPERSLICE_TOPOLOGY=%s", topology)
			return nil
		}
		count := 1
		for _, p := range parts {
			c, err := strconv.Atoi(p)
			if err != nil {
				t.Logf("Unparsable SUPERSLICE_TOPOLOGY=%s: %v", topology, err)
				return nil
			}
			count *= c
		}
		if (count < 16 || count%64 != 0) && topology != "2x4x1" {
			t.Logf("expected full cubes (at least 4x4x4 and multiple of 64) or special 2x4x1 in SUPERSLICE_TOPOLOGY=%s", topology)
			return nil
		}
		params := scaleTestParams{
			workerImage: img,
		}
		if topology == "2x4x1" {
			params.slices = 2
			params.nodesPerSlice = 4
			params.replicaSize = 2 // Data parallelism of 4.
		} else {
			cubes := count / 64
			params.slices = cubes
			params.nodesPerSlice = 16
			params.replicaSize = cubes * 16 / 4 // Data parallelism of 4.
		}
		t.Logf("Using scale test %+v", params)
		verifyTimeout = 30 * time.Minute
		waitTimeout = 15 * time.Minute // deleting lots of pods can be slow
		return &params
	}()
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
func initializeTestSlices(ctx context.Context, t *testing.T, numSlices, sliceSize int, opts ...NodePoolOption) {
	t.Helper()

	pools := getTestSlices(ctx, t, opts...)
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
				createTestPool(ctx, t, sliceSize, opts...)
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

// waitForDaemonset considers nodes as in initializeTestSlices and waits for the CSINode to be ready with the MTC driver.
func waitForDaemonset(ctx context.Context, t *testing.T, numSlices, sliceSize int, opts ...NodePoolOption) {
	t.Helper()
	pools := getTestSlices(ctx, t, opts...)
	for pool := range pools {
		for _, node := range pools[pool] {
			err := wait.PollUntilContextTimeout(ctx, time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
				var csinode storagev1.CSINode
				if err := CRClient.Get(ctx, types.NamespacedName{Name: node}, &csinode); err != nil {
					t.Logf("Couldn't get CSINode %s: %v", node, err)
					return false, nil
				}
				for _, driver := range csinode.Spec.Drivers {
					if driver.Name == mtcCsiDriverName {
						t.Logf("Found MTC driver on %s", node)
						return true, nil
					}
				}
				// retry
				return false, nil
			})
			if err != nil {
				t.Fatalf("Timeout getting mtc driver for %s", node)
			}
		}
	}
}

func uploadObject(ctx context.Context, bucket, path, data string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	wc := client.Bucket(bucket).Object(path).NewWriter(ctx)
	wc.ContentType = "text/plain"
	if _, err := wc.Write([]byte(data)); err != nil {
		return err
	}
	return wc.Close()
}

func readObject(ctx context.Context, bucket, path string) ([]byte, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	rc, err := client.Bucket(bucket).Object(path).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// Read the object's data
	return io.ReadAll(rc)
}

func deleteObjectsByPrefix(ctx context.Context, bucketName, prefix string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	bucket := client.Bucket(bucketName)
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	failed := []string{}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("Bucket(%s).Objects().Next: %w", bucketName, err)
		}

		obj := bucket.Object(attrs.Name)
		if err := obj.Delete(ctx); err != nil {
			failed = append(failed, attrs.Name)
			// Log error but continue trying to delete other objects
			log.Printf("Failed to delete Object(%s), continuing: %v", attrs.Name, err)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("Failed to delete objects (see above for errors): %v", failed)
	}
	return nil
}

func getEmulatorBucket(t *testing.T) string {
	bucket := os.Getenv("MULTITIER_TEST_GCS_BUCKET")
	assert.Assert(t, bucket != "", "Missing MULTITIER_TEST_GCS_BUCKET")
	return bucket
}

func getEmulatorKSA(t *testing.T) string {
	bucket := os.Getenv("EMULATOR_KSA")
	assert.Assert(t, bucket != "", "Missing EMULATOR_KSA")
	return bucket
}

func setEmulatorConfig(ctx context.Context, t *testing.T, uuid string, start, end int, options ...string) {
	t.Helper()
	params := getSupersliceTestParams(t)
	supersliceJob := true
	if params == nil {
		supersliceJob = false
		params = getScaleTestParams(t)
		if params == nil {
			t.Fatalf("no emulated test parameters")
		}
	}

	cleanup := start == 0
	restore := start > 0
	path := fmt.Sprintf("%s/workload-config.yaml", uuid)
	config := []string{
		fmt.Sprintf("start_step: %d", start),
		fmt.Sprintf("end_step: %d", end),
		fmt.Sprintf("cleanup_backup: %t", cleanup),
		fmt.Sprintf("cleanup_local: %t", cleanup),
		fmt.Sprintf("restore_expected: %t", restore),
		"", // Force a newline at the end of the file.
	}
	if slices.Contains(options, useCubePeers) {
		config = append(config, "data_parallelism: 0", fmt.Sprintf("peer_cube_size: %d", params.nodesPerSlice))
	} else if supersliceJob {
		// Matches getSupersliceTestParams().
		config = append(config, "data_parallelism: 4")
	} else {
		// Multislice jobs assumed data parallel.
		if params.slices < 3 {
			t.Fatalf("Must have at least 3 slices for multislice test as we assume data parallel.")
		}
		config = append(config, fmt.Sprintf("data_parallelism: %d", params.slices))
	}
	if slices.Contains(options, waitForDisruption) {
		config = append(config, "wait_for_disruption: true")
	} else {
		config = append(config, "wait_for_disruption: false")
	}

	assert.NilError(t, uploadObject(ctx, getEmulatorBucket(t), path, strings.Join(config, "\n")))
}

func waitForDisruptionReady(ctx context.Context, t *testing.T, uuid string, step int) bool {
	path := fmt.Sprintf("%s/disruption-ready-local-%d.txt", uuid, step)
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 20*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := readObject(ctx, getEmulatorBucket(t), path)
		t.Logf("fetching disruption %s/%s: %v", getEmulatorBucket(t), path, err)
		return err == nil, nil
	})
	return err == nil
}

func clearDistruptionFiles(ctx context.Context, t *testing.T, uuid string) bool {
	prefix := fmt.Sprintf("%s/disruption-ready-local", uuid)
	return deleteObjectsByPrefix(ctx, getEmulatorBucket(t), prefix) == nil
}

func waitForFailedJobset(ctx context.Context, t *testing.T, target *jobsetv1alpha.JobSet) {
	t.Helper()
	jobsetKey := types.NamespacedName{Namespace: target.GetNamespace(), Name: target.GetName()}

	assert.NilError(t, wait.PollUntilContextTimeout(ctx, 2*time.Second, 40*time.Minute, true, func(ctx context.Context) (bool, error) {
		var jobset jobsetv1alpha.JobSet
		err := CRClient.Get(ctx, jobsetKey, &jobset)
		if err != nil {
			t.Logf("Error probing jobset for failure: %v: %v", jobsetKey, err)
			return false, nil
		}
		t.Logf("Jobset terminal status: %+v", jobset.Status)
		for _, status := range jobset.Status.ReplicatedJobsStatus {
			if status.Failed > 0 {
				return true, nil
			}
		}
		return false, nil
	}))
}

func waitForJobsetFinished(ctx context.Context, t *testing.T, target *jobsetv1alpha.JobSet) {
	t.Helper()
	jobsetKey := types.NamespacedName{Namespace: target.GetNamespace(), Name: target.GetName()}

	assert.NilError(t, wait.PollUntilContextTimeout(ctx, 2*time.Second, 40*time.Minute, true, func(ctx context.Context) (bool, error) {
		var jobset jobsetv1alpha.JobSet
		if err := CRClient.Get(ctx, jobsetKey, &jobset); err != nil {
			return false, err
		}

		if jobset.Status.TerminalState == "Failed" {
			return false, fmt.Errorf("%v failed", jobsetKey)
		}

		t.Logf("%v: Jobset status: %s %+v", time.Now(), jobsetKey, jobset.Status)
		// Why are we waiting for 3 succeeded jobs? Either this is a copy/paste error, or the count can be flakey and
		// we're happy when a few succeed. But this won't work if there's two completions...
		jobsSucceeded := len(jobset.Status.ReplicatedJobsStatus) > 0 && jobset.Status.ReplicatedJobsStatus[0].Succeeded == 3
		return strings.Contains(jobset.Status.TerminalState, "Complete") || jobsSucceeded, nil
	}))
}

// cleanupJobset deletes a jobset; it's meant to be used in a defer and ignores errors.
func cleanupJobset(ctx context.Context, t *testing.T, namespace, name string) {
	t.Logf("deleting jobset %s/%s", namespace, name)
	jobset := jobsetv1alpha.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	err := CRClient.Delete(ctx, &jobset)
	if err != nil {
		t.Logf("Error deleting jobset, ignoring: %v", err)
	} else {
		t.Logf("waiting for pods to delete")
		if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			var pods corev1.PodList

			// TODO: this assumes the only pods are from the jobset, should check the owner or at least the pod name
			err := CRClient.List(ctx, &pods, client.InNamespace(namespace))
			if err != nil {
				t.Logf("Error listing pods waiting for jobset to delete, ignoring: %v", err)
				return false, nil
			}
			return len(pods.Items) == 0, nil
		}); err != nil {
			t.Logf("Error waiting for jobset to delete, ignoring: %v", err)
		}
	}
}
