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

package idfile

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	proto "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/lib/grpc/ranks/proto"
)

const (
	ranksServerPort = 19257
)

var (
	RanksService proto.RanksServiceClient
	Server       *ranksServer
)

func mustSetupRanksCluster(t *testing.T) (context.Context, func(context.Context)) {
	t.Helper()

	origWaitTimeout := WaitTimeout
	WaitTimeout = 60 * time.Second

	opts := ControllerOpts{
		Namespace:  ControllerNamespace,
		DriverName: driverName,
	}

	server, serverOpts := NewRanksServer(opts)
	Server = server.(*ranksServer)

	grpcSvr, serveForever, err := Server.PrepareToServe(ranksServerPort)
	if err != nil {
		t.Fatalf("grpc server not started: %v", err)
	}

	go func() {
		if err := serveForever(); err != nil {
			t.Logf("error returned from serving: %v", err)
		}
	}()

	ctx, cancel := mustSetupCluster(func(cfg *rest.Config) (ctrl.Manager, error) {
		return NewRanksManager(testCfg, serverOpts, testControllerOpts)
	})

	conn, err := grpc.Dial(fmt.Sprintf(":%d", ranksServerPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("did not connect to ranks server: %v", err)
	}

	RanksService = proto.NewRanksServiceClient(conn)

	return ctx, func(ctx context.Context) {
		WaitTimeout = origWaitTimeout
		conn.Close()
		grpcSvr.GracefulStop()
		cancel(ctx)
	}
}

type testSyncer struct {
	mutex      sync.Mutex
	assignment map[string]int
	controller map[string]string
}

var _ clientSyncer = &testSyncer{}

func newTestSyncer() *testSyncer {
	return &testSyncer{assignment: map[string]int{}, controller: map[string]string{}}
}

func (s *testSyncer) updateState(state driverState) error {
	klog.Infof("updated %s to %s/%d", state.Node, state.State, state.Rank)
	return nil
}

func (s *testSyncer) completeState(state driverState) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	klog.Infof("completed %s to %s/%d", state.Node, state.State, state.Rank)
	s.assignment[state.Node] = state.Rank
	s.controller[state.Node] = state.ControllerIP
	return nil
}

func (s *testSyncer) getAssignments() map[string]int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	out := map[string]int{}
	for n, r := range s.assignment {
		out[n] = r
	}
	return out
}

func (s *testSyncer) getController() map[string]string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	out := map[string]string{}
	for n, ip := range s.controller {
		out[n] = ip
	}
	return out
}

func (s *testSyncer) controllersAllMatch(ip string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if len(s.controller) == 0 {
		return false
	}
	for _, check := range s.controller {
		if check != ip {
			klog.Errorf("ips: %v", s.controller)
			return false
		}
	}
	return true
}

func (s *testSyncer) clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.assignment = map[string]int{}
	s.controller = map[string]string{}
}

func TestRanksJobRestarts(t *testing.T) {
	runTest := func(t *testing.T, numSlices, sliceSize int) {
		ctx, cleanup := mustSetupRanksCluster(t)
		defer cleanup(ctx)

		jobset := createJobSet(ctx, t, "job", numSlices, sliceSize)
		for slice := 0; slice < numSlices; slice++ {
			for worker := 0; worker < sliceSize; worker++ {
				idx := slice*sliceSize + worker
				createNode(ctx, t, fmt.Sprintf("n%d", idx), fmt.Sprintf("p%d", slice))
			}
		}

		syncer := newTestSyncer()
		updaters := []ranksUpdater{}
		for i := 0; i < numSlices*sliceSize; i++ {
			updaters = append(updaters, newRanksAssigningUpdater(RanksService, syncer, nil))
		}

		// First deploy pods that can use the initial ranks.
		pods := make([]*corev1.Pod, numSlices*sliceSize)
		for slice := 0; slice < numSlices; slice++ {
			for worker := 0; worker < sliceSize; worker++ {
				idx := slice*sliceSize + worker
				pods[idx] = createWorker(ctx, t, "job", slice, worker, fmt.Sprintf("n%d", idx), 0)
			}
		}
		for i := range numSlices * sliceSize {
			pods[i].Status = corev1.PodStatus{
				PodIP: fmt.Sprintf("192.168.0.%d", 33+i),
			}
			err := k8sClient.Status().Update(ctx, pods[i])
			assert.NilError(t, err)
		}

		for i := 0; i < numSlices*sliceSize; i++ {
			updaters[i].setState(driverState{
				Jobset:      "default/job",
				JobsetShape: fmt.Sprintf("%dx%d", numSlices, sliceSize),
				Node:        fmt.Sprintf("n%d", i),
				Rank:        -1,
				PodUID:      pods[i].GetUID(),
			})
			updaters[i].startUpdateLoop(ctx)
		}

		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			asgn := syncer.getAssignments()
			return len(asgn) == numSlices*sliceSize, nil
		})
		asgn := syncer.getAssignments()
		assert.Equal(t, len(asgn), numSlices*sliceSize)
		for i := 0; i < numSlices*sliceSize; i++ {
			idx, found := asgn[fmt.Sprintf("n%d", i)]
			assert.Assert(t, found)
			assert.Equal(t, idx, i)
		}
		assert.Assert(t, syncer.controllersAllMatch("192.168.0.33"))

		t.Logf("Initial ranks succeeded, simulating slice deletion")

		for _, u := range updaters {
			u.killCurrentUpdateLoop()
		}
		syncer.clear()
		for _, pod := range pods {
			err := k8sClient.Delete(ctx, pod)
			assert.NilError(t, err)
		}
		err := k8sClient.Delete(ctx, jobset)
		assert.NilError(t, err)
		jobset = createJobSet(ctx, t, "job", numSlices, sliceSize)

		// Reschedule with some nodes swapped.
		var nextSlice int
		if numSlices == 1 && sliceSize == 1 {
			// Nothing to swap, restarts for this shape are in multi-node tests.
			return
		} else if sliceSize == 1 {
			// swap the first two slices
			pods[0] = createWorker(ctx, t, "job", 1, 0, "n0", 0)
			pods[1] = createWorker(ctx, t, "job", 0, 0, "n1", 0)
			nextSlice = 2
		} else {
			// shift the workers in the first slice
			for i := 0; i < sliceSize; i++ {
				worker := (i + 1) % sliceSize
				pods[i] = createWorker(ctx, t, "job", 0, worker, fmt.Sprintf("n%d", i), 0)
			}
			nextSlice = 1
		}
		// put the remaining pods onto nodes matching their indices.
		for slice := nextSlice; slice < numSlices; slice++ {
			for worker := 0; worker < sliceSize; worker++ {
				idx := slice*sliceSize + worker
				pods[idx] = createWorker(ctx, t, "job", slice, worker, fmt.Sprintf("n%d", idx), 0)
			}
		}

		for i := 0; i < numSlices*sliceSize; i++ {
			state := updaters[i].getState()
			state.PodUID = pods[i].GetUID()
			updaters[i].setState(state)
			updaters[i].startUpdateLoop(ctx)
		}

		// The IPs gets assigned late, but it shouldn't matter.
		for i := range numSlices * sliceSize {
			pods[i].Status = corev1.PodStatus{
				PodIP: fmt.Sprintf("192.168.0.%d", 54+i),
			}
			err := k8sClient.Status().Update(ctx, pods[i])
			assert.NilError(t, err)
		}

		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			asgn := syncer.getAssignments()
			return len(asgn) == numSlices*sliceSize, nil
		})
		asgn = syncer.getAssignments()
		assert.Equal(t, len(asgn), numSlices*sliceSize)
		// Even with the swapped workers, the assigned index should match the node.
		for i := 0; i < numSlices*sliceSize; i++ {
			idx, found := asgn[fmt.Sprintf("n%d", i)]
			assert.Assert(t, found)
			assert.Equal(t, idx, i)
		}
		assert.Assert(t, syncer.controllersAllMatch("192.168.0.54"))
	}

	for _, tc := range []struct {
		a, b int
	}{
		{1, 1},
		{2, 1},
		{2, 2},
		{3, 1},
		{3, 2},
		{5, 3},
		{16, 8},
	} {
		a := tc.a
		b := tc.b
		t.Run(fmt.Sprintf("%d-%d", a, b), func(t *testing.T) { runTest(t, a, b) })
		if a != b {
			t.Run(fmt.Sprintf("%d-%d", b, a), func(t *testing.T) { runTest(t, b, a) })
		}
	}
}

func TestRanksNewSlice(t *testing.T) {
	runTest := func(t *testing.T, numSlices, sliceSize, deletedSlice int) {
		ctx, cleanup := mustSetupRanksCluster(t)
		defer cleanup(ctx)

		jobset := createJobSet(ctx, t, "job", numSlices, sliceSize)
		for slice := 0; slice < numSlices; slice++ {
			for worker := 0; worker < sliceSize; worker++ {
				idx := slice*sliceSize + worker
				createNode(ctx, t, fmt.Sprintf("n%d", idx), fmt.Sprintf("p%d", slice))
			}
		}

		syncer := newTestSyncer()
		updaters := []ranksUpdater{}
		for i := 0; i < numSlices*sliceSize; i++ {
			updaters = append(updaters, newRanksAssigningUpdater(RanksService, syncer, nil))
		}

		// First deploy pods that can use the initial ranks.
		pods := make([]*corev1.Pod, numSlices*sliceSize)
		for slice := 0; slice < numSlices; slice++ {
			for worker := 0; worker < sliceSize; worker++ {
				idx := slice*sliceSize + worker
				pods[idx] = createWorker(ctx, t, "job", slice, worker, fmt.Sprintf("n%d", idx), 0)
			}
		}
		for i := range numSlices * sliceSize {
			pods[i].Status = corev1.PodStatus{
				PodIP: fmt.Sprintf("192.168.0.%d", 33+i),
			}
			err := k8sClient.Status().Update(ctx, pods[i])
			assert.NilError(t, err)
		}

		for i := 0; i < numSlices*sliceSize; i++ {
			updaters[i].setState(driverState{
				Jobset:      "default/job",
				JobsetShape: fmt.Sprintf("%dx%d", numSlices, sliceSize),
				Node:        fmt.Sprintf("n%d", i),
				Rank:        -1,
				PodUID:      pods[i].GetUID(),
			})
			updaters[i].startUpdateLoop(ctx)
		}

		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			asgn := syncer.getAssignments()
			return len(asgn) == numSlices*sliceSize, nil
		})
		asgn := syncer.getAssignments()
		assert.Equal(t, len(asgn), numSlices*sliceSize)
		for i := 0; i < numSlices*sliceSize; i++ {
			idx, found := asgn[fmt.Sprintf("n%d", i)]
			assert.Assert(t, found)
			assert.Equal(t, idx, i)
		}
		assert.Assert(t, syncer.controllersAllMatch("192.168.0.33"))

		t.Logf("Initial ranks succeeded, simulating node failure & restart")

		for _, u := range updaters {
			u.killCurrentUpdateLoop()
		}
		syncer.clear()
		for _, pod := range pods {
			err := k8sClient.Delete(ctx, pod)
			assert.NilError(t, err)
		}
		err := k8sClient.Delete(ctx, jobset)
		assert.NilError(t, err)
		jobset = createJobSet(ctx, t, "job", numSlices, sliceSize)

		for i := 0; i < sliceSize; i++ {
			idx := deletedSlice*sliceSize + i
			createNode(ctx, t, fmt.Sprintf("new-n%d", idx), fmt.Sprintf("p%d", deletedSlice))
		}

		// Schedule all pods to new nodes.
		for slice := 0; slice < numSlices; slice++ {
			for worker := 0; worker < sliceSize; worker++ {
				nodeIdx := slice*sliceSize + worker
				workerIdx := (1 + slice*sliceSize + worker) % (numSlices * sliceSize)
				workerSlice := workerIdx / sliceSize
				workerNum := workerIdx % sliceSize
				var node string
				if slice == deletedSlice {
					node = fmt.Sprintf("new-n%d", nodeIdx)
				} else {
					node = fmt.Sprintf("n%d", nodeIdx)
				}
				pods[nodeIdx] = createWorker(ctx, t, "job", workerSlice, workerNum, node, 0)
			}
		}

		// Since the controller may not be on pod[0], assign IPs to all pods.
		for i := 0; i < numSlices*sliceSize; i++ {
			pods[i].Status = corev1.PodStatus{
				PodIP: fmt.Sprintf("10.0.0.%d", i),
			}
			err := k8sClient.Status().Update(ctx, pods[i])
			assert.NilError(t, err)
		}

		for i := 0; i < numSlices*sliceSize; i++ {
			if i/sliceSize == deletedSlice {
				updaters[i].setState(driverState{
					Node:   fmt.Sprintf("new-n%d", i),
					Rank:   -1,
					PodUID: pods[i].GetUID(),
				})
			} else {
				state := updaters[i].getState()
				state.PodUID = pods[i].GetUID()
				updaters[i].setState(state)
			}
			updaters[i].startUpdateLoop(ctx)
		}

		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			asgn := syncer.getAssignments()
			return len(asgn) == numSlices*sliceSize, nil
		})
		asgn = syncer.getAssignments()
		assert.Equal(t, len(asgn), numSlices*sliceSize)
		// Even with the rearranged workers, the assigned index should match the node.
		deletedAssignment := make([]bool, sliceSize)
		var controllerNode string
		for i := 0; i < numSlices*sliceSize; i++ {
			var node string
			if i/sliceSize == deletedSlice {
				node = fmt.Sprintf("new-n%d", i)
			} else {
				node = fmt.Sprintf("n%d", i)
			}
			idx, found := asgn[node]
			assert.Assert(t, found, i)
			if i/sliceSize == deletedSlice {
				assert.Assert(t, idx >= deletedSlice*sliceSize && idx < (deletedSlice+1)*sliceSize, i)
				deletedAssignment[idx-deletedSlice*sliceSize] = true
			} else {
				assert.Equal(t, idx, i)
			}
			if idx == 0 {
				controllerNode = node
			}
		}
		for i := 0; i < sliceSize; i++ {
			assert.Assert(t, deletedAssignment[i], i)
		}

		assert.Assert(t, controllerNode != "")
		var controllerIP string
		for i := 0; i < numSlices*sliceSize; i++ {
			if pods[i].Spec.NodeName == controllerNode {
				controllerIP = pods[i].Status.PodIP
				break
			}
		}
		assert.Assert(t, controllerIP != "")
		assert.Assert(t, syncer.controllersAllMatch(controllerIP))
	}

	for _, tc := range []struct {
		a, b int
	}{
		{2, 1},
		{2, 2},
		{5, 1},
		{5, 3},
		{16, 8},
	} {
		a := tc.a
		b := tc.b
		targets := []int{0}
		if a > 2 {
			targets = append(targets, 1, a-1)
		}
		if a > 5 {
			targets = append(targets, 2, a-2)
		}
		for _, toDelete := range targets {
			t.Run(fmt.Sprintf("%d-%d.%d", a, b, toDelete), func(t *testing.T) { runTest(t, a, b, toDelete) })
			// Only run the inverse if there is more than one slice, and the deleted slice is in range.
			if a != b && b > 1 && toDelete < b {
				t.Run(fmt.Sprintf("%d-%d.%d", b, a, toDelete), func(t *testing.T) { runTest(t, b, a, toDelete) })
			}
		}
	}
}

func TestRanksDistinctJobs(t *testing.T) {
	ctx, cleanup := mustSetupRanksCluster(t)
	defer cleanup(ctx)

	createJobSet(ctx, t, "job0", 2, 1)
	createJobSet(ctx, t, "job1", 2, 1)
	createNode(ctx, t, "n0", "p0")
	createNode(ctx, t, "n1", "p0")
	createNode(ctx, t, "n2", "p1")
	createNode(ctx, t, "n3", "p1")

	syncer := newTestSyncer()
	updaters := []ranksUpdater{}
	for i := 0; i < 4; i++ {
		updaters = append(updaters, newRanksAssigningUpdater(RanksService, syncer, nil))
	}

	t.Logf("First deployment of job0 and job1 using jobset ranks")

	pods := []*corev1.Pod{}
	pods = append(pods, createWorker(ctx, t, "job0", 0, 0, "n0", 0))
	pods = append(pods, createWorker(ctx, t, "job1", 0, 0, "n1", 0))
	pods = append(pods, createWorker(ctx, t, "job0", 1, 0, "n2", 0))
	pods = append(pods, createWorker(ctx, t, "job1", 1, 0, "n3", 0))

	pods[0].Status.PodIP = "192.168.0.1"
	err := k8sClient.Status().Update(ctx, pods[0])
	assert.NilError(t, err)
	pods[1].Status.PodIP = "192.168.1.1"
	err = k8sClient.Status().Update(ctx, pods[1])
	assert.NilError(t, err)
	pods[2].Status.PodIP = "192.168.0.2"
	err = k8sClient.Status().Update(ctx, pods[2])
	assert.NilError(t, err)
	pods[3].Status.PodIP = "192.168.1.2"
	err = k8sClient.Status().Update(ctx, pods[3])
	assert.NilError(t, err)

	for i := 0; i < 4; i++ {
		var jobset string
		if i < 2 {
			jobset = "default/job0"
		} else {
			jobset = "default/job1"
		}
		updaters[i].setState(driverState{
			Jobset:      jobset,
			JobsetShape: "2x1",
			Node:        fmt.Sprintf("n%d", i),
			Rank:        -1,
			PodUID:      pods[i].GetUID(),
		})
		updaters[i].startUpdateLoop(ctx)
	}
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		asgn := syncer.getAssignments()
		return len(asgn) == 4, nil
	})
	asgn := syncer.getAssignments()
	assert.Equal(t, len(asgn), 4)
	assert.Equal(t, asgn["n0"], 0)
	assert.Equal(t, asgn["n1"], 0)
	assert.Equal(t, asgn["n2"], 1)
	assert.Equal(t, asgn["n3"], 1)
	ctl := syncer.getController()
	assert.Equal(t, ctl["n0"], "192.168.0.1")
	assert.Equal(t, ctl["n1"], "192.168.1.1")
	assert.Equal(t, ctl["n2"], "192.168.0.1")
	assert.Equal(t, ctl["n3"], "192.168.1.1")

	t.Logf("Redeployment, swapping job0 pods only")

	for _, p := range pods {
		err := k8sClient.Delete(ctx, p)
		assert.NilError(t, err)
	}
	for _, u := range updaters {
		u.killCurrentUpdateLoop()
	}
	syncer.clear()

	pods[0] = createWorker(ctx, t, "job0", 1, 0, "n0", 0)
	pods[1] = createWorker(ctx, t, "job1", 0, 0, "n1", 0)
	pods[2] = createWorker(ctx, t, "job0", 0, 0, "n2", 0)
	pods[3] = createWorker(ctx, t, "job1", 1, 0, "n3", 0)

	pods[0].Status.PodIP = "192.168.0.2"
	err = k8sClient.Status().Update(ctx, pods[0])
	assert.NilError(t, err)
	pods[1].Status.PodIP = "192.168.1.2"
	err = k8sClient.Status().Update(ctx, pods[1])
	assert.NilError(t, err)
	pods[2].Status.PodIP = "192.168.0.3"
	err = k8sClient.Status().Update(ctx, pods[2])
	assert.NilError(t, err)
	pods[3].Status.PodIP = "192.168.1.3"
	err = k8sClient.Status().Update(ctx, pods[3])
	assert.NilError(t, err)

	for i, u := range updaters {
		state := u.getState()
		state.PodUID = pods[i].GetUID()
		u.setState(state)
		u.startUpdateLoop(ctx)
	}
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		asgn := syncer.getAssignments()
		return len(asgn) == 4, nil
	})
	asgn = syncer.getAssignments()
	assert.Equal(t, len(asgn), 4)
	assert.Equal(t, asgn["n0"], 0)
	assert.Equal(t, asgn["n1"], 0)
	assert.Equal(t, asgn["n2"], 1)
	assert.Equal(t, asgn["n3"], 1)
	ctl = syncer.getController()
	assert.Equal(t, ctl["n0"], "192.168.0.2")
	assert.Equal(t, ctl["n1"], "192.168.1.2")
	assert.Equal(t, ctl["n2"], "192.168.0.2")
	assert.Equal(t, ctl["n3"], "192.168.1.2")
}

func TestRanksOverlappingJobs(t *testing.T) {
	ctx, cleanup := mustSetupRanksCluster(t)
	defer cleanup(ctx)

	createJobSet(ctx, t, "job0", 1, 2)
	createJobSet(ctx, t, "job1", 1, 2)
	createNode(ctx, t, "n0", "p0")
	createNode(ctx, t, "n1", "p0")
	createNode(ctx, t, "n2", "p0")

	syncer := newTestSyncer()
	updaters := []ranksUpdater{}
	for i := 0; i < 3; i++ {
		updaters = append(updaters, newRanksAssigningUpdater(RanksService, syncer, nil))
	}

	pods := []*corev1.Pod{}
	pods = append(pods, createWorker(ctx, t, "job0", 0, 0, "n0", 0))
	pods = append(pods, createWorker(ctx, t, "job0", 0, 1, "n1", 0))
	pods[0].Status.PodIP = "192.168.0.1"
	err := k8sClient.Status().Update(ctx, pods[0])
	assert.NilError(t, err)
	pods[1].Status.PodIP = "192.168.0.2"
	err = k8sClient.Status().Update(ctx, pods[1])
	assert.NilError(t, err)

	for i := 0; i < 2; i++ {
		updaters[i].setState(driverState{
			Jobset:      "default/job0",
			JobsetShape: "1x2",
			Node:        fmt.Sprintf("n%d", i),
			Rank:        -1,
			PodUID:      pods[i].GetUID(),
		})
		updaters[i].startUpdateLoop(ctx)
	}

	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		asgn := syncer.getAssignments()
		return len(asgn) == 2, nil
	})
	asgn := syncer.getAssignments()
	assert.Equal(t, len(asgn), 2)
	assert.Equal(t, asgn["n0"], 0)
	assert.Equal(t, asgn["n1"], 1)
	assert.Assert(t, syncer.controllersAllMatch("192.168.0.1"))

	syncer.clear()
	updaters[1].killCurrentUpdateLoop()

	pods[1] = createWorker(ctx, t, "job1", 0, 0, "n1", 0)
	pods = append(pods, createWorker(ctx, t, "job1", 0, 1, "n2", 0))
	pods[1].Status.PodIP = "192.168.2.0"
	err = k8sClient.Status().Update(ctx, pods[1])
	assert.NilError(t, err)
	pods[2].Status.PodIP = "192.168.2.1"
	err = k8sClient.Status().Update(ctx, pods[2])
	assert.NilError(t, err)

	for i := 1; i < 3; i++ {
		updaters[i].setState(driverState{
			Jobset:      "default/job1",
			JobsetShape: "1x2",
			Node:        fmt.Sprintf("n%d", i),
			Rank:        -1,
			PodUID:      pods[i].GetUID(),
		})
		updaters[i].startUpdateLoop(ctx)
	}
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		asgn := syncer.getAssignments()
		return len(asgn) == 2, nil
	})
	asgn = syncer.getAssignments()
	assert.Equal(t, len(asgn), 2)
	assert.Equal(t, asgn["n1"], 0)
	assert.Equal(t, asgn["n2"], 1)
	assert.Assert(t, syncer.controllersAllMatch("192.168.2.0"))
}

func TestRanksPodRefresh(t *testing.T) {
	ctx, cleanup := mustSetupRanksCluster(t)
	defer cleanup(ctx)

	const numSlices = 4
	const sliceSize = 32

	createJobSet(ctx, t, "job", numSlices, sliceSize)
	for slice := 0; slice < numSlices; slice++ {
		for worker := 0; worker < sliceSize; worker++ {
			idx := slice*sliceSize + worker
			createNode(ctx, t, fmt.Sprintf("n%d", idx), fmt.Sprintf("p%d", slice))
		}
	}

	syncer := newTestSyncer()
	updaters := []ranksUpdater{}
	for i := 0; i < numSlices*sliceSize; i++ {
		updaters = append(updaters, newRanksAssigningUpdater(RanksService, syncer, nil))
	}

	pods := make([]*corev1.Pod, numSlices*sliceSize)
	for slice := 0; slice < numSlices; slice++ {
		for worker := 0; worker < sliceSize; worker++ {
			idx := slice*sliceSize + worker
			pods[idx] = createWorker(ctx, t, "job", slice, worker, fmt.Sprintf("n%d", idx), 0)
		}
	}
	for i := range numSlices * sliceSize {
		pods[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", 33+i),
		}
		err := k8sClient.Status().Update(ctx, pods[i])
		assert.NilError(t, err)
	}

	for i := 0; i < numSlices*sliceSize; i++ {
		updaters[i].setState(driverState{
			Jobset:      "default/job",
			JobsetShape: fmt.Sprintf("%dx%d", numSlices, sliceSize),
			Node:        fmt.Sprintf("n%d", i),
			Rank:        -1,
			PodUID:      pods[i].GetUID(),
		})
		updaters[i].startUpdateLoop(ctx)
	}
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		asgn := syncer.getAssignments()
		return len(asgn) == numSlices*sliceSize, nil
	})
	asgn := syncer.getAssignments()
	assert.Equal(t, len(asgn), numSlices*sliceSize)
	for i := 0; i < numSlices*sliceSize; i++ {
		idx, found := asgn[fmt.Sprintf("n%d", i)]
		assert.Assert(t, found)
		assert.Equal(t, idx, i)
	}
	assert.Assert(t, syncer.controllersAllMatch("192.168.0.33"))
}

func TestRanksInconsistentInitialMapping(t *testing.T) {
	ctx, cleanup := mustSetupRanksCluster(t)
	defer cleanup(ctx)

	// Need at least two slices and two workers for the initial rank to work out.
	const numSlices = 3
	const sliceSize = 2

	createJobSet(ctx, t, "job", numSlices, sliceSize)
	for slice := 0; slice < numSlices; slice++ {
		for worker := 0; worker < sliceSize; worker++ {
			idx := slice*sliceSize + worker
			createNode(ctx, t, fmt.Sprintf("n%d", idx), fmt.Sprintf("p%d", slice))
		}
	}

	syncer := newTestSyncer()
	updaters := []ranksUpdater{}
	for i := 0; i < numSlices*sliceSize; i++ {
		updaters = append(updaters, newRanksAssigningUpdater(RanksService, syncer, nil))
	}

	pods := make([]*corev1.Pod, numSlices*sliceSize)
	for slice := 0; slice < numSlices; slice++ {
		for worker := 0; worker < sliceSize; worker++ {
			idx := slice*sliceSize + worker
			pods[idx] = createWorker(ctx, t, "job", slice, worker, fmt.Sprintf("n%d", idx), 0)

			// Worker 0 of slice i gets the rank of worker i in slice 1.
			if worker == 0 && slice < sliceSize {
				updaters[idx].setState(driverState{
					Jobset:      "default/job",
					JobsetShape: fmt.Sprintf("%dx%d", numSlices, sliceSize),
					Node:        fmt.Sprintf("n%d", idx),
					Rank:        sliceSize + slice,
					PodUID:      pods[idx].GetUID(),
				})
			} else {
				updaters[idx].setState(driverState{
					Jobset:      "default/job",
					JobsetShape: fmt.Sprintf("%dx%d", numSlices, sliceSize),
					Node:        fmt.Sprintf("n%d", idx),
					Rank:        -1,
					PodUID:      pods[idx].GetUID(),
				})
			}
			updaters[idx].startUpdateLoop(ctx)
		}
	}
	for i := range numSlices * sliceSize {
		pods[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", 33+i),
		}
		err := k8sClient.Status().Update(ctx, pods[i])
		assert.NilError(t, err)
	}

	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		asgn := syncer.getAssignments()
		return len(asgn) == numSlices*sliceSize, nil
	})
	t.Logf("assignment succeeded, although its value is arbitrary")
}
