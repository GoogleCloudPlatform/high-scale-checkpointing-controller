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
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	proto "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/lib/grpc/ranks/proto"
)

const (
	clientTestTarget = ":48932"
)

type ranksClientTestService struct {
	reqChan chan *proto.UpdateRequest
	rspChan chan *proto.UpdateResponse
}

var _ proto.RanksServiceServer = &ranksClientTestService{}

type ranksClientTestUpdater struct {
	state       driverState
	loopStarted bool
}

var _ ranksUpdater = &ranksClientTestUpdater{}

func (s *ranksClientTestService) Update(ctx context.Context, req *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	klog.Infof("ranks update request: %+v", req)
	select {
	case s.reqChan <- req:
	case <-ctx.Done():
		return nil, fmt.Errorf("test service canceled when saving request")
	}
	var rsp *proto.UpdateResponse
	select {
	case rsp = <-s.rspChan:
	case <-ctx.Done():
		return nil, fmt.Errorf("test service canceled before response found")
	}
	klog.Infof("ranks update response: %+v", rsp)
	return rsp, nil
}

func (u *ranksClientTestUpdater) setState(state driverState) {
	u.state = state
}

func (u *ranksClientTestUpdater) getState() driverState {
	return u.state
}

func (u *ranksClientTestUpdater) startUpdateLoop(context.Context) {
	u.loopStarted = true
}

func (u *ranksClientTestUpdater) killCurrentUpdateLoop() {
	u.loopStarted = false
}

func (u *ranksClientTestUpdater) reset() {
	*u = ranksClientTestUpdater{}
}

func startTestService(t *testing.T) (chan *proto.UpdateRequest, chan *proto.UpdateResponse, proto.RanksServiceClient, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", clientTestTarget)
	assert.NilError(t, err)

	// Note the channels must be buffered as the Unmount call is blocking.
	testSvr := &ranksClientTestService{make(chan *proto.UpdateRequest, 1), make(chan *proto.UpdateResponse, 1)}

	grpcSvr := grpc.NewServer()
	proto.RegisterRanksServiceServer(grpcSvr, testSvr)
	go func() { grpcSvr.Serve(lis) }()

	conn, err := grpc.Dial(clientTestTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NilError(t, err)

	clientApi := proto.NewRanksServiceClient(conn)

	return testSvr.reqChan, testSvr.rspChan, clientApi, func() {
		conn.Close()
		grpcSvr.GracefulStop()
	}
}

func setupClientDir(t *testing.T) (RanksClientOpts, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "ranks-client-test-")
	assert.NilError(t, err)
	cleanup := func() {
		err := os.RemoveAll(tempDir)
		if err != nil {
			t.Logf("warning: could not delete %s: %v", tempDir, err)
		}
	}
	ok := false
	defer func() {
		if !ok {
			cleanup()
		}
	}()

	stateDir := filepath.Join(tempDir, "state")
	err = os.Mkdir(stateDir, 0755)
	assert.NilError(t, err)
	idfileDir := filepath.Join(tempDir, "idfile")
	err = os.Mkdir(idfileDir, 0755)
	assert.NilError(t, err)

	ok = true
	return RanksClientOpts{
		StateDir:  stateDir,
		IdFileDir: idfileDir,
	}, cleanup
}

func TestRanksClient(t *testing.T) {
	opts, cleanup := setupClientDir(t)
	defer cleanup()

	opts.IdFileClientOpts = IdFileClientOpts{NodeName: "node"}
	opts.ProcessInfoFilename = "process-info"
	opts.CoordinatorPort = 1234

	reqChan, rspChan, clientApi, shutdown := startTestService(t)
	defer shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	// The cancel must happen before test service shutdown in order to be reliable.
	defer cancel()

	// Run this with a test updater that does not run the real update loop.
	// The ranks update api is only called from Unmount, to send a shutdown
	// update.
	updater := &ranksClientTestUpdater{}

	newClient := func() *ranksClient {
		return &ranksClient{
			updater: updater,
			api:     clientApi,
			opts:    opts,
		}
	}

	client := newClient()

	err := client.NewMount(ctx, types.UID("pod-id"))
	assert.NilError(t, err)

	// Check initial state from an empty cache.
	assert.Equal(t, updater.state, driverState{
		PodUID: types.UID("pod-id"),
		Node:   "node",
		State:  proto.Lifecycle_PENDING,
		Rank:   -1,
	})
	assert.Assert(t, updater.loopStarted)

	client.updateState(driverState{
		PodUID: types.UID("pod-id"),
		Node:   "node",
		State:  proto.Lifecycle_ASSIGNED,
		Rank:   2,
	})

	// Check cached state
	updater.reset()
	client = newClient()
	err = client.NewMount(ctx, types.UID("pod-id"))
	assert.NilError(t, err)
	assert.Equal(t, updater.state, driverState{
		PodUID: types.UID("pod-id"),
		Node:   "node",
		State:  proto.Lifecycle_ASSIGNED,
		Rank:   2,
	})

	err = client.completeState(updater.state)
	assert.ErrorContains(t, err, "Missing controller IP")
	updater.state = driverState{
		PodUID:       types.UID("pod-id"),
		Node:         "node",
		State:        proto.Lifecycle_ASSIGNED,
		Rank:         2,
		ControllerIP: "IP",
	}
	err = client.updateState(updater.state)
	assert.NilError(t, err)
	err = client.completeState(updater.state)
	assert.NilError(t, err)
	assert.Assert(t, !updater.loopStarted)

	data, err := ioutil.ReadFile(filepath.Join(opts.IdFileDir, "process-info"))
	assert.NilError(t, err)
	assert.Equal(t, string(data), "2\nIP:1234\n")

	client.NewMount(ctx, types.UID("pod-id"))
	assert.Assert(t, updater.loopStarted)
	rspChan <- &proto.UpdateResponse{}
	client.Unmount(ctx, types.UID("pod-id"))
	assert.Assert(t, !updater.loopStarted)
	req := <-reqChan
	assert.Equal(t, req.State, proto.Lifecycle_SHUTDOWN)
}

func TestRanksClientJobsetMismatch(t *testing.T) {
	for _, testcase := range []struct {
		name        string
		state       proto.Lifecycle
		rank        int
		jobset      string
		jobsetShape string
	}{
		{
			name:        "assigned, stale jobset",
			state:       proto.Lifecycle_ASSIGNED,
			rank:        2,
			jobset:      "default/old-job",
			jobsetShape: "3x2",
		},
		{
			name:        "assigned, wrong shape",
			state:       proto.Lifecycle_ASSIGNED,
			rank:        2,
			jobset:      "default/new-job",
			jobsetShape: "1x2",
		},
		{
			name:        "pending, stale job",
			state:       proto.Lifecycle_ASSIGNED,
			rank:        2,
			jobset:      "default/old-job",
			jobsetShape: "3x2",
		},
	} {
		tc := testcase
		t.Run(tc.name, func(t *testing.T) {
			opts, cleanup := setupClientDir(t)
			defer cleanup()

			opts.IdFileClientOpts = IdFileClientOpts{NodeName: "node"}
			opts.ProcessInfoFilename = "process-info"
			opts.CoordinatorPort = 1234

			reqChan, rspChan, clientApi, shutdown := startTestService(t)
			defer shutdown()

			ctx, cancel := context.WithCancel(context.Background())
			// The cancel must happen before test service shutdown in order to be reliable.
			defer cancel()

			client := &ranksClient{
				api:  clientApi,
				opts: opts,
			}
			client.updater = newRanksAssigningUpdater(clientApi, client, nil)

			client.updateState(driverState{
				PodUID:      types.UID("old-pod"),
				Node:        "node",
				State:       tc.state,
				Rank:        tc.rank,
				Jobset:      tc.jobset,
				JobsetShape: tc.jobsetShape,
			})

			rspChan <- &proto.UpdateResponse{
				NewState:    proto.Lifecycle_PENDING,
				Rank:        int32(tc.rank),
				Jobset:      "default/new-job",
				JobsetShape: "3x2",
			}
			err := client.NewMount(ctx, types.UID("pod-id"))
			assert.NilError(t, err)

			req := <-reqChan // The first request
			assert.Equal(t, tc.rank, int(req.Rank))

			req = <-reqChan // The followup after the new jobset.
			assert.Equal(t, req.Rank, int32(-1))
			assert.Equal(t, req.Jobset, "default/new-job")
			assert.Equal(t, req.JobsetShape, "3x2")

			t.Logf("finished %s", tc.name)
		})
	}
}

// todo: test recovery from gRPC errors in update loop and mismatched id on unmount.
