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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	proto "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/lib/grpc/ranks/proto"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/metrics"
)

const (
	localCacheFilename = "rank-data"

	backoffStart = 250 * time.Millisecond
	backoffMax   = 10 * time.Second
	pendingDelay = 2 * time.Second

	updateDelay = 5 * time.Millisecond
)

type RanksClientOpts struct {
	IdFileClientOpts
	StateDir            string
	IdFileDir           string
	ProcessInfoFilename string
	CoordinatorPort     int
	ServerTarget        string

	// OPTIONAL, only used if metrics needs to be logged.
	MetricsManager metrics.MetricsManager
}

type RanksClient interface {
	NewMount(ctx context.Context, podUID types.UID) error
	Unmount(ctx context.Context, podUID types.UID)
}

type clientSyncer interface {
	updateState(driverState) error
	completeState(driverState) error
}

type ranksUpdater interface {
	getState() driverState
	setState(driverState)
	startUpdateLoop(context.Context)
	killCurrentUpdateLoop()
}

// driverState captures the rank state. Its fields are exported for encoding/json.
// Note that versioning of the on-disk encoding is not needed, as it's only
// serialized to the ramdisk that will be deleted any time the driver is
// updated.
type driverState struct {
	State        proto.Lifecycle
	Node         string
	PodUID       types.UID
	Jobset       string
	JobsetShape  string
	Rank         int
	ControllerIP string
}

// ranksClient implements RanksClient. The ranksClient handles the local cache and is the
// entry point for CSI. The rank protocol logic is in the ranksUpdater, which communicates
// using a client syncer. This allows the protocol logic to be tested independently.
type ranksClient struct {
	mutex sync.Mutex

	currUID types.UID
	updater ranksUpdater
	api     proto.RanksServiceClient
	opts    RanksClientOpts
}

var _ RanksClient = &ranksClient{}
var _ clientSyncer = &ranksClient{}

func NewRanksClient(opts RanksClientOpts) (RanksClient, error) {
	// TODO: figure out better credentials.
	conn, err := grpc.Dial(opts.ServerTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	clientAPI := proto.NewRanksServiceClient(conn)

	client := &ranksClient{
		api:  clientAPI,
		opts: opts,
	}
	updater := newRanksAssigningUpdater(clientAPI, client, opts.MetricsManager)
	client.updater = updater

	return client, nil
}

type ranksAssigningUpdater struct {
	mutex          sync.Mutex
	api            proto.RanksServiceClient
	syncer         clientSyncer
	state          driverState
	updateFinished chan struct{}
	updateCancelFn func()

	mm metrics.MetricsManager
}

func newRanksAssigningUpdater(api proto.RanksServiceClient, syncer clientSyncer, mManager metrics.MetricsManager) ranksUpdater {
	return &ranksAssigningUpdater{
		api:    api,
		syncer: syncer,
		mm:     mManager,
	}
}

func (r *ranksClient) NewMount(ctx context.Context, podUID types.UID) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.currUID = podUID

	r.updater.killCurrentUpdateLoop()

	if err := os.Remove(filepath.Join(r.opts.IdFileDir, r.opts.ProcessInfoFilename)); err != nil && !errors.Is(err, os.ErrNotExist) {
		klog.Warningf("error removing old process info file, ignored: %v", err)
	}

	state, err := r.updateStateFromLocalCache()
	if err != nil {
		return err
	}
	state.PodUID = podUID
	r.updater.setState(state)
	r.updater.startUpdateLoop(ctx)
	return nil
}

func (r *ranksClient) Unmount(ctx context.Context, uid types.UID) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if uid == r.currUID {
		r.updater.killCurrentUpdateLoop()
		state := r.updater.getState()
		state.State = proto.Lifecycle_SHUTDOWN
		if err := r.writeLocalCache(state); err != nil {
			klog.Warningf("Error updating local cache on shutdown (ignored): %v", err)
		}
		if _, err := r.api.Update(ctx, &proto.UpdateRequest{
			State:  state.State,
			Node:   state.Node,
			PodUid: string(state.PodUID),
			Rank:   int32(state.Rank),
		}); err != nil {
			klog.Warningf("Error on shutdown update (ignored): %v", err)
		}
	}
}

func (r *ranksClient) updateStateFromLocalCache() (driverState, error) {
	data, err := ioutil.ReadFile(filepath.Join(r.opts.StateDir, localCacheFilename))
	var cachedState driverState
	if os.IsNotExist(err) {
		klog.Infof("local cache not found")
		// Proceed with empty cached state.
	} else if err != nil {
		return driverState{}, err
	} else {
		// Otherwise read cachedState from the file.
		if err := json.Unmarshal(data, &cachedState); err != nil {
			return driverState{}, err
		}
	}
	if cachedState.State == proto.Lifecycle_SHUTDOWN {
		cachedState.State = proto.Lifecycle_PENDING
	}

	if cachedState.State != proto.Lifecycle_PENDING && cachedState.State != proto.Lifecycle_ASSIGNED {
		klog.Infof("Invalid cached state, resetting: %+v", cachedState)
		cachedState = driverState{
			State: proto.Lifecycle_PENDING,
			Rank:  -1}
	}
	if cachedState.Node != r.opts.NodeName {
		klog.Infof("Cache node mismatch, resetting: %+v", cachedState)
		cachedState = driverState{
			Node:  r.opts.NodeName,
			State: proto.Lifecycle_PENDING,
			Rank:  -1}
	}
	if err := r.writeLocalCache(cachedState); err != nil {
		return driverState{}, fmt.Errorf("Failed to update local cache: %w", err)
	}
	return cachedState, nil
}

func (r *ranksClient) writeLocalCache(state driverState) error {
	path := filepath.Join(r.opts.StateDir, localCacheFilename)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		return err
	}
	return nil
}

func (r *ranksClient) updateState(state driverState) error {
	return r.writeLocalCache(state)
}

func (r *ranksClient) completeState(state driverState) error {
	if state.ControllerIP == "" || state.Rank < 0 {
		return fmt.Errorf("Missing controller IP or rank in state, cannot complete rank assignment: %v", state)
	}

	// Do an atomic write first to a tempfile, then move.
	tmpfile := filepath.Join(r.opts.IdFileDir, ".tmp."+r.opts.ProcessInfoFilename)
	procfile := filepath.Join(r.opts.IdFileDir, r.opts.ProcessInfoFilename)
	contents := fmt.Sprintf("%d\n%s:%d\n", state.Rank, state.ControllerIP, r.opts.CoordinatorPort)
	if err := os.WriteFile(tmpfile, []byte(contents), 0555); err != nil {
		return err
	}
	if err := os.Rename(tmpfile, procfile); err != nil {
		return err
	}
	klog.Infof("Wrote process info at %s: %s", procfile, contents)
	r.updater.killCurrentUpdateLoop()
	return nil
}

func (r *ranksAssigningUpdater) setState(state driverState) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.state = state
}

func (r *ranksAssigningUpdater) getState() driverState {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.state
}

func (r *ranksAssigningUpdater) startUpdateLoop(ctx context.Context) {
	r.killCurrentUpdateLoop()
	cancelableCtx, cancelFn := context.WithCancel(ctx)
	r.updateCancelFn = cancelFn
	r.updateFinished = make(chan struct{})
	if r.mm == nil {
		klog.Infof("start ranks update loop")
		go r.updateLoop(cancelableCtx)
	} else {
		klog.Infof("start ranks update loop and record metrics")
		opDoneChan := r.mm.RecordNodeOperationWithTimeout(cancelableCtx, "/RanksClient/IdFileWrite")
		go func() { opDoneChan <- r.updateLoop(cancelableCtx) }()
	}
}

func (r *ranksAssigningUpdater) killCurrentUpdateLoop() {
	if r.updateCancelFn != nil {
		klog.Infof("canceling update loop...")
		r.updateCancelFn()
		select {
		case <-r.updateFinished:
			klog.Infof("update loop canceled")
		case <-time.After(time.Minute):
			klog.Infof("update loop stuck, aborting")
		}
		r.updateCancelFn = nil
	}
}

func (r *ranksAssigningUpdater) updateLoop(ctx context.Context) error {
	defer close(r.updateFinished)

	klog.Infof("running update loop on %+v", r.state)
	backoff := backoffStart
	backoffSleep := func() {
		time.Sleep(backoff)
		if backoff < backoffMax {
			backoff += backoff / time.Duration(2)
		}
	}
	var lastPendingLog time.Time

	for {
		rsp, err := r.api.Update(ctx, &proto.UpdateRequest{
			State:       r.state.State,
			Node:        r.state.Node,
			PodUid:      string(r.state.PodUID),
			Rank:        int32(r.state.Rank),
			Jobset:      r.state.Jobset,
			JobsetShape: r.state.JobsetShape,
		})
		if err != nil {
			if status, ok := status.FromError(err); ok {
				switch status.Code() {
				case codes.Canceled:
					klog.Infof("exiting ranks update loop due to cancel")
					return err
				case codes.DeadlineExceeded, codes.Unavailable:
					klog.Warningf("retriable rpc error: %s", status.Code())
					backoffSleep()
					continue
				}
			}
			if strings.Contains("Error while dialing", err.Error()) {
				klog.Warningf("retriable dial error, retrying: %v", err)
				backoffSleep()
				continue
			}
			klog.Warningf("Unknown loop gRPC error, exiting: %v", err)
			return err
		}
		// Throttle logging about pending responses.
		doLog := true
		if rsp.NewState == proto.Lifecycle_PENDING {
			if time.Since(lastPendingLog) < pendingDelay {
				doLog = false
			} else {
				lastPendingLog = time.Now()
			}
		}
		if doLog {
			klog.Infof("%s/%d got update response %+v", r.state.Node, r.state.Rank, rsp)
		}
		if finished, err := r.runUpdate(rsp); finished {
			klog.Infof("update finished")
			return err
		}
		backoff = backoffStart
		time.Sleep(updateDelay)
	}
}

func (r *ranksAssigningUpdater) runUpdate(rsp *proto.UpdateResponse) (bool, error) {
	// This function exists to wrap this mutex
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if rsp.Jobset != "" && (rsp.Jobset != r.state.Jobset || rsp.JobsetShape != r.state.JobsetShape) {
		klog.Infof("reseting stale jobset on %s %s/%s -> %s%s", r.state.Node, r.state.Jobset, r.state.JobsetShape, rsp.Jobset, rsp.JobsetShape)
		r.state.Jobset = rsp.Jobset
		r.state.JobsetShape = rsp.JobsetShape
		r.state.Rank = -1
		var err error
		if err = r.syncer.updateState(r.state); err != nil {
			// Not being able to write to the local cache is disasterous. By exiting
			// from the update loop, rank generation for the node will be halted and
			// the workload will stall, which will be the best way to communicate
			// the error.
			klog.Errorf("Error updating local cache with jobset, quitting: %v", err)
		}
		// Do another iteration so the empty rank can be communicated up.
		return false, err
	}

	if rsp.NewState == proto.Lifecycle_COMPLETED && r.state.Rank != int(rsp.Rank) {
		err := status.Errorf(codes.Aborted, "rank mismatch on completion. Local is %d, remote is %d. Halting update", r.state.Rank, rsp.Rank)
		klog.Error(err)
		return true, err
	}

	if r.state.Rank != -1 && int(rsp.Rank) != r.state.Rank {
		klog.Infof("updating current state to %d when existing rank is already %d, this will invalidate any local checkpoint. This is done because the existing ranks are inconsistent with the slice topology. Continuing execution", rsp.Rank, r.state.Rank)
	}

	if rsp.NewState != proto.Lifecycle_COMPLETED {
		r.state.State = rsp.NewState
	}
	r.state.Rank = int(rsp.Rank)
	r.state.ControllerIP = rsp.ControllerIp
	if err := r.syncer.updateState(r.state); err != nil {
		// Not being able to write to the local cache is disasterous. By exiting
		// from the update loop, rank generation for the node will be halted and
		// the workload will stall, which will be the best way to communicate
		// the error.
		err := status.Errorf(codes.Internal, "Error updating local cache in update loop, quitting: %v", err)
		klog.Error(err)
		return true, err
	}
	if rsp.NewState == proto.Lifecycle_COMPLETED {
		var err error
		if err = r.syncer.completeState(r.state); err != nil {
			klog.Errorf("Error during update completion, quitting: %v", err)
		}
		return true, err
	}
	return false, nil
}
