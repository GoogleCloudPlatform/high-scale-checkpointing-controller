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
	"maps"
	"net"
	"slices"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	proto "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/lib/grpc/ranks/proto"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

type RanksServerOpts struct {
	ControllerOpts
	ranksServer RanksServer
}

type RanksServer interface {
	// Returns a server, a function that will server forever (unless an error happens). If
	// something happens during preparation, an error is returned instead.
	PrepareToServe(port int) (*grpc.Server, func() error, error)

	HandlePod(*corev1.Pod) error
	HandleJobset(*jobsetv1alpha.JobSet) error
	AddNodeToPool(node, pool string)
	RemoveNode(node string)
	AddInformerClient(client.Client)
}

type podState struct {
	jobset           types.NamespacedName
	slice, workerIdx int
	node             string
	uid              types.UID
	ip               string
	state            proto.Lifecycle
	rank             int
	controllerIP     string
	hasUpdate        bool
}

type jobsetState struct {
	numJobs   int
	sliceSize int
	numSlices int
}

type jobsetAssignment struct {
	rankToPod []types.UID
	podToRank map[types.UID]int
}

type ranksServer struct {
	k8sClient  client.Client
	driverName string

	mutex        sync.Mutex
	podListMutex sync.Mutex
	pods         map[types.UID]*podState
	jobsets      map[types.NamespacedName]*jobsetState
	pools        map[string]string

	// podsToAdd and podsToDelete are used to reduce contention between HandlePod and the
	// Update gRPCs from nodes. There may be multiple delete and add events, and the maps
	// coalesce these into saving the most recent. In practice events are received in
	// order, so when a pod is deleted it is removed from the add list, and
	// vice-versa. It's not clear if this is guaranteed by the k8s API, but even if it's
	// mostly true, conforming to that behavior will reduce the number of stale pods to
	// handle.
	podsToAdd    map[types.UID]*podState
	podsToDelete map[types.UID]bool

	jobsetAssignments map[types.NamespacedName]jobsetAssignment
}

var _ RanksServer = &ranksServer{}
var _ proto.RanksServiceServer = &ranksServer{}

func NewRanksServer(opts ControllerOpts) (RanksServer, RanksServerOpts) {
	server := &ranksServer{
		driverName:        opts.DriverName,
		pods:              map[types.UID]*podState{},
		jobsets:           map[types.NamespacedName]*jobsetState{},
		pools:             map[string]string{},
		jobsetAssignments: map[types.NamespacedName]jobsetAssignment{},
		podsToAdd:         map[types.UID]*podState{},
		podsToDelete:      map[types.UID]bool{},
	}
	ranksOpts := RanksServerOpts{
		ControllerOpts: opts,
		ranksServer:    server,
	}
	return server, ranksOpts
}

func NewRanksManager(cfg *rest.Config, opts RanksServerOpts, globalOpts config.Controller) (ctrl.Manager, error) {
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kube client: %v", err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme.Scheme,
		Controller: globalOpts,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create manager: %w", err)
	}
	return AddRanksReconcilerToManager(mgr, kubeClient, opts)
}

type reconcilerBase struct {
	client.Client
	Scheme     *runtime.Scheme
	kubeClient *kubernetes.Clientset
	server     RanksServer
}

type ranksPodReconciler struct {
	*reconcilerBase
}

type ranksJobsetReconciler struct {
	*reconcilerBase
}

type ranksNodeReconciler struct {
	*reconcilerBase
}

func AddRanksReconcilerToManager(mgr ctrl.Manager, kubeClient *kubernetes.Clientset, opts RanksServerOpts) (ctrl.Manager, error) {
	rec := &reconcilerBase{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		kubeClient: kubeClient,
		server:     opts.ranksServer,
	}

	if opts.ranksServer != nil {
		opts.ranksServer.AddInformerClient(rec.Client)
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("pod").
		For(&corev1.Pod{}).
		Complete(&ranksPodReconciler{rec}); err != nil {
		return nil, err
	}
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("jobset").
		For(&jobsetv1alpha.JobSet{}).
		Complete(&ranksJobsetReconciler{rec}); err != nil {
		return nil, err
	}
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("node").
		For(&corev1.Node{}).
		Complete(&ranksNodeReconciler{rec}); err != nil {
		return nil, err
	}
	return mgr, nil
}

func (r *ranksPodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); apierrors.IsNotFound(err) {
		klog.Infof("pod %v unavailable: %v", req.NamespacedName, err)
		return ctrl.Result{}, nil
	} else if err != nil {
		// requeue & try again
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.server.HandlePod(&pod)
}

func (r *ranksJobsetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var jobset jobsetv1alpha.JobSet
	if err := r.Get(ctx, req.NamespacedName, &jobset); apierrors.IsNotFound(err) {
		log.FromContext(ctx).Info("jobset unavailable", "jobset", req.NamespacedName, "err", err)
		return ctrl.Result{}, nil
	} else if err != nil {
		// requeue & try again
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.server.HandleJobset(&jobset)
}

func (r *ranksNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var node corev1.Node
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		// requeue & try again
		return ctrl.Result{}, err
	}
	if node.GetDeletionTimestamp() != nil {
		r.server.RemoveNode(node.GetName())
		return ctrl.Result{}, nil
	}
	pool := getPoolLabelFromNode(&node)
	if pool == "" {
		klog.Infof("node missing labels: %v", req.NamespacedName)
		return ctrl.Result{}, nil
	}
	r.server.AddNodeToPool(node.GetName(), pool)
	return ctrl.Result{}, nil
}

func (r *ranksServer) PrepareToServe(port int) (*grpc.Server, func() error, error) {
	if port <= 0 {
		return nil, nil, fmt.Errorf("bad ranks server port %d", port)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("ranks server failed to listen: %v", err)
	}

	s := grpc.NewServer()
	proto.RegisterRanksServiceServer(s, r)

	return s, func() error { return s.Serve(lis) }, nil
}

func (r *ranksServer) AddNodeToPool(node, pool string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.pools[node] = pool
}

func (r *ranksServer) RemoveNode(node string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.pools, node)
}

func (r *ranksServer) AddInformerClient(k8sClient client.Client) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.k8sClient = k8sClient
}

func (r *ranksServer) HandlePod(pod *corev1.Pod) error {
	// In order to minimize contention and not delay receiving pod updates this
	// method doesn't lock the general mutex but only the pod list mutex, via
	// the r.addPod and r.addDeletedPod calls.

	if pod.DeletionTimestamp != nil {
		klog.Infof("deleting pod %s at %s", pod.GetUID(), pod.Status.PodIP)
		r.addDeletedPod(pod.GetUID())
		return nil
	}

	if info := r.createPodState(pod); info != nil {
		uid := info.uid
		node := info.node
		r.addPod(info)
		klog.Infof("added pod %s to %s", uid, node)
	} else {
		// Otherwise the pod was not a jobset pod or is not ready. In any case it's not requeued.
	}
	return nil
}

func (r *ranksServer) createPodState(pod *corev1.Pod) *podState {
	if pod.Spec.NodeName == "" || pod.Status.PodIP == "" {
		// Not scheduled or given IP yet, nothing for us to do.
		return nil
	}
	node := pod.Spec.NodeName

	ann := pod.GetAnnotations()
	jobsetName, found := ann[jobsetNameAnnotation]
	if !found {
		return nil
	}

	if !r.hasCheckpointingVolume(&pod.Spec) {
		return nil
	}

	slice, err := util.GetAnnotationInt(ann, jobsetJobIndexAnnotation)
	if err != nil {
		klog.Warningf("pod missing slice annotation: %s", pod.GetName())
		return nil
	}
	workerIdx, err := util.GetAnnotationInt(ann, jobsetCompletionIndexAnnotation)
	if err != nil {
		klog.Warningf("pod missing worker annotation: %s", pod.GetName())
		return nil
	}

	return &podState{
		node:      node,
		jobset:    types.NamespacedName{Name: jobsetName, Namespace: pod.GetNamespace()},
		slice:     slice,
		workerIdx: workerIdx,
		ip:        pod.Status.PodIP,
		uid:       pod.GetUID(),
		state:     proto.Lifecycle_PENDING,
		rank:      -1,
	}
}

func (r *ranksServer) HandleJobset(jobset *jobsetv1alpha.JobSet) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	name := types.NamespacedName{Name: jobset.GetName(), Namespace: jobset.GetNamespace()}
	if jobset.DeletionTimestamp != nil {
		// We don't delete the jobset since it may be recreated again.
		// TODO: time out old jobsets
		return nil
	}
	klog.Infof("updating or creating jobset %s", jobset.GetName())
	if len(jobset.Spec.ReplicatedJobs) != 1 {
		klog.Warningf("jobset %v has replicated job count %d, not 1. Assuming not a checkpointing job", jobset.GetName(), len(jobset.Spec.ReplicatedJobs))
		return nil
	}

	if !r.hasCheckpointingVolume(&jobset.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec) {
		klog.Infof("skipping %s as no checkpointing volume", jobset.GetName())
		return nil
	}
	numSlices := int(jobset.Spec.ReplicatedJobs[0].Replicas)
	sliceSize := 1
	if jobset.Spec.ReplicatedJobs[0].Template.Spec.Parallelism != nil {
		sliceSize = int(*jobset.Spec.ReplicatedJobs[0].Template.Spec.Parallelism)
	}
	r.jobsets[name] = &jobsetState{
		numSlices: numSlices,
		numJobs:   numSlices * sliceSize,
		sliceSize: sliceSize,
	}
	return nil
}

func (r *ranksServer) resetJobset(jobset types.NamespacedName) {
	toDelete := []types.UID{}
	for uid, pod := range r.pods {
		if pod.jobset != jobset {
			continue
		}
		toDelete = append(toDelete, uid)
	}
	for _, uid := range toDelete {
		delete(r.pods, uid)
	}
	delete(r.jobsetAssignments, jobset)
}

func (r *ranksServer) Update(ctx context.Context, req *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if req.State == proto.Lifecycle_SHUTDOWN {
		klog.Infof("shutdown %s / %s", req.Node, req.PodUid)
		delete(r.pods, types.UID(req.PodUid))
		// The return value from a shutdown is ignored by the driver
		return &proto.UpdateResponse{}, nil
	}

	newPods, deletedPods := r.swapPodLists()

	for _, uid := range deletedPods {
		klog.Infof("committing deleted pod %s", uid)
		delete(r.pods, uid)
	}

	for _, state := range newPods {
		if _, found := r.pods[state.uid]; !found {
			klog.Infof("committing new pod %s", state.uid)
			r.pods[state.uid] = state
		} else {
			klog.Infof("skipped duplicate commit of %s", state.uid)
		}
	}

	podInfo, found := r.pods[types.UID(req.PodUid)]
	if !found {
		klog.Infof("no pod found for %s, waiting for update: %s", req.Node, req.PodUid)
		return r.updateResponse(podInfo, proto.Lifecycle_PENDING, int(req.Rank), ""), nil
	}
	klog.Infof("updating %s/%d,%d", podInfo.node, req.Rank, podInfo.rank)
	podInfo.hasUpdate = true
	if podInfo.rank < 0 && req.Rank >= 0 {
		podInfo.rank = int(req.Rank)
	}

	jobset, found := r.jobsets[podInfo.jobset]
	if !found {
		klog.Infof("no jobset found for %s, waiting", req.Node)
		return r.updateResponse(podInfo, proto.Lifecycle_PENDING, int(req.Rank), ""), nil
	}

	if req.Jobset != jobsetString(podInfo.jobset) || req.JobsetShape != shapeString(jobset) {
		klog.Infof("missing or stale jobset in request, iterating: %s", req.Node)
		return r.updateResponse(podInfo, proto.Lifecycle_PENDING, int(req.Rank), ""), nil
	}

	if podInfo.state == proto.Lifecycle_COMPLETED {
		if podInfo.rank != int(req.Rank) {
			// Not clear if we can recover from this.
			return nil, status.Errorf(codes.Internal, "mismatch at completed pod of rank %s / %s", podInfo.node, podInfo.uid)
		}
		if podInfo.rank < 0 || podInfo.controllerIP == "" {
			// Also unclear what to do
			return nil, status.Errorf(codes.Internal, "pod completed, but has no rank or controller %s / %s", podInfo.node, podInfo.uid)
		}
		// Otherwise the driver must have missed a previous reply, so resend it
		klog.Infof("resending completion for %s", req.Node)
		return r.updateResponse(podInfo, podInfo.state, podInfo.rank, podInfo.controllerIP), nil
	}

	success, allAssigned, err := r.findAssignment(podInfo)
	if err != nil {
		return nil, err
	} else if !success {
		podInfo.state = proto.Lifecycle_PENDING
		klog.Infof("no assignment for %s (%d)", podInfo.node, podInfo.rank)
		newRank := int(req.Rank)
		if podInfo.rank >= 0 && podInfo.rank != int(req.Rank) {
			klog.Infof("rank updated for pod %s from %d to %d", podInfo.node, req.Rank, podInfo.rank)
			newRank = podInfo.rank
		}
		return r.updateResponse(podInfo, podInfo.state, newRank, ""), nil
	}

	if allAssigned {
		klog.Infof("all ranks assigned for %v, moving to completion", podInfo.jobset)
		podInfo.state = proto.Lifecycle_COMPLETED
		return r.updateResponse(podInfo, podInfo.state, podInfo.rank, podInfo.controllerIP), nil
	}

	// Otherwise this pod can move to (or stay) assigned while remaining pending pods are updated.
	if podInfo.state != proto.Lifecycle_ASSIGNED {
		klog.Infof("assigning %s=%d from computed assignment", req.Node, podInfo.rank)
	}
	podInfo.state = proto.Lifecycle_ASSIGNED
	return r.updateResponse(podInfo, podInfo.state, podInfo.rank, podInfo.controllerIP), nil
}

func (r *ranksServer) findAssignment(currPod *podState) (success, allAssigned bool, err error) {
	assignment := r.jobsetAssignments[currPod.jobset]
	if assignment.rankToPod == nil {
		assignment, err = r.computeAssignment(currPod)
		if err != nil {
			return false, false, err
		}
		if assignment.rankToPod == nil {
			return false, false, nil
		}
		r.jobsetAssignments[currPod.jobset] = assignment
	}
	if currPod.rank < 0 || currPod.rank >= len(assignment.rankToPod) || assignment.rankToPod[currPod.rank] != currPod.uid {
		// assignment is stale.
		delete(r.jobsetAssignments, currPod.jobset)
		return false, false, nil
	}

	allAssigned = true
	for i, uid := range assignment.rankToPod {
		pod, found := r.pods[uid]
		if !found {
			klog.Errorf("unknown rank %d pod in assignment %s", i, uid)
			return false, false, status.Errorf(codes.Internal, "unknown rank %d pod in assignment %s", i, uid)
		}
		if pod.rank != i {
			msg := fmt.Sprintf("inconsistent internal rank mapping: pod %s/%d is not expected %d", pod.node, pod.rank, i)
			klog.Error(msg)
			return false, false, status.Error(codes.Internal, msg)
		}
		if pod.state != proto.Lifecycle_ASSIGNED && pod.state != proto.Lifecycle_COMPLETED {
			allAssigned = false
			break
		}
	}
	return true, allAssigned, nil
}

func (r *ranksServer) computeAssignment(currPod *podState) (jobsetAssignment, error) {
	jobsetInfo, found := r.jobsets[currPod.jobset]
	if !found {
		klog.Errorf("no jobset %v found for %s", currPod.jobset, currPod.node)
		return jobsetAssignment{}, status.Errorf(codes.Internal, "jobset %v not found", currPod.jobset)
	}
	assigner := newAssigner(jobsetInfo.numSlices, jobsetInfo.sliceSize)
	podsWithRank := 0
	podsAdded := 0
	nodeToPod := map[string]*podState{}
	toDelete := []types.UID{}
	defer func() {
		for _, uid := range toDelete {
			delete(r.pods, uid)
		}
	}()
	for _, p := range r.pods {
		if p.jobset == currPod.jobset {
			if p.node == currPod.node && p.uid != currPod.uid {
				// Since currPod has just given an update, we know that its uid is not stale.
				klog.Infof("stale pod found on %s, deleting and continuing: %s", p.node, p.uid)
				toDelete = append(toDelete, p.uid)
				continue
			}
			if !p.hasUpdate {
				klog.Infof("waiting on update for %s %s", p.node, p.uid)
				return jobsetAssignment{}, nil
			}
			initialRank := p.slice*jobsetInfo.sliceSize + p.workerIdx
			pool, found := r.pools[p.node]
			if !found {
				klog.Infof("no pool found for node %s, will retry", p.node)
				return jobsetAssignment{}, nil
			}
			if _, found := nodeToPod[p.node]; found {
				// Since p.node != currPod.node, there is a collision with another pod. Wait until
				// it updates and is the current podInfo.
				klog.Infof("duplicate pods found on node %s, waiting for update to resolve", p.node)
				return jobsetAssignment{}, nil
			}
			nodeToPod[p.node] = p
			assigner.addNode(p.node, pool, p.rank, initialRank)
			podsAdded++
			if p.rank >= 0 {
				podsWithRank++
			}
		}
	}
	if podsAdded > jobsetInfo.numJobs {
		// Oops, found stale pods
		klog.Infof("stale pods found for %v, reseting", currPod.jobset)
		r.resetJobset(currPod.jobset)
		return jobsetAssignment{}, nil
	} else if podsAdded < jobsetInfo.numJobs {
		klog.Infof("waiting on remaining pods for %v (%d/%d; %d with rank)", currPod.jobset, podsAdded, jobsetInfo.numJobs, podsWithRank)
		return jobsetAssignment{}, nil
	}

	assignmentRanks := assigner.existingAssignment()
	var err error

	// Try #1: use the jobset index ranks.
	if assignmentRanks == nil {
		klog.Info("assigning from initial ranks")
		assignmentRanks, err = assigner.extendFromInitialRanks()
	}

	// Try #2: extend from current ranks, ignoring jobset ranks.
	if err != nil {
		klog.Infof("assigning from initial ranks failed, assigning arbitrarily: %v", err)
		assignmentRanks, err = assigner.extendFromCurrentRank()
	}

	// Try #3: current ranks are impossible, so clear and assign arbitrarily, considering
	// only the slice topology.
	if err != nil {
		klog.Errorf("no rank assignment: %v", err)
		klog.Errorf("clearing current ranks and making arbitrary assignment: %v", err)
		assigner.clearCurrentRanks()
		assignmentRanks, err = assigner.extendFromCurrentRank()
	}

	// These should only be internal errors or some fundamental user configuration problem.
	if err != nil {
		return jobsetAssignment{}, status.Errorf(codes.InvalidArgument, "no valid rank assignment possible: %v", err)
	}

	if len(assignmentRanks) != jobsetInfo.numJobs {
		return jobsetAssignment{}, status.Errorf(codes.Internal, "assignment length mismatch for %v: %d vs %d", currPod.jobset, len(assignmentRanks), jobsetInfo.numJobs)
	}

	controllerPod, found := nodeToPod[assignmentRanks[0].name]
	if !found {
		return jobsetAssignment{}, status.Errorf(codes.Internal, "nodeToPod missing controller %s", assignmentRanks[0].name)
	}
	controllerIP := controllerPod.ip
	if controllerIP == "" {
		klog.Infof("ranks assigned, but controller has not yet received IP")
		return jobsetAssignment{}, nil
	}
	rankToPod := make([]types.UID, len(assignmentRanks))
	podToRank := map[types.UID]int{}
	for i := range len(assignmentRanks) {
		p, found := nodeToPod[assignmentRanks[i].name]
		if !found {
			return jobsetAssignment{}, status.Errorf(codes.Internal, "nodeToPod missing update %s", assignmentRanks[i].name)
		}
		if p.rank >= 0 && p.rank != i {
			klog.Infof("on %s overridding existing rank %d to %d", p.node, p.rank, i)
		}
		klog.Infof("updating %s rank to %d", p.node, i)
		p.rank = i
		p.controllerIP = controllerIP
		rankToPod[i] = p.uid
		podToRank[p.uid] = i
	}

	return jobsetAssignment{rankToPod, podToRank}, nil
}

func (r *ranksServer) updateResponse(podInfo *podState, state proto.Lifecycle, rank int, controller string) *proto.UpdateResponse {
	rsp := &proto.UpdateResponse{
		NewState:     state,
		Rank:         int32(rank),
		ControllerIp: controller,
	}
	if podInfo != nil {
		rsp.Jobset = jobsetString(podInfo.jobset)
		rsp.JobsetShape = shapeString(r.jobsets[podInfo.jobset])
	}
	return rsp
}

func (r *ranksServer) hasCheckpointingVolume(spec *corev1.PodSpec) bool {
	for _, v := range spec.Volumes {
		if v.VolumeSource.CSI != nil && v.VolumeSource.CSI.Driver == r.driverName {
			return true
		}
	}
	return false
}

func (r *ranksServer) addPod(state *podState) {
	if _, found := r.podsToDelete[state.uid]; found {
		klog.Errorf("unexpected pod added after being deleted. Assuming deletion takes precedence. UID: %s", state.uid)
		return
	}

	r.podListMutex.Lock()
	defer r.podListMutex.Unlock()
	r.podsToAdd[state.uid] = state
}

func (r *ranksServer) addDeletedPod(uid types.UID) {
	r.podListMutex.Lock()
	defer r.podListMutex.Unlock()
	r.podsToDelete[uid] = true
	delete(r.podsToAdd, uid)
}

func (r *ranksServer) swapPodLists() (newPods []*podState, deletedPods []types.UID) {
	r.podListMutex.Lock()
	defer r.podListMutex.Unlock()

	newPods = slices.Collect(maps.Values(r.podsToAdd))
	r.podsToAdd = map[types.UID]*podState{}
	deletedPods = slices.Collect(maps.Keys(r.podsToDelete))
	r.podsToDelete = map[types.UID]bool{}
	return
}

func jobsetString(jobset types.NamespacedName) string {
	return fmt.Sprintf("%s/%s", jobset.Namespace, jobset.Name)
}

func shapeString(state *jobsetState) string {
	if state == nil {
		return ""
	}
	return fmt.Sprintf("%dx%d", state.numSlices, state.sliceSize)
}
