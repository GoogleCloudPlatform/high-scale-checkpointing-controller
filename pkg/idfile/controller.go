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

// Package idfile implements the k8s-determined process id mapping.
// See the following for motivation and design: https://docs.google.com/document/d/1xTbXZoXb812JM0kp_hnM4PANN06iNaPqtvWwb59PqP4/edit?resourcekey=0-18TL28yPZuVIbuBsh99u9g&tab=t.0#heading=h.cszq41uu0kvz
package idfile

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	jobsetNameAnnotation            = "jobset.sigs.k8s.io/jobset-name"
	jobsetJobIndexAnnotation        = "jobset.sigs.k8s.io/job-index"
	jobsetCompletionIndexAnnotation = "batch.kubernetes.io/job-completion-index"
	jobsetRestartAttemptAnnotation  = "jobset.sigs.k8s.io/restart-attempt"
	nodePoolLabel                   = "cloud.google.com/gke-nodepool"

	WaitingForRankRequeueTime = 500 * time.Millisecond
)

type ControllerOpts struct {
	// Namespace is the controller namespace, where configuration like
	// the mapping ConfigMap live. It is not the job namespace; all
	// namespaces will be watched for workers.
	Namespace string

	// DriverName is the name used in the volume source, eg phase1-checkpoint.csi.storage.gke.io
	DriverName string
}

type reconcilerNodeInfo struct {
	jobset types.NamespacedName
	pool   string
}

type reconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	kubeClient *kubernetes.Clientset
	opts       ControllerOpts

	nodeInfo map[string]reconcilerNodeInfo
	idfiles  map[types.NamespacedName]IdFileServer // jobset -> server

	// Each handler (jobset, node, pod) runs in its own thread. They all access
	// the same shared state so must run exclusively.
	handlerMutex sync.Mutex

	initialized bool
}

type nodeReconciler struct {
	*reconciler
}

type jobsetReconciler struct {
	*reconciler
}

// Init should be run from the main init() function, or early in testing.
func Init() {
	// This should get all core objects.
	utilruntime.Must(scheme.AddToScheme(scheme.Scheme))
	utilruntime.Must(jobsetv1alpha.AddToScheme(scheme.Scheme))
}

func NewIdFileManager(cfg *rest.Config, opts ControllerOpts, globalOpts config.Controller) (ctrl.Manager, error) {
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
	return AddIdFileReconcilerToManager(mgr, kubeClient, opts)
}

func AddIdFileReconcilerToManager(mgr ctrl.Manager, kubeClient *kubernetes.Clientset, opts ControllerOpts) (ctrl.Manager, error) {
	rec := &reconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		kubeClient: kubeClient,
		opts:       opts,
		nodeInfo:   map[string]reconcilerNodeInfo{},
		idfiles:    map[types.NamespacedName]IdFileServer{},
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("pod").
		// Use For(&corev1.Pod{}) instead
		Watches(&corev1.Pod{}, &handler.EnqueueRequestForObject{}).
		Complete(rec); err != nil {
		return nil, err
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("node").
		// Use For(&corev1.Node{}) instead?
		Watches(&corev1.Node{}, &handler.EnqueueRequestForObject{}).
		Complete(&nodeReconciler{rec}); err != nil {
		return nil, err
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("jobset").
		// Use For(&Jobset{}) instead?
		Watches(&jobsetv1alpha.JobSet{}, &handler.EnqueueRequestForObject{}).
		Complete(&jobsetReconciler{rec}); err != nil {
		return nil, err
	}

	return mgr, nil
}

// initializeCluster must be called after the informers have synced. Controller-runtime
// doesn't export such a signal, unfortunately, so this must be called the first time any
// reconcile happens.
func (r *reconciler) initializeCluster(ctx context.Context) error {
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes); err != nil {
		return err
	}
	nodePools := map[string]string{}
	for _, node := range nodes.Items {
		pool := getPoolLabelFromNode(&node)
		if pool == "" {
			return fmt.Errorf("no pool found for node %s", node.GetName())
		}
		nodePools[node.GetName()] = pool
	}

	configMaps, err := r.kubeClient.CoreV1().ConfigMaps(r.opts.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, cm := range configMaps.Items {
		// We only use the node mappings to reconstruct local state; the
		// coordinator info is implicit. In a perfect world we'd have typed
		// config maps or a CRD; here if the mapping cannot be parsed we just
		// assume it's not a node mapping.
		info, err := parseNodeMapping(&cm)
		if err != nil {
			// not a node mapping, ignore
			log.FromContext(ctx).Info("ignoring non-mapping configmap %s")
			continue
		}

		node := cm.GetName()
		pool, found := nodePools[node]
		if !found {
			log.FromContext(ctx).Error(nil, "stale configmap", "node", node)
			if err := r.Delete(ctx, &cm); err != nil {
				log.FromContext(ctx).Error(err, "cannot delete stale configmap")
			}
			continue
		}

		if err := r.verifyJobset(ctx, info.Job); err != nil {
			return err
		}
		svr := r.idfiles[info.Job]
		_, err = svr.AddNode(node, pool, info.ProcessIndex)
		if err != nil {
			return err
		}
		_, err = svr.UpdateNode(node, info.Address, info.Generation, info.PodName, info.PodUID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.handlerMutex.Lock()
	defer r.handlerMutex.Unlock()

	if !r.initialized {
		if err := r.initializeCluster(ctx); err != nil {
			log.FromContext(ctx).Error(err, "handle pod initialization")
			return ctrl.Result{}, err
		}
		r.initialized = true
	}

	log := log.FromContext(ctx)
	log.Info("pod reconcile", "pod", req.NamespacedName)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); apierrors.IsNotFound(err) {
		log.Info("pod unavailable, nothing to do", "pod", req.NamespacedName, "err", err)
		return ctrl.Result{}, nil
	} else if err != nil {
		// Requeue the presumably temporary error.
		return ctrl.Result{}, err
	}

	if pod.DeletionTimestamp != nil {
		return r.handlePodDelete(ctx, &pod)
	}

	return r.handlePod(ctx, &pod)
}

func (r *nodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.handlerMutex.Lock()
	defer r.handlerMutex.Unlock()

	if !r.initialized {
		if err := r.initializeCluster(ctx); err != nil {
			log.FromContext(ctx).Error(err, "handle pod initialization")
			return ctrl.Result{}, err
		}
		r.initialized = true
	}

	log := log.FromContext(ctx)
	nodeName := req.NamespacedName.Name
	log.Info("node reconcile", "node", nodeName)

	var node corev1.Node
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return r.handleNodeDelete(ctx, nodeName)
		}
		return ctrl.Result{}, err // presumbably this is a temporary error.
	}

	if node.DeletionTimestamp != nil {
		return r.handleNodeDelete(ctx, nodeName)
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
			return r.handleNodeDelete(ctx, nodeName)
		}
	}

	log.Info("Node still healthy", "node", nodeName)
	return ctrl.Result{}, nil
}

func (r *jobsetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.handlerMutex.Lock()
	defer r.handlerMutex.Unlock()

	if !r.initialized {
		if err := r.initializeCluster(ctx); err != nil {
			log.FromContext(ctx).Error(err, "handle pod initialization")
			return ctrl.Result{}, err
		}
		r.initialized = true
	}

	jobsetName := req.NamespacedName

	var jobset jobsetv1alpha.JobSet
	if err := r.Get(ctx, jobsetName, &jobset); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	} else if apierrors.IsNotFound(err) || jobset.DeletionTimestamp != nil {
		return r.handleJobsetDelete(ctx, jobsetName)
	}

	return ctrl.Result{}, nil
}

func (r *reconciler) handleNodeDelete(ctx context.Context, nodeName string) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	info, found := r.nodeInfo[nodeName]
	if !found {
		log.Info("deleted node not found (ignored)", "node", nodeName)
		return ctrl.Result{}, nil
	}

	svr, found := r.idfiles[info.jobset]
	if !found {
		log.Info("jobset not found, unable to delete", "node", nodeName)
		return ctrl.Result{}, nil
	}

	// While all nodes in the slice will eventually be deleted, it might be
	// happening in a strange order. So only delete this node.
	svr.RemoveNode(nodeName, info.pool)
	if err := svr.DeleteNodeMapping(ctx, nodeName); err != nil {
		// The mapping may have been deleted by the driver already.
		log.Error(err, "node removed, but mapping not deleted (ignored)", "node", nodeName)
	}

	delete(r.nodeInfo, nodeName)

	return ctrl.Result{}, nil
}

func (r *reconciler) handleJobsetDelete(ctx context.Context, jobset types.NamespacedName) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	log.Info("jobset delete", "node", jobset)

	svr, found := r.idfiles[jobset]
	if !found {
		log.Info("jobset not found, nothing to delete", "jobset", jobset)
		return ctrl.Result{}, nil
	}
	toDelete := []string{}
	for node, info := range r.nodeInfo {
		if info.jobset != jobset {
			continue
		}
		svr.RemoveNode(node, info.pool)
		if err := svr.DeleteNodeMapping(ctx, node); err != nil {
			log.Error(err, "remove mapping after jobset delete", "node", node, "jobset", info.jobset)
			// continue to try to delete other nodes. We will delete the jobset from internal structures so
			// there will be nothing to retry.
		}
		log.Info("deleted node after jobset", "node", node, "jobset", info.jobset)
		toDelete = append(toDelete, node)
	}
	for _, node := range toDelete {
		delete(r.nodeInfo, node)
	}

	if err := svr.ResetCoordinator(ctx); err != nil {
		log.Error(err, "coordinator reset during node delete")
		return ctrl.Result{}, err
	}
	delete(r.idfiles, jobset)

	return ctrl.Result{}, nil
}

// existingRankForPod returns a rank for a pod for an existing node, or -1 if
// the pod does not have a rank for the node it's scheduled on. An error is
// returned only in an exceptional circumstance.
func (r *reconciler) existingRankForPod(ctx context.Context, svr IdFileServer, pod *corev1.Pod, node string) (int, error) {
	info, found := svr.GetNode(node)
	if !found {
		return -1, nil
	}
	log.FromContext(ctx).Info("using existing rank for pod", "pod", pod.GetName(), "node", node, "rank", info.ProcessIndex)
	return info.ProcessIndex, nil
}

// rankForPod determines the best rank for a pod, or -1 if none can be found (in
// which case we should requeue and wait for node deletion). On an error the
// reconcile should exit. The rank is not committed, ie no data structures are
// updated based on this choice.
func (r *reconciler) rankForPod(ctx context.Context, svr IdFileServer, jobset types.NamespacedName, pod *corev1.Pod, node, pool string) (int, error) {
	idx, err := r.existingRankForPod(ctx, svr, pod, node)
	if err != nil {
		return -1, err
	}
	if idx >= 0 {
		return idx, nil
	}

	ann := pod.GetAnnotations()
	slice, err := util.GetAnnotationInt(ann, jobsetJobIndexAnnotation)
	if err != nil {
		return -1, fmt.Errorf("pod missing slice annotation")
	}
	workerIdxInSlice, err := util.GetAnnotationInt(ann, jobsetCompletionIndexAnnotation)
	if err != nil {
		return -1, fmt.Errorf("pod missing worker annotation")
	}

	nodeSlice := svr.NodeSlice(node, pool)
	if nodeSlice >= 0 {
		log.FromContext(ctx).Info("trying existing slice", "pod", pod.GetName(), "node", node, "nodeslice", nodeSlice, "podslice", slice)
		slice = nodeSlice
	} else {
		log.FromContext(ctx).Info("trying jobset slice", "pod", pod.GetName(), "node", node, "podslice", slice)
	}

	idx, err = svr.FindIndexInSlice(node, pool, slice, workerIdxInSlice)
	if err != nil {
		return -1, err
	}
	if idx >= 0 || nodeSlice >= 0 {
		// Either an index was found, or there was no free index in the slice
		// that the node is already in.
		if idx < 0 {
			log.FromContext(ctx).Error(nil, "no index found for fixed slice", "pod", pod.GetName(), "node", node, "slice", nodeSlice)
		}
		return idx, nil
	}

	// The jobset slice is in use, but the node is the first for its pool and
	// perhaps there is another free slice.
	log.FromContext(ctx).Info("trying alternate", "pod", pod.GetName(), "node", node, "podslice", slice, "nodeslice", nodeSlice)
	availableSlices := map[int]bool{}
	for i := 0; i < r.idfiles[jobset].NumSlices(); i++ {
		availableSlices[i] = true
	}
	for n := range svr.Nodes() {
		info, found := r.nodeInfo[n]
		if !found {
			log.FromContext(ctx).Error(nil, "node found, ignoring for slice", "node", n, "jobset", jobset)
			continue
		}
		delete(availableSlices, svr.NodeSlice(n, info.pool))
	}
	slice = -1
	for s := range availableSlices {
		if slice == -1 || s < slice {
			slice = s
		}
	}
	if slice == -1 {
		// no free slices
		log.FromContext(ctx).Error(nil, "no free slices", "pod", pod.GetName(), "node", node)
		return -1, nil
	}
	idx, err = svr.FindIndexInSlice(node, pool, slice, workerIdxInSlice)
	if err != nil {
		return -1, err
	}
	target := slice*svr.SliceSize() + workerIdxInSlice
	if idx != target {
		return -1, fmt.Errorf("inconsistency: slice %d is unused, but chose %d instead of %d", slice, idx, target)

	}
	return idx, nil
}

func (r *reconciler) handlePod(ctx context.Context, pod *corev1.Pod) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	podName := pod.GetName()
	ann := pod.GetAnnotations()
	jobsetName, found := ann[jobsetNameAnnotation]
	if !found {
		return ctrl.Result{}, nil
	}
	jobset := types.NamespacedName{Name: jobsetName, Namespace: pod.GetNamespace()}

	if pod.Spec.NodeName == "" {
		return ctrl.Result{}, nil
	}

	if err := r.verifyJobset(ctx, jobset); err != nil {
		return ctrl.Result{}, err
	}
	svr := r.idfiles[jobset]

	if pod.Status.PodIP == "" {
		return ctrl.Result{}, nil
	}

	node := pod.Spec.NodeName
	pool := r.getNodePoolLabel(ctx, node)
	if pool == "" {
		return ctrl.Result{}, fmt.Errorf("no node pool label found for node %s", node)
	}

	foundVolume := false
	for _, volume := range pod.Spec.Volumes {
		if volume.VolumeSource.CSI != nil && volume.VolumeSource.CSI.Driver == r.opts.DriverName {
			foundVolume = true
			break
		}
	}
	if !foundVolume {
		log.Info("pod does not have a driver volume", "pod", podName, "jobset", jobset, "driver", r.opts.DriverName, "volumes", pod.Spec.Volumes)
		return ctrl.Result{}, nil
	}

	rank, err := r.rankForPod(ctx, svr, jobset, pod, node, pool)
	if err != nil {
		log.Error(nil, "error finding rank", "pod", pod.GetName(), "node", node, "pool", pool)
		return ctrl.Result{}, err
	}
	if rank < 0 {
		log.Info("No rank found, retrying", "pod", pod.GetName(), "node", node, "jobset", jobset, "state", svr.DebugState())
		return ctrl.Result{RequeueAfter: WaitingForRankRequeueTime}, nil
	}

	// A rank has been found, update as necessary.
	podGeneration, err := util.GetAnnotationInt(ann, jobsetRestartAttemptAnnotation)
	if err != nil {
		log.Error(err, "bad pod, skipping", "pod", podName, "jobset", jobset)
		return ctrl.Result{}, nil
	}

	info, found := svr.GetNode(node)
	if !found || info.ProcessIndex != rank {
		info, err = svr.AddNode(node, pool, rank)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	if info, found := r.nodeInfo[node]; found && info.jobset != jobset {
		log.Error(nil, "stale jobset on node (overwritten & continuing)", "node", node, "jobset", jobset, "stale-jobset", info.jobset)
	}
	r.nodeInfo[node] = reconcilerNodeInfo{jobset, pool}

	// These are unset if AddNode has just been called.
	if info.Address != pod.Status.PodIP || info.Generation != podGeneration {
		if podGeneration < info.Generation {
			log.Error(nil, "surprising low pod generation", "node", node, "jobset", jobset, "pod", pod.GetName(), "info", info, "pod-generation", podGeneration)
		}
		var err error
		info, err = svr.UpdateNode(node, pod.Status.PodIP, podGeneration, pod.GetName(), pod.GetUID())
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := writeNodeMapping(ctx, r.kubeClient, r.opts.Namespace, info); err != nil {
			return ctrl.Result{}, err
		}
		if err := svr.UpdateCoordinatorInfo(ctx); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("updated mapping", "pod", pod.GetName(), "node", node, "ip", pod.Status.PodIP, "info", info)
	}

	return ctrl.Result{}, nil
}

func (r *reconciler) handlePodDelete(ctx context.Context, pod *corev1.Pod) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if pod.Spec.NodeName == "" {
		log.Info("deleted pod missing node, ignored", "pod", pod.GetName())
		return ctrl.Result{}, nil
	}

	info, found := r.nodeInfo[pod.Spec.NodeName]
	if !found {
		log.Info("deleted pod node unknown, ignored", "pod", pod.GetName(), "node", pod.Spec.NodeName)
		return ctrl.Result{}, nil
	}

	ann := pod.GetAnnotations()
	jobsetName, found := ann[jobsetNameAnnotation]
	if !found {
		log.Info("skipped as not a jobset worker", "pod", pod.GetName())
		return ctrl.Result{}, nil
	}
	jobset := types.NamespacedName{Name: jobsetName, Namespace: pod.GetNamespace()}

	if jobset != info.jobset {
		log.Error(nil, "deleted pod does not match jobset for node (will delete anyway)", "pod", pod.GetName(), "node", pod.Spec.NodeName, "pod-jobset", jobset, "node-jobset", info.jobset)
	}

	podGeneration, err := util.GetAnnotationInt(ann, jobsetRestartAttemptAnnotation)
	if err != nil {
		log.Error(err, "deleted pod missing generation, ignoring", "pod", pod.GetName(), "node", pod.Spec.NodeName)
		return ctrl.Result{}, nil
	}

	svr, found := r.idfiles[info.jobset]
	if !found {
		log.Info("unknown jobset for deleted pod, skipping", "pod", pod.GetName(), "node", pod.Spec.NodeName)
		return ctrl.Result{}, nil
	}

	mapping, found := svr.GetNode(pod.Spec.NodeName)
	if !found {
		log.Info("unknown mapping for deleted pod, skipping", "pod", pod.GetName(), "node", pod.Spec.NodeName)
		return ctrl.Result{}, nil
	}
	if mapping.Generation > podGeneration || mapping.PodName != pod.GetName() {
		log.Info("stale pod ignored", "pod", pod.GetName(), "pod-generation", podGeneration, "node", mapping)
		return ctrl.Result{}, nil
	}

	mapping, err = svr.UpdateNode(pod.Spec.NodeName, mapping.Address, -1, "", types.UID(""))
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := writeNodeMapping(ctx, r.kubeClient, r.opts.Namespace, mapping); err != nil {
		return ctrl.Result{}, err
	}
	if err := svr.ResetCoordinator(ctx); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("reset generation for deleted pod", "pod", pod.GetName(), "node", mapping)
	return ctrl.Result{}, nil
}

func (r *reconciler) verifyJobset(ctx context.Context, jobsetName types.NamespacedName) error {
	_, found := r.idfiles[jobsetName]
	if found {
		return nil
	}

	var jobset jobsetv1alpha.JobSet
	if err := r.Get(ctx, jobsetName, &jobset); err != nil {
		return fmt.Errorf("cannot get jobset %v: %w", jobsetName, err)
	}

	if len(jobset.Spec.ReplicatedJobs) != 1 {
		return fmt.Errorf("bad jobset %v, replicated job count is %d, not 1", jobsetName, len(jobset.Spec.ReplicatedJobs))
	}

	numSlices := int(jobset.Spec.ReplicatedJobs[0].Replicas)
	sliceSize := 1
	if jobset.Spec.ReplicatedJobs[0].Template.Spec.Parallelism != nil {
		sliceSize = int(*jobset.Spec.ReplicatedJobs[0].Template.Spec.Parallelism)
	}

	svr, err := NewIdFileServer(IdFileServerOpts{
		KubeClient: r.kubeClient,
		Namespace:  r.opts.Namespace,
		NumWorkers: numSlices * sliceSize,
		SliceSize:  sliceSize,
		Job:        jobsetName,
	})
	if err != nil {
		return fmt.Errorf("cannot create idfile for %v: %w", jobsetName, err)
	}
	r.idfiles[jobsetName] = svr
	return nil
}

func (r *reconciler) getNodePoolLabel(ctx context.Context, name string) string {
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: name}, &node); err != nil {
		log.FromContext(ctx).Error(err, "getting node pool label", "node", name)
		return ""
	}
	label := getPoolLabelFromNode(&node)
	if label == "" {
		log.FromContext(ctx).Error(nil, "no node pool label", "node", name)
	}
	return label
}

func getPoolLabelFromNode(node *corev1.Node) string {
	labels := node.GetLabels()
	if labels == nil {
		return ""
	}
	return labels[nodePoolLabel]
}
