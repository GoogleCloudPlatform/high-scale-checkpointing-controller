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

package multitiercontroller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	checkpoint "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/apis/checkpointing.gke.io/v1"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/idfile"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	// CheckpointConfiguration Customer Resource Definition information
	cpcGroup    = "checkpointing.gke.io"
	cpcKind     = "CheckpointConfiguration"
	cpcVersion  = "v1"
	cpcResource = "checkpointconfigurations"

	csiPrefix       = "multitier-driver"
	finalizerSuffix = "checkpoint-target-protection"
	gcsVolumeName   = "gcs"
)

var (
	cpcGK = schema.GroupKind{
		Group: cpcGroup,
		Kind:  cpcKind,
	}
	CPCGVR = schema.GroupVersionResource{
		Group:    cpcGK.Group,
		Version:  cpcVersion,
		Resource: cpcResource,
	}
)

type ControllerOptions struct {
	MasterURL               string
	Kubeconfig              string
	MetricsAddress          string
	MetricsPath             string
	ManagedNamespace        string
	CSIServiceAccountName   string
	CSIDriverName           string
	ImageConfigMapName      string
	LocalDeploy             bool
	DebugBackup             bool
	LeaderElection          bool
	LeaderElectionNamespace string
	LeaderElectionID        string
	LeaseDuration           *time.Duration
	RenewDeadline           *time.Duration
	RetryPeriod             *time.Duration
	HealthProbeBindAddress  string
	LegacyIdFile            bool
	RanksServerPort         int
	//gke metrics collector
	EnableMetricsCollector  bool
	MetricsCollectorConfig  *MetricsCollectorConfig
	EnableUptimeController  bool
	UptimeControllerBackoff time.Duration
}

type reconciler struct {
	client.Client
	scheme    *runtime.Scheme
	finalizer string
	csiConfig csiconfig
	gvr       schema.GroupVersionResource
	k8sClient *kubernetes.Clientset
	mcConfig  *MetricsCollectorConfig
	recorder  record.EventRecorder
}

type csiconfig struct {
	csiNamespace          string
	csiServiceAccountName string
	csiDriverName         string
	imageConfigMapName    string
	localDeploy           bool
	debugBackup           bool
	legacyIdFile          bool
	ranksServerPort       int
}

type MetricsCollectorConfig struct {
	ProjectNumber     string
	ClusterLocation   string
	ClusterName       string
	KSATokenFileName  string
	Audience          string
	TokenURL          string
	TokenMode         string
	KSAFilePath       string
	ADCFilePath       string
	ConsumerProjectID string
	ADCVolumeMountDir string
	KSAVolumeMountDir string
	GKEHostName       string
}

func Init() {
	klog.Infof("adding schemes used by idfile controllers")
	idfile.Init()

	klog.Infof("adding core schemes and cpc")
	utilruntime.Must(corev1.AddToScheme(scheme.Scheme))
	utilruntime.Must(checkpoint.AddToScheme(scheme.Scheme))
}

func NewControllerManager(cfg *rest.Config, opts ControllerOptions) (ctrl.Manager, error) {
	if opts.RanksServerPort <= 0 {
		return nil, fmt.Errorf("bad ranks server port %d", opts.RanksServerPort)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		Metrics: metricsserver.Options{
			BindAddress:   opts.MetricsAddress,
			SecureServing: true,
		},
		LeaderElection:          opts.LeaderElection,
		LeaderElectionID:        opts.LeaderElectionID,
		LeaderElectionNamespace: opts.LeaderElectionNamespace,
		RetryPeriod:             opts.RetryPeriod,
		LeaseDuration:           opts.LeaseDuration,
		RenewDeadline:           opts.RenewDeadline,
		HealthProbeBindAddress:  opts.HealthProbeBindAddress,

		// The cache is shared by all reconcilers managed by mgr.
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&v1.DaemonSet{}: {
					Namespaces: map[string]cache.Config{
						opts.ManagedNamespace: {},
					},
				},
				&corev1.Pod{}: {
					Namespaces: map[string]cache.Config{
						opts.ManagedNamespace: {},
					},
					Transform: uptimeReconcilerPodTrimmer,
				},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create manager: %w", err)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	csiconfig := csiconfig{
		csiNamespace:          opts.ManagedNamespace,
		csiServiceAccountName: opts.CSIServiceAccountName,
		imageConfigMapName:    opts.ImageConfigMapName,
		localDeploy:           opts.LocalDeploy,
		debugBackup:           opts.DebugBackup,
		csiDriverName:         opts.CSIDriverName,
		legacyIdFile:          opts.LegacyIdFile,
		ranksServerPort:       opts.RanksServerPort,
	}

	checkpoint.AddToScheme(mgr.GetScheme())

	broadcaster := record.NewBroadcaster()
	broadcaster.StartStructuredLogging(0)
	broadcaster.StartRecordingToSink(&typedv1.EventSinkImpl{
		Interface: kubeClient.CoreV1().Events(""),
	})
	recorder := broadcaster.NewRecorder(mgr.GetScheme(), corev1.EventSource{Component: "highscalecheckpointing"})

	rec := &reconciler{
		scheme:    mgr.GetScheme(),
		Client:    mgr.GetClient(),
		k8sClient: kubeClient,
		gvr:       CPCGVR,
		csiConfig: csiconfig,
		finalizer: cpcGK.Group + "/" + finalizerSuffix,
		recorder:  recorder,
	}
	if opts.EnableMetricsCollector {
		klog.Infof("metrics collector enabled, creating config..")
		rec.mcConfig = opts.MetricsCollectorConfig
	}

	cpc := &checkpoint.CheckpointConfiguration{}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(cpc).
		Complete(rec); err != nil {
		return nil, err
	}

	if opts.EnableUptimeController {
		uptimeReconciler := &UptimeReconciler{
			Client:                   mgr.GetClient(),
			Scheme:                   mgr.GetScheme(),
			k8sClient:                kubeClient,
			csiConfig:                csiconfig,
			resumeReconcileTimestamp: time.Now(),
			backoffDuration:          opts.UptimeControllerBackoff,
			gracePeriod:              5 * time.Minute,
		}
		if err := ctrl.NewControllerManagedBy(mgr).
			For(&corev1.Pod{}).
			Complete(uptimeReconciler); err != nil {
			return nil, err
		}
	}

	idfileOpts := idfile.ControllerOpts{
		Namespace:  opts.ManagedNamespace,
		DriverName: opts.CSIDriverName,
	}
	if opts.LegacyIdFile {
		klog.Infof("using legacy idfile")
		mgr, err = idfile.AddIdFileReconcilerToManager(mgr, kubeClient, idfileOpts)
	} else {
		klog.Infof("using ranks server which is deployed separately")
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("unable to set up ready check: %w", err)
	}

	klog.Infof("controller manager created")
	return mgr, err
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("reconcilling", "checkpointconfiguration", req.NamespacedName)

	var err error

	// Get the CheckpointConfiguration
	var cpc checkpoint.CheckpointConfiguration
	err = r.Get(ctx, req.NamespacedName, &cpc, &client.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("checkpointconfiguration no longer exists", "checkpointconfiguration", fmt.Sprintf("%s/%s", req.Namespace, req.Name))
			return ctrl.Result{}, nil
		}

		log.Error(err, "failed to get checkpointconfiguration", "checkpointconfiguration", fmt.Sprintf("%s/%s", req.Namespace, req.Name))
		return ctrl.Result{}, err
	}

	// Check if the csi driver already exists on the selected nodes
	daemonSetName := fmt.Sprintf("%s-%s", csiPrefix, cpc.UID)
	ds := &v1.DaemonSet{}
	err = r.Get(ctx, types.NamespacedName{Namespace: r.csiConfig.csiNamespace, Name: daemonSetName}, ds)
	if err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "failed to check for existing CSI daemonset", "daemonset", fmt.Sprintf("%s/%s", r.csiConfig.csiNamespace, daemonSetName))
			return ctrl.Result{}, err
		} else {
			log.Info("pre-existing daemonset not found", "daemonset", types.NamespacedName{Namespace: r.csiConfig.csiNamespace, Name: daemonSetName})
			ds = nil
		}
	}

	if ds != nil && ds.GetDeletionTimestamp() != nil {
		ds = nil
	}

	// If the CheckpointConfiguration gets deleted. Delete the corresponding CSI DaemonSet
	if cpc.GetDeletionTimestamp() != nil {
		requeue, err := r.handleDeletedCheckpointConfiguration(ctx, &cpc, ds)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: requeue}, nil
	}

	// If no daemonset, check for conflict with existing daemonsets before
	// creating one. This could be an expensive check (as it lists all nodes and
	// pods in in the driver namespace), so only do it if a daemonset would be
	// created.
	if ds == nil {
		if conflict, err := r.checkDaemonsetConflict(ctx, &cpc); conflict || err != nil {
			if err != nil {
				log.Error(err, "failed to check for daemonset conflict")
				return ctrl.Result{}, err
			}
			if conflict {
				log.Info("daemonset conflict found", "cpc", cpc)
				r.recorder.Eventf(
					&cpc, // The CRD object the event is about
					corev1.EventTypeWarning,
					"Conflict",
					"CheckpointConfiguration %s overlaps with existing configuration",
					cpc.GetName(),
				)
				// Requeue to wait for crd to be cleaned up.
				return ctrl.Result{Requeue: true}, nil
			}
		}
		log.Info("No conflict with existing daemonset found, proceeding with creation", "cpc", cpc.Name)

		// Inject customer provided fields to DaemonSet spec
		// TODO: Implement logic for EnableExternalJaxProcessIds
		p, err := newPreparer(ctx, r.k8sClient, &r.csiConfig, &cpc, r.mcConfig)
		if err != nil && !errors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("could not create preparer: %w. checkpointconfiguration %s/%s", err, req.Namespace, req.Name)
		}
		daemonSet := p.prepareCSIDaemonSet(daemonSetName)

		// Add finalizer to the CheckpointConfiguration before create the CSI DaemonSet
		err = r.addFinalizer(ctx, cpc)
		if err != nil {
			log.Error(err, "failed to add finalizer from checkpointconfiguration", "checkpointconfiguration", fmt.Sprintf("%s/%s", req.Namespace, req.Name))
			return ctrl.Result{}, err
		}

		log.Info("Creating CSI Driver", "daemonset", fmt.Sprintf("%s/%s", r.csiConfig.csiNamespace, daemonSetName))
		err = r.Create(ctx, &daemonSet)
		if err != nil {
			log.Error(err, "failed to create CSI Driver", "daemonset", fmt.Sprintf("%s/%s", r.csiConfig.csiNamespace, daemonSetName))
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil

}

func (r *reconciler) handleDeletedCheckpointConfiguration(ctx context.Context, cpc *checkpoint.CheckpointConfiguration, ds *v1.DaemonSet) (bool, error) {
	log := log.FromContext(ctx)
	if ds != nil {
		// If the daemonset hasn't been deleted, delete it.
		log.Info("Deleting CSI Driver", "daemonset", fmt.Sprintf("%s/%s", r.csiConfig.csiNamespace, ds.Name))
		err := r.Delete(ctx, ds)
		if err != nil && !errors.IsNotFound(err) {
			return false, err
		}
	}
	// Note the k8sClient must be use, as the controller-runtime client tries to watch over all namespaces.
	configMaps, err := r.k8sClient.CoreV1().ConfigMaps(r.csiConfig.csiNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, configMap := range configMaps.Items {
		name, found := configMap.GetAnnotations()[util.CpcAnnotation]
		if found && name == cpc.GetName() {
			log.Info("Deleting cpc configmap", "name", cpc.GetName())
			err := r.Delete(ctx, &configMap)
			if err != nil {
				return false, err
			}
		}
	}

	if ds == nil {
		// Only remove the finalizer when the daemonset is actually deleted. If it was just deleted
		// above, wait until the next reconcile iteration to remove the finalizer.
		err = r.removeFinalizer(ctx, cpc)
		if err != nil {
			log.Error(err, "failed to remove finalizer from checkpointconfiguration", "namespace", cpc.GetNamespace(), "name", cpc.GetName())
			return false, err
		}

		return false, nil
	}
	// Otherwise, requeue to wait for the daemonset to be deleted.
	return true, nil
}

// checkDaemonsetConflict returns true if there are existing driver pods on the
// nodes selected by cpc.  An alternative implementation would list all nodes
// from any other CheckpointConfigurations and compare against those from cpc;
// that would probably be just as good in practice. Checking against driver pods
// does have the advantage of catching any pods that somehow have not been
// cleaned up from an already deleted checkpoint configuration.
func (r *reconciler) checkDaemonsetConflict(ctx context.Context, cpc *checkpoint.CheckpointConfiguration) (bool, error) {
	var nodeList corev1.NodeList
	err := r.List(ctx, &nodeList, client.MatchingLabels(cpc.Spec.NodeSelector))
	if err != nil {
		return false, fmt.Errorf("couldn't list nodes for conflict check: %v", err)
	}
	nodes := map[string]bool{}
	for _, node := range nodeList.Items {
		nodes[node.GetName()] = true
	}

	var pods corev1.PodList
	err = r.List(ctx, &pods, client.InNamespace(r.csiConfig.csiNamespace))
	if err != nil {
		return false, fmt.Errorf("couldn't list pods for conflict check: %v", err)
	}
	for _, pod := range pods.Items {
		if !strings.HasPrefix(pod.GetName(), csiPrefix) {
			continue
		}
		if pod.Spec.NodeName == "" {
			return false, fmt.Errorf("driver pod not yet scheduled: %s", pod.GetName())
		}
		if _, found := nodes[pod.Spec.NodeName]; found {
			log.FromContext(ctx).Info("conflict", "cpc", cpc.GetName(), "pod", pod.GetName(), "node", pod.Spec.NodeName)
			return true, nil
		}
	}
	// No conflict.
	return false, nil
}

func (r *reconciler) addFinalizer(ctx context.Context, cc checkpoint.CheckpointConfiguration) error {
	finalizers := cc.GetFinalizers()
	found := false
	for _, v := range finalizers {
		if r.finalizer == v {
			found = true
			break
		}
	}
	if found {
		// Finalizer already exists, nothing to do in this case
		return nil
	}

	log.FromContext(ctx).Info("add finalizer", "cpc", cc.Spec)
	ccClone := cc.DeepCopy()
	ccClone.ObjectMeta.Finalizers = append(ccClone.ObjectMeta.Finalizers, r.finalizer)

	err := r.Update(ctx, ccClone)
	if err != nil {
		return fmt.Errorf("failed to add protection finalizer to checkpointconfiguration %v", err)
	}

	log.FromContext(ctx).Info("Added protection finalizer", "checkpointconfiguration", cc.Name)
	return nil
}

func (r *reconciler) removeFinalizer(ctx context.Context, cc *checkpoint.CheckpointConfiguration) error {
	finalizers := cc.GetFinalizers()
	found := false
	for _, v := range finalizers {
		if r.finalizer == v {
			found = true
			break
		}
	}
	if !found {
		// Finalizer does not exist, nothing to do in this case
		log.FromContext(ctx).Info("no finalizer to remove", "checkpointconfiguration", cc.Name)
		return nil
	}

	log.FromContext(ctx).Info("remove finalizer", "cpc", cc.Spec)
	ccClone := cc.DeepCopy()
	ccClone.ObjectMeta.Finalizers = slices.DeleteFunc(ccClone.ObjectMeta.Finalizers, func(f string) bool { return f == r.finalizer })
	err := r.Update(ctx, ccClone)

	if err != nil {
		return fmt.Errorf("failed to remove protection finalizer from checkpointconfiguration %s, %v", cc.Name, err)
	}
	log.FromContext(ctx).Info("Removed protection finalizer", "checkpointconfiguration", cc.Name)
	return nil
}

type UptimeReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	csiConfig   csiconfig
	k8sClient   *kubernetes.Clientset
	gracePeriod time.Duration

	podDeleteAttemptCount    int // Tracking attempts instead of deletions avoids overwhelming API server.
	backoffDuration          time.Duration
	resumeReconcileTimestamp time.Time
	mu                       sync.Mutex
}

// UptimeReconciler deletes unhealthy pods, allowing the daemonset to create new pods.
// Note: The pod/req information is trimmed by uptimeReconcilerPodTrimmer before arriving to reconciler.
func (r *UptimeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only track health for pods owned by the DaemonSet.
	owner := metav1.GetControllerOf(&pod)
	if owner == nil || owner.Kind != "DaemonSet" {
		return ctrl.Result{}, nil
	}

	podInFailureState := false
	for _, cond := range pod.Status.Conditions {
		// Having a false PodReady condition means we are potentially in a failing state.
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionFalse {
			gracePeriodRemaining := time.Until(cond.LastTransitionTime.Time.Add(r.gracePeriod))
			// If we don't have any grace period left, we assume we're on a permanent failing state.
			if gracePeriodRemaining <= 0 {
				podInFailureState = true

				break
			} else {
				// Pods that have recetly entered a failing state will get requeued for the amount of grace period.
				logger.Info("Pod is in a failure state but within grace period. Requeuing.",
					"conditionType", corev1.PodReady,
					"gracePeriodRemaining", gracePeriodRemaining,
				)

				return ctrl.Result{RequeueAfter: gracePeriodRemaining}, nil
			}
		}
	}
	// Cover scenario where PodReady status is not in the pod.Status.Conditions array.
	if !podInFailureState {
		return ctrl.Result{}, nil
	}

	// Having a Mutex esures:
	// - parallel reconciles can read the resumeReconcileTimestamp properly .
	// - podDeleteAttemptCount matches with the number of delete calls sent to API server.
	checkReconcilerState := func() (bool, *ctrl.Result, error) {
		r.mu.Lock()
		defer r.mu.Unlock()

		// We are currently in a backoff state.
		if time.Now().Before(r.resumeReconcileTimestamp) {
			logger.Info("HighScaleCheckpointing Pod is in a known failure state, but reconciler is in backoff state",
				"podName", pod.Name,
				"backoffUntil", r.resumeReconcileTimestamp,
				"reattemptPodDeleteOn", time.Now().Add(r.backoffDuration).String(),
			)
			// We requeue after backoff period to avoid overwhelming apiserver right after resumeReconcileTimestamp.
			return false, &ctrl.Result{RequeueAfter: r.backoffDuration}, nil
		}

		// For this reconcile call, we are safe to attempt deletion.
		logger.Info("HighScaleCheckpointing Pod is in a known failure state. Deleting pod",
			"podName", pod.Name,
		)

		r.podDeleteAttemptCount += 1

		var ds v1.DaemonSet
		dsName := types.NamespacedName{Name: owner.Name, Namespace: pod.Namespace}
		if err := r.Get(ctx, dsName, &ds); err != nil {
			logger.Error(err, "failed to get owner DaemonSet", "daemonset", dsName)

			return false, &ctrl.Result{}, err
		}

		// Check if any future reconcile calls should be in backoff state.
		if r.podDeleteAttemptCount >= int(ds.Status.DesiredNumberScheduled) {
			r.resumeReconcileTimestamp = time.Now().Add(r.backoffDuration)
			r.podDeleteAttemptCount = 0
		}

		return true, nil, nil
	}

	if deletePod, reconcilerState, err := checkReconcilerState(); !deletePod {
		return *reconcilerState, err
	}

	// Delete the pod immediately.
	if err := r.Delete(ctx, &pod); err != nil {
		logger.Error(err, "failed to delete unhealthy pod, will attempt again")

		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	logger.Info("Successfully deleted unhealthy pod.", "podName", pod.Name)

	return ctrl.Result{}, nil
}
