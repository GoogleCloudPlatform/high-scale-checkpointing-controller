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

package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	cc "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/multitier-controller"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	kubeconfigPath          = flag.String("kubeconfig-path", "", "kubeconfig path")
	managedNamespace        = flag.String("managed-namespace", "", "The namespace for csi driver DaemonSet and the configmap which stores container images")
	csiServiceAccountName   = flag.String("csi-service-account-name", "", "The Serivce Account for csi driver DaemonSet")
	driverName              = flag.String("driver-name", "", "The name of the driver, eg multitier-checkpoint.csi.storage.gke.io")
	imageConfigMapName      = flag.String("image-configmap-name", "", "Name of configmap stores container images")
	localDeploy             = flag.Bool("local-deploy", false, "Create multitler csi with local deployment spec")
	debugBackup             = flag.Bool("replication-worker-debug-backup", false, "Enable debugging backup for the replication worker container")
	metricsAddress          = flag.String("metrics-address", "", "The bind address for the metrics server.")
	enableUptimeController  = flag.Bool("enable-uptime-controller", false, "enable uptime controller")
	uptimeControllerBackoff = flag.String("uptime-controller-backoff", "5m", "Uptime controller backoff duration")
	legacyIdFile            = flag.Bool("legacy-id-file", false, "Use the legacy idfile rank management")
	ranksServerPort         = flag.Int("ranks-server-port", -1, "The ranks controller gRPC service port, to inject into drivers")

	k8sApiQps = flag.Int("k8s-api-qps", 150, "QPS for the k8s api server. Burst will be twice this figure")

	healthCheckEndpoint         = flag.String("http-endpoint", "", "The bind address for the health check server.")
	enableLeaderElection        = flag.Bool("leader-election", false, "Enable leader election")
	leaderElectionNamespace     = flag.String("leader-election-namespace", "", "Namespace where the leader election resource lives. Defaults to the pod namespace if not set.")
	leaderElectionLeaseDuration = flag.Duration("leader-election-lease-duration", 15*time.Second, "Duration, in seconds, that non-leader candidates will wait to force acquire leadership. Defaults to 15 seconds.")
	leaderElectionRenewDeadline = flag.Duration("leader-election-renew-deadline", 10*time.Second, "Duration, in seconds, that the acting leader will retry refreshing leadership before giving up. Defaults to 10 seconds.")
	leaderElectionRetryPeriod   = flag.Duration("leader-election-retry-period", 5*time.Second, "Duration, in seconds, the LeaderElector clients should wait between tries of actions. Defaults to 5 seconds.")

	setupLog = ctrl.Log.WithName("controller").WithName("high-scale-checkpointing")

	// GKE metrics collector
	enableMetricsCollector = flag.Bool("enable-metrics-collector", false, "Enable gke metrics collector")
	projectNumber          = flag.String("project-number", "", "GCP project number")
	clusterLocation        = flag.String("cluster-location", "", "GKE cluster location")
	clusterName            = flag.String("cluster-name", "", "GKE cluster name")
	paramKSATokenFileName  = flag.String("p4sa-ksa-token-file-name", "", "NodeP4SAParams.KSATokenFileName")
	paramAudience          = flag.String("p4sa-audience", "", "NodeP4SAParams.Audience")
	paramTokenURL          = flag.String("p4sa-token-url", "", "NodeP4SAParams.TokenURL")
	paramTokenMode         = flag.String("p4sa-token-mode", "", "NodeP4SAParams.TokenMode")
	paramKSAFilePath       = flag.String("p4sa-ksa-file-path", "", "NodeP4SAParams.KSAFilePath")
	paramADCFilePath       = flag.String("p4sa-adc-file-path", "", "NodeP4SAParams.ADCFilePath")
	paramConsumerProjectID = flag.String("p4sa-consumer-project-id", "", "NodeP4SAParams.ConsumerProjectID")
	paramADCVolumeMountDir = flag.String("p4sa-adc-volume-mount-dir", "", "NodeP4SAParams.ADCVolumeMountDir")
	paramKSAVolumeMountDir = flag.String("p4sa-ksa-volume-mount-dir", "", "NodeP4SAParams.KSAVolumeMountDir")
	paramGKEHostName       = flag.String("p4sa-gke-host-name", "", "NodeP4SAParams.GKEHostName")
)

func main() {
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	klog.InitFlags(nil)
	flag.Parse()

	runController()
}

func runController() {
	if *managedNamespace == "" || *csiServiceAccountName == "" || *imageConfigMapName == "" {
		setupLog.Error(errors.New("bad flags"), "Missing --csi-namespace or --csi-service-account-name or --image-configmap-name or --image-configmap-namespace")
		os.Exit(1)
	}

	if *driverName == "" {
		setupLog.Error(nil, "missing --driver-name")
		os.Exit(1)
	}

	if *healthCheckEndpoint != "" && *metricsAddress == *healthCheckEndpoint {
		klog.Error("metrics and health check http servers can't have the same address")
	}

	if *enableMetricsCollector && (*projectNumber == "" || *clusterLocation == "" || *clusterName == "" || util.GetEnvVar(util.EnvGKEComponentVersion) == "") {
		klog.Errorf("--enable-metrics-collector requires --project-number, --cluster-location, --cluster-name and env var %q set", util.EnvGKEComponentVersion)
		os.Exit(1)
	}

	uptimeControllerBackoffDuration, err := time.ParseDuration(*uptimeControllerBackoff)
	if err != nil {
		klog.Errorf("--uptime-controller-backoff flag value is invalid: %v", err)
		os.Exit(1)
	}

	opts := cc.ControllerOptions{
		MasterURL:               "",
		Kubeconfig:              *kubeconfigPath,
		MetricsAddress:          *metricsAddress,
		ManagedNamespace:        *managedNamespace,
		CSIServiceAccountName:   *csiServiceAccountName,
		ImageConfigMapName:      *imageConfigMapName,
		LocalDeploy:             *localDeploy,
		DebugBackup:             *debugBackup,
		LeaderElection:          *enableLeaderElection,
		LeaderElectionID:        "highscalecheckpointing-leader",
		LeaderElectionNamespace: *leaderElectionNamespace,
		RetryPeriod:             leaderElectionRetryPeriod,
		LeaseDuration:           leaderElectionLeaseDuration,
		RenewDeadline:           leaderElectionRenewDeadline,
		HealthProbeBindAddress:  *healthCheckEndpoint,
		CSIDriverName:           *driverName,
		LegacyIdFile:            *legacyIdFile,
		RanksServerPort:         *ranksServerPort,
		EnableMetricsCollector:  *enableMetricsCollector,
		EnableUptimeController:  *enableUptimeController,
		UptimeControllerBackoff: uptimeControllerBackoffDuration,
	}

	if *enableMetricsCollector {
		opts.MetricsCollectorConfig = &cc.MetricsCollectorConfig{
			ProjectNumber:     *projectNumber,
			ClusterLocation:   *clusterLocation,
			ClusterName:       *clusterName,
			KSATokenFileName:  *paramKSATokenFileName,
			Audience:          *paramAudience,
			TokenURL:          *paramTokenURL,
			TokenMode:         *paramTokenMode,
			KSAFilePath:       *paramKSAFilePath,
			ADCFilePath:       *paramADCFilePath,
			ConsumerProjectID: *paramConsumerProjectID,
			ADCVolumeMountDir: *paramADCVolumeMountDir,
			KSAVolumeMountDir: *paramKSAVolumeMountDir,
			GKEHostName:       *paramGKEHostName,
		}
	}

	cc.Init()

	config, err := buildConfig(*kubeconfigPath)
	if err != nil {
		setupLog.Error(err, "failed to build a Kubernetes config")
	}
	if *k8sApiQps > 0 {
		config.QPS = float32(*k8sApiQps)
		config.Burst = 2 * *k8sApiQps
	}

	mgr, err := cc.NewControllerManager(config, opts)
	if err != nil {
		setupLog.Error(err, "new manager creation")
		os.Exit(1)
	}

	// There is some weirdness in the signal handling, there seems to be unexpected
	// termination signals. Usual mgr.Start would be called with the context from
	// ctrl.SetupSignalHandler, but that is causing unexpected exit. This exhaustive
	// logging of when signals are received might enable us to understand what's going on.
	setupLog.Info("setting up alternate signal handler")
	shutdown := make(chan os.Signal, 5) // Use a big buffer in case there are a lot of signals.
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		for {
			setupLog.Info("signal received", "signal", <-shutdown)
		}
	}()

	setupLog.Info("starting manager")
	signalCtx := ctrl.SetupSignalHandler()
	go func() {
		<-signalCtx.Done()
		setupLog.Info("signal context is done (ignored)")
	}()
	err = mgr.Start(context.Background())
	setupLog.Error(err, "manager finished")
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
