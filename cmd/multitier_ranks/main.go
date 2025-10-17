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
	"flag"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/idfile"
)

var (
	namespace       = flag.String("namespace", "", "Namespace for worker pods")
	k8sApiQps       = flag.Int("k8s-api-qps", 150, "QPS for the k8s api server. Burst will be twice this figure")
	driverName      = flag.String("driver-name", "", "The name of the driver, eg phase1-checkpoint.csi.storage.gke.io")
	ranksServerPort = flag.Int("ranks-server-port", -1, "The ranks controller gRPC service")

	setupLog = ctrl.Log.WithName("setup")
)

func main() {
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	flag.Parse()

	if *namespace == "" {
		setupLog.Error(nil, "missing --namespace")
		os.Exit(1)
	}
	if *driverName == "" {
		setupLog.Error(nil, "missing --driver-name")
		os.Exit(1)
	}

	idfile.Init()

	cfg := ctrl.GetConfigOrDie()
	if *k8sApiQps > 0 {
		cfg.QPS = float32(*k8sApiQps)
		cfg.Burst = 2 * *k8sApiQps
	}

	var mgr ctrl.Manager
	var err error
	opts := idfile.ControllerOpts{
		Namespace:  *namespace,
		DriverName: *driverName,
	}
	server, rankOpts := idfile.NewRanksServer(opts)
	mgr, err = idfile.NewRanksManager(cfg, rankOpts, config.Controller{})
	if err != nil {
		setupLog.Error(err, "new ranks manager creation")
		os.Exit(1)
	}
	// The grpcSvr from PrepareToServe is ignored since we never GracefulStop it.
	_, serveForever, err := server.PrepareToServe(*ranksServerPort)
	if err != nil {
		setupLog.Error(err, "new ranks server preparation")
		os.Exit(1)
	}
	go func() {
		if err := serveForever(); err != nil {
			setupLog.Error(err, "error returned from ranks grpc server")
			os.Exit(1)
		}
	}()

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting ranks manager")
	err = mgr.Start(ctrl.SetupSignalHandler())
	setupLog.Error(err, "unexpected manager exit")
	os.Exit(1)
}
