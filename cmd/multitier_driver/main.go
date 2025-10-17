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
	"flag"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/csi"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/idfile"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/localvolume"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/metrics"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/replication"
)

const (
	tmpfsName = "tmpfs"
)

var (
	endpoint        = flag.String("endpoint", "unix:/tmp/csi.sock", "CSI endpoint")
	tmpfsSizeMiB    = flag.Int("tmpfs-size-mib", -1, "Size of tmpfs ram filesystem in MiB.")
	driverName      = flag.String("driver-name", "", "name of driver (should match the CSIDriver object)")
	csiNodeId       = flag.String("csi-node-id", "", "The node id to use for csi registration, probably pod spec.NodeName")
	namespace       = flag.String("namespace", "", "The namespace for the driver to keep its state")
	cpcName         = flag.String("cpc-name", "", "The name of the CheckpointConfiguration that generated this driver instance")
	peerDir         = flag.String("peer-dir", "", "The directory to be used for peer replication")
	remoteDir       = flag.String("remote-dir", "", "The directory to be used for remote replication, shared with the replication worker")
	persistentDir   = flag.String("persistent-dir", "", "The directory that mounts to persistent storage (e.g. GCSFuse)")
	localDir        = flag.String("local-dir", "", "The directory to share the local ramdisk with the replication worker")
	metricsEndpoint = flag.String("metrics-address", "", "The TCP network address where the prometheus metrics endpoint will listen (example: `:8080`). The default is empty, which means metrics endpoint is disabled.")
	nfsExportDir    = flag.String("nfs-export", "/exports", "The directory path exported by the NFS sidecar")
	replicationPort = flag.Int("replication-port", 2112, "The port for the replication api gRPC server, on localhost")

	// jax process id mapping
	jaxIdMapping        = flag.Bool("jax-id-mapping", false, "if the jax id mapping calculation is turned on")
	processInfoFilename = flag.String("process-info-filename", "jax-init-info.txt", "The file storing process info, relative to the root of the ram volume.")
	coordinatorPort     = flag.Int("coordinator-port", 8476, "The coordinator port to use in the process info file.")
	legacyIdFile        = flag.Bool("legacy-id-file", false, "Use the legacy idfile rank management")
	ranksServerTarget   = flag.String("ranks-server-target", "", "The ranks controller gRPC service")

	version string // This should be set during build.
)

func init() {
	// klog verbosity guide for this package
	// Use V(2) for one time config information
	// Use V(4) for general debug information logging
	// Use V(5) for GCE Cloud Provider Call informational logging
	// Use V(6) for extra repeated/polling information
	klog.InitFlags(flag.CommandLine)
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	if *driverName == "" {
		klog.Fatalf("Missing --driver-name")
	}

	if *tmpfsSizeMiB < 0 {
		klog.Fatalf("Missing --tmpfs-size-mib")
	}
	if *csiNodeId == "" {
		klog.Fatalf("Missing --csi-node-id")
	}
	if *namespace == "" {
		klog.Fatalf("Missing --namespace")
	}
	if *cpcName == "" {
		klog.Fatalf("Missing --cpc-name")
	}
	if *peerDir == "" {
		klog.Fatalf("Missing --peer-dir")
	}
	if *remoteDir == "" {
		klog.Fatalf("Missing --remote-dir")
	}
	if *localDir == "" {
		klog.Fatalf("Missing --local-dir")
	}
	if *persistentDir == "" {
		klog.Fatalf("Missing --persistent-dir")
	}

	if *jaxIdMapping && *processInfoFilename == "" {
		klog.Fatalf("--process-info-filename must be set when --jax-id-mapping is set to true")
	}

	ctx := context.Background()

	kubeClient, err := kubernetes.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		klog.Fatalf("Could not create kubeclient: %v", err)
	}

	tmpfsPath := filepath.Join(*localDir, tmpfsName)

	vol, err := localvolume.NewTmpfsVolume(ctx, tmpfsPath, *tmpfsSizeMiB)
	if err != nil {
		klog.Fatalf("Could not create tmpfs: %v", err)
	}

	var mm metrics.MetricsManager
	// Initialize metrics endpoint if provided
	if *metricsEndpoint != "" {
		klog.Infof("Initializing metrics endpoint %q", *metricsEndpoint)
		mm = metrics.NewMetricsManager()
		mm.InitializeHTTPHandler(*metricsEndpoint)
	}

	var creator csi.VolumeCreatorForPod
	if *jaxIdMapping {
		klog.Infof("turning on the jax process id mapping feature")
		opts := idfile.IdFileClientOpts{
			KubeClient: kubeClient,
			Namespace:  *namespace,
			NodeName:   *csiNodeId,
		}
		if *legacyIdFile {
			creator, err = csi.NewIdFileVolumeCreator(vol, *processInfoFilename, *coordinatorPort, opts)
			if err != nil {
				klog.Fatalf("Could not create idfile volume: %v", err)
			}
		} else {
			creator, err = csi.NewRanksVolumeCreator(vol, idfile.RanksClientOpts{
				IdFileClientOpts:    opts,
				ProcessInfoFilename: *processInfoFilename,
				CoordinatorPort:     *coordinatorPort,
				ServerTarget:        *ranksServerTarget,
				MetricsManager:      mm,
			})
			if err != nil {
				klog.Fatalf("Could not create ranks client: %v", err)
			}
		}
	} else {
		creator = csi.NewStaticVolumeCreator(vol)
	}

	driverConfig := csi.CheckpointDriverConfig{
		DriverName: *driverName,
		NodeId:     *csiNodeId,
		Endpoint:   *endpoint,
		Version:    version,
	}

	driver, err := csi.NewDriver(creator, &driverConfig)
	if err != nil {
		klog.Fatalf("Cannot create driver: %v", err)
	}

	replOpts := replication.ServerOptions{
		Namespace:      *namespace,
		CpcName:        *cpcName,
		PeerBase:       *peerDir,
		RemoteBase:     *remoteDir,
		PersistentBase: *persistentDir,
		NfsExport:      *nfsExportDir,
		MetricsManager: mm,
	}

	replicationServer, err := replication.NewServer(kubeClient, replOpts)
	if err != nil {
		klog.Fatalf("Cannot create replication api server: %v", err)
	}

	finished := make(chan struct{})
	go func() {
		klog.Infof("running GKE checkpoint driver version %v", version)
		err := driver.Run()
		klog.Fatalf("unexpected driver exit: %v", err)
	}()

	go func() {
		klog.Infof("running replication API server")
		err := replicationServer.Start(*replicationPort)
		klog.Fatalf("unexpected replication api exit: %v", err)
	}()

	// Nothing ever writes on this channel in the current implementation.
	<-finished
	klog.Infof("graceful exit")
}
