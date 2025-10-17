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
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/klog/v2"

	proto "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/lib/grpc/replication/proto"
)

var (
	port = flag.Int("port", 2112, "grpc port on localhost")

	cmd        = flag.String("cmd", "", "command to run")
	mountpoint = flag.String("mountpoint", "", "mountpoint for --cmd")
	ip         = flag.String("ip", "", "ip for --cmd")
	job        = flag.String("job", "", "job for --cmd")
)

func init() {
	klog.InitFlags(flag.CommandLine)
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", *port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		klog.Fatalf("gRPC connection: %v", err)
	}
	defer conn.Close()
	client := proto.NewReplicationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var reqErr error
	switch *cmd {
	case "set-peer":
		klog.Infof("stub set-peer on %q against %q", *mountpoint, *ip)
		_, reqErr = client.SetReplicationPeer(ctx, &proto.SetReplicationPeerRequest{
			LocalMountpoint: *mountpoint,
			TargetIp:        *ip,
		})
	case "unmount":
		klog.Infof("stub unmount on %q", *mountpoint)
		_, reqErr = client.UnmountPeer(ctx, &proto.UnmountPeerRequest{
			LocalMountpoint: *mountpoint,
		})
	case "unmount-all":
		klog.Infof("stub unmount all")
		_, reqErr = client.UnmountAllPeers(ctx, &proto.UnmountAllPeersRequest{})
	case "mount-gcs":
		klog.Infof("mount against GCS bucket folder %q", *mountpoint)
		_, reqErr = client.MountGCSBucket(ctx, &proto.MountGCSBucketRequest{
			LocalMountpoint: *mountpoint,
		})
	case "register-coordinator":
		klog.Infof("stub register coordinator %s/%s", *job, *ip)
		_, reqErr = client.RegisterCoordinator(ctx, &proto.RegisterCoordinatorRequest{
			JobName: *job,
			Ip:      *ip,
		})
	case "unregister-coordinator":
		klog.Infof("stub unregister coordinator for %s/%s", *job, *ip)
		_, reqErr = client.UnregisterCoordinator(ctx, &proto.UnregisterCoordinatorRequest{
			JobName: *job,
			Ip:      *ip,
		})
	case "get-coordinator":
		klog.Infof("stub get coordinator for %s", *job)
		var rsp *proto.GetCoordinatorResponse
		rsp, reqErr = client.GetCoordinator(ctx, &proto.GetCoordinatorRequest{
			JobName: *job,
		})
		if reqErr == nil {
			klog.Infof("RESPONSE: %s", rsp.Ip)
		}
	default:
		klog.Fatalf("Bad command %s", *cmd)
	}
	if reqErr != nil {
		klog.Fatalf("%s error: %v", *cmd, reqErr)
	}
	os.Exit(0)
}
