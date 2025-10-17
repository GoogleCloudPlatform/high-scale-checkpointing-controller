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

package csi

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"
)

// LocalVolumeNotReadyError is returned from a LocalVolumeCreator if the volume is not ready
// (eg, we don't have topology information).
type LocalVolumeNotReadyError struct {
	error
}

type CheckpointDriverConfig struct {
	DriverName string // Driver Name
	Version    string // Driver Version
	NodeId     string // Node Name
	Endpoint   string // CSI endpoint
}

// Driver is the object backing the CSI driver. It also implements identity and node services, q.v.
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer
	*CheckpointDriverConfig
	volumeCreator VolumeCreatorForPod
}

var _ csi.IdentityServer = &Driver{}
var _ csi.NodeServer = &Driver{}

// NewDriver creates a new local volume CSI driver using the given LocalVolumeCreator.
// endpoint is the csi socket, and nodeId is the id to use for csi registration.
func NewDriver(creator VolumeCreatorForPod, config *CheckpointDriverConfig) (*Driver, error) {
	klog.V(4).Infof("Driver: %v version: %v running on %s", config.DriverName, config.Version, config.NodeId)

	d := &Driver{
		CheckpointDriverConfig: config,
		volumeCreator:          creator,
	}

	return d, nil
}

// Run will serve the CSI driver. Normally this will run forever; an error will be returned otherwise.
func (d *Driver) Run() error {
	klog.Infof("Running driver: %v", d.DriverName)

	s := NewNonBlockingGRPCServer()
	s.Start(d.Endpoint, d, d)
	s.Wait()
	return fmt.Errorf("server wait finished")
}
