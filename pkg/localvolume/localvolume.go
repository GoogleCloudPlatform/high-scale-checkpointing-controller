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

package localvolume

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

const (
	fsType     = "ext4"
	procMounts = "/proc/mounts"
)

// LocalVolume represents a local volume to the CSI node driver. It should have a
// path that locates the volume in the local filesystem. This must be bind-mountable.
type LocalVolume interface {
	Path() string
}

// pathVolume is a local volume from a path.
type pathVolume struct {
	path string
}

var _ LocalVolume = &pathVolume{}

// deviceVolume is a local volume from a device.
type deviceVolume struct {
	devicePath string
	mountPath  string
}

var _ LocalVolume = &deviceVolume{}

// NewPathVolume creates a local volume from a path. The state of the path is not verified.
func NewPathVolume(path string) LocalVolume {
	return &pathVolume{path}
}

func (v *pathVolume) Path() string {
	return v.path
}

// NewDeviceVolume creates a local volume from a device. The device will be
// formatted if necessary and mounted at the specified location. If the device
// is already mounted to mountPath, the existing mount is returned.
func NewFromDevice(devicePath, mountPath string) (LocalVolume, error) {
	actualDevice, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		return nil, fmt.Errorf("Cannot resolve %s: %w", devicePath, err)
	}
	mounts, err := os.ReadFile(procMounts)
	if err != nil {
		return nil, fmt.Errorf("Cannot read %s: %w", procMounts, err)
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		if strings.Contains(line, mountPath) {
			if !strings.Contains(line, actualDevice) {
				return nil, fmt.Errorf("Already mounted, but not to expected device %s: %s", actualDevice, line)
			}
			klog.Infof("Found %s already mounted at %s", devicePath, mountPath)
			return &deviceVolume{
				devicePath,
				mountPath,
			}, nil
		}
	}

	if err := os.MkdirAll(mountPath, 0750); err != nil {
		return nil, fmt.Errorf("Couldn't create mount point: %w", err)
	}

	mounter := &mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      exec.New(),
	}
	if err := mounter.FormatAndMount(devicePath, mountPath, fsType, nil); err != nil {
		return nil, fmt.Errorf("cannot format %s to %s: %w", devicePath, mountPath, err)
	}
	return &deviceVolume{
		devicePath,
		mountPath,
	}, nil
}

func (v *deviceVolume) Path() string {
	return v.mountPath
}
