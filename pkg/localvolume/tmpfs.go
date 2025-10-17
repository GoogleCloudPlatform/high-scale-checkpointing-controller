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
	"context"
	"fmt"
	"os"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

const (
	// This must be hostPath'd into our container in order to be writable.
	transparentHugepageEnableFile = "/sys/kernel/mm/transparent_hugepage/enabled"
)

type tmpfsVolume struct {
	path string
}

var _ LocalVolume = &tmpfsVolume{}

// NewTmpfsVolume makes a new ram volume based on a tmpfs mounted to path.  The
// tmpfs creation happens at the time of this call, and an error will be
// returned if the mount fails. The tmpfs is created with hugepages. path is
// created if it doesn't already exist.
func NewTmpfsVolume(ctx context.Context, path string, sizeMiB int) (LocalVolume, error) {
	if sizeMiB <= 0 {
		return nil, fmt.Errorf("Bad size %d MiB", sizeMiB)
	}

	if err := checkHugepageSupport(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(path, 0750); err != nil {
		return nil, fmt.Errorf("Could not use or create %s: %w", path, err)
	}

	mountOpts := []string{
		fmt.Sprintf("size=%dM", sizeMiB),
		fmt.Sprintf("huge=always"),
	}

	mounter := &mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      exec.New(),
	}
	if err := mounter.Mount("tmpfs", path, "tmpfs", mountOpts); err != nil {
		return nil, fmt.Errorf("Could not mount at %s with %v: %w", path, mountOpts, err)
	}

	return &tmpfsVolume{
		path: path,
	}, nil

}

func (v *tmpfsVolume) Path() string {
	return v.path
}

func checkHugepageSupport() error {
	status, err := os.ReadFile(transparentHugepageEnableFile)
	if err != nil {
		return err
	}
	if strings.Contains(string(status), "[always]") {
		klog.Infof("transparent hugepage already enabled: %s", string(status))
		return nil
	}
	err = os.WriteFile(transparentHugepageEnableFile, []byte("always"), 0644)
	if err != nil {
		return err
	}
	newStatus, err := os.ReadFile(transparentHugepageEnableFile)
	if err != nil {
		return err
	}
	if !strings.Contains(string(newStatus), "[always]") {
		return fmt.Errorf("could not enable transparent hugepage: %s", string(newStatus))
	}
	klog.Infof("transparent hugepage change to %s from %s", strings.TrimSpace(string(newStatus)), string(status))
	return nil
}
