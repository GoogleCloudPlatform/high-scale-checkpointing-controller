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
	"path/filepath"

	"k8s.io/klog/v2"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	scpCommand = "scp-wrapper.sh"
	initSshCmd = "/etc/init.d/ssh"
)

var (
	initSshArgs = []string{"start", "-E /var/log/sshd"}
)

type scpVolume struct {
	target, path string
}

var _ LocalVolume = &scpVolume{}

// InitScpServer starts an sshd instance used for scp volumes.
func InitScpServer(ctx context.Context) error {
	output, err := util.RunCommand(initSshCmd, initSshArgs...)
	if err != nil {
		return fmt.Errorf("Cannot init ssh: %w", err)
	}
	klog.Infof("ssh init logs: %s", output)
	return nil
}

// NewScpVolume creates a LocalVolume on path that on initialization copies any newer data down from target.
// The root is usually a ram-based emptydir, and is assumed to be the same both locally and remotely.
func NewScpVolume(ctx context.Context, target, root string) (LocalVolume, error) {
	if err := scpTargetToPath(ctx, target, root); err != nil {
		klog.Infof("%s did not have any scp data, starting with clean disk (%v)", target, err)
	}
	// Since root is already a local volume, there's nothing else to do.
	return &scpVolume{target, root}, nil
}

func scpTargetToPath(ctx context.Context, target, root string) error {
	// The /mnt path on the target depends on the pod configuration.
	_, err := util.RunCommand(scpCommand, "-rv", fmt.Sprintf("%s:%s", target, filepath.Join(root, "*")), root)
	if err != nil {
		return fmt.Errorf("scp from %s failed: %w", target, err)
	}
	klog.Infof("Successful recursive scp output from %s to %s", target, root)
	return nil
}

func (v *scpVolume) Path() string {
	return v.path
}
