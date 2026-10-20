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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

const (
	// This must be hostPath'd into our container in order to be writable.
	transparentHugepageEnableFile = "/sys/kernel/mm/transparent_hugepage/enabled"

	// tmpfsMarkerFile is created at the root of the tmpfs right after we mount
	// it. It lets a restarted driver process recognize the tmpfs it mounted
	// earlier in the same pod, so that the tmpfs is reused instead of a second
	// one being stacked on top. A stacked tmpfs hides the checkpoint data and
	// state in the lower one, and splits the containers that share this volume
	// (replicator, nfs-server, workload bind mounts) across two filesystems.
	//
	// The marker lives at the root of the tmpfs, outside of the client/
	// directory that is bind mounted into workloads, NFS exported, and cleaned
	// up by the replicator, so nothing else removes it.
	tmpfsMarkerFile = ".multitier-tmpfs"
)

type tmpfsVolume struct {
	path string
}

var _ LocalVolume = &tmpfsVolume{}

// tmpfsMarker is the content of tmpfsMarkerFile. Only the existence of the
// marker is used to decide whether to reuse the tmpfs; for now the content is
// only informational, for debugging.
//
// We considered recording an incarnation identifier (for example the pod UID)
// and requiring it to match on reuse, but decided it's not necessary: path is
// on top of the pod's emptyDir, which kubelet creates per pod UID, so a tmpfs
// found there can only have been mounted by an earlier driver process in this
// same pod. A new pod, or a node reboot, always starts with a fresh emptyDir
// and no marker. Changing the size also changes the pod spec, which recreates
// the pod.
type tmpfsMarker struct {
	// Pod is the hostname of the driver container, i.e. the pod name.
	Pod       string    `json:"pod"`
	SizeMiB   int       `json:"sizeMiB"`
	MountedAt time.Time `json:"mountedAt"`
}

// NewTmpfsVolume makes a new ram volume based on a tmpfs mounted to path.  The
// tmpfs creation happens at the time of this call, and an error will be
// returned if the mount fails. The tmpfs is created with hugepages. path is
// created if it doesn't already exist.
//
// If a tmpfs created by an earlier call (for example, before a restart of the
// driver container) is already mounted at path, it is reused rather than a new
// tmpfs being mounted on top of it.
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

	if err := mountTmpfsOnce(path, sizeMiB, mount.New("")); err != nil {
		return nil, err
	}

	return &tmpfsVolume{
		path: path,
	}, nil
}

// mountTmpfsOnce mounts a tmpfs at path and marks it with tmpfsMarkerFile,
// unless a tmpfs marked by an earlier driver process is already mounted there.
// mountIface is a parameter so that tests can use a fake mounter.
func mountTmpfsOnce(path string, sizeMiB int, mountIface mount.Interface) error {
	if hasTmpfsMarker(path) {
		return nil
	}

	mountOpts := []string{
		fmt.Sprintf("size=%dM", sizeMiB),
		fmt.Sprintf("huge=always"),
	}

	mounter := &mount.SafeFormatAndMount{
		Interface: mountIface,
		Exec:      exec.New(),
	}
	if err := mounter.Mount("tmpfs", path, "tmpfs", mountOpts); err != nil {
		return fmt.Errorf("Could not mount at %s with %v: %w", path, mountOpts, err)
	}

	// If the process dies between the mount above and writing the marker, the
	// next start won't find the marker and will mount again, which is the
	// behavior before the marker was introduced. The window is tiny, and
	// mounting again then loses no data: the rest of the driver hasn't run yet,
	// so nothing (replicator.yaml, checkpoints, rank data) has been written to
	// the tmpfs that gets covered.
	if err := writeTmpfsMarker(path, sizeMiB); err != nil {
		// Unmount before failing. Otherwise the driver restart that follows
		// would not find the marker either, and would stack another tmpfs.
		if uerr := mountIface.Unmount(path); uerr != nil {
			return fmt.Errorf("Could not write tmpfs marker in %s: %w (also could not unmount the tmpfs: %v)", path, err, uerr)
		}
		return fmt.Errorf("Could not write tmpfs marker in %s: %w", path, err)
	}
	return nil
}

// hasTmpfsMarker returns true if the tmpfs mounted at path was created by an
// earlier driver process, as indicated by tmpfsMarkerFile.
func hasTmpfsMarker(path string) bool {
	markerPath := filepath.Join(path, tmpfsMarkerFile)
	data, err := os.ReadFile(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		// Don't know; fall back to mounting a new tmpfs.
		klog.Warningf("Could not read tmpfs marker %s, mounting a new tmpfs: %v", markerPath, err)
		return false
	}
	var marker tmpfsMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		// The content is informational only, the marker existing is what matters.
		klog.Warningf("Could not parse tmpfs marker %s (%q), reusing the tmpfs anyway: %v", markerPath, string(data), err)
	} else {
		klog.Infof("Reusing tmpfs at %s mounted at %s by %s with size %d MiB, not mounting a new one", path, marker.MountedAt.Format(time.RFC3339), marker.Pod, marker.SizeMiB)
	}
	return true
}

func writeTmpfsMarker(path string, sizeMiB int) error {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = fmt.Sprintf("<unknown: %v>", err)
	}
	data, err := json.Marshal(tmpfsMarker{
		Pod:       hostname,
		SizeMiB:   sizeMiB,
		MountedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, tmpfsMarkerFile), data, 0644)
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
