// Copyright 2026 Google LLC
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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
	"k8s.io/mount-utils"
)

// failingMounter is a FakeMounter whose mounts always fail.
type failingMounter struct {
	*mount.FakeMounter
}

func (f *failingMounter) Mount(source, target, fstype string, options []string) error {
	return errors.New("mount failed")
}

func actions(m *mount.FakeMounter) []string {
	var a []string
	for _, l := range m.GetLog() {
		a = append(a, l.Action)
	}
	return a
}

func newTmpfsDir(t *testing.T) string {
	path := filepath.Join(t.TempDir(), "tmpfs")
	assert.NilError(t, os.MkdirAll(path, 0750))
	return path
}

func TestMountTmpfsOnceMountsAndWritesMarker(t *testing.T) {
	path := newTmpfsDir(t)
	m := mount.NewFakeMounter(nil)

	assert.NilError(t, mountTmpfsOnce(path, 1024, m))

	log := m.GetLog()
	assert.Equal(t, len(log), 1)
	assert.Equal(t, log[0].Action, mount.FakeActionMount)
	assert.Equal(t, log[0].FSType, "tmpfs")
	assert.Equal(t, log[0].Target, path)
	assert.DeepEqual(t, m.MountPoints[0].Opts, []string{"size=1024M", "huge=always"})

	data, err := os.ReadFile(filepath.Join(path, tmpfsMarkerFile))
	assert.NilError(t, err)
	var marker tmpfsMarker
	assert.NilError(t, json.Unmarshal(data, &marker))
	assert.Equal(t, marker.SizeMiB, 1024)
	hostname, _ := os.Hostname()
	assert.Equal(t, marker.Pod, hostname)
	assert.Assert(t, !marker.MountedAt.IsZero())
}

// A restart of the driver container must reuse the tmpfs rather than stacking
// a new one on top of it (b/571833585).
func TestMountTmpfsOnceReusesAfterRestart(t *testing.T) {
	path := newTmpfsDir(t)

	first := mount.NewFakeMounter(nil)
	assert.NilError(t, mountTmpfsOnce(path, 1024, first))
	assert.DeepEqual(t, actions(first), []string{mount.FakeActionMount})
	markerBefore, err := os.ReadFile(filepath.Join(path, tmpfsMarkerFile))
	assert.NilError(t, err)

	// Each restart is a new process, so it gets a new mounter.
	for i := 0; i < 3; i++ {
		restarted := mount.NewFakeMounter(nil)
		assert.NilError(t, mountTmpfsOnce(path, 1024, restarted))
		assert.Equal(t, len(restarted.GetLog()), 0, "restart %d mounted a new tmpfs", i)
	}

	// The marker is left as written by the process that mounted the tmpfs.
	markerAfter, err := os.ReadFile(filepath.Join(path, tmpfsMarkerFile))
	assert.NilError(t, err)
	assert.Equal(t, string(markerAfter), string(markerBefore))
}

// If the marker can't be read, a new tmpfs is mounted. If the marker then
// can't be written, the new tmpfs is unmounted again and an error returned, so
// that the driver restart doesn't stack another tmpfs.
func TestMountTmpfsOnceMarkerUnreadableAndUnwritable(t *testing.T) {
	path := newTmpfsDir(t)
	// A directory where the marker file should be makes both reading and
	// writing it fail.
	assert.NilError(t, os.MkdirAll(filepath.Join(path, tmpfsMarkerFile), 0750))

	m := mount.NewFakeMounter(nil)
	err := mountTmpfsOnce(path, 1024, m)
	assert.ErrorContains(t, err, "Could not write tmpfs marker")
	assert.DeepEqual(t, actions(m), []string{mount.FakeActionMount, mount.FakeActionUnmount})
	assert.Equal(t, len(m.MountPoints), 0)
}

func TestMountTmpfsOnceMountFailure(t *testing.T) {
	path := newTmpfsDir(t)
	m := &failingMounter{mount.NewFakeMounter(nil)}

	assert.ErrorContains(t, mountTmpfsOnce(path, 1024, m), "mount failed")

	_, err := os.Stat(filepath.Join(path, tmpfsMarkerFile))
	assert.Assert(t, errors.Is(err, os.ErrNotExist), "marker must not be written when the mount fails")
}
