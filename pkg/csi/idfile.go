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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/idfile"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/localvolume"
)

type VolumeCreatorForPod interface {
	CreateLocalVolume(ctx context.Context, podUID types.UID) (localvolume.LocalVolume, error)
	Unmount(ctx context.Context, podUID types.UID)
}

type staticVolumeCreator struct {
	volume localvolume.LocalVolume
}

var _ VolumeCreatorForPod = &staticVolumeCreator{}

// NewStaticVolumeCreator is a simple creator that returns an already created local volume.
func NewStaticVolumeCreator(volume localvolume.LocalVolume) VolumeCreatorForPod {
	return &staticVolumeCreator{volume}
}

func (v *staticVolumeCreator) CreateLocalVolume(ctx context.Context, _ types.UID) (localvolume.LocalVolume, error) {
	return v.volume, nil
}

func (_ *staticVolumeCreator) Unmount(_ context.Context, _ types.UID) {
	// nothing to do
}

type idfileVolumeCreator struct {
	volume              localvolume.LocalVolume
	idfile              idfile.IdFileClient
	processInfoFilename string
	coordinatorPort     int
	currentCancel       context.CancelFunc
}

var _ VolumeCreatorForPod = &idfileVolumeCreator{}

func NewIdFileVolumeCreator(vol localvolume.LocalVolume, processInfoFilename string, coordinatorPort int, opts idfile.IdFileClientOpts) (VolumeCreatorForPod, error) {
	if !filepath.IsLocal(processInfoFilename) {
		return nil, fmt.Errorf("process info file %s is not local", processInfoFilename)
	}
	client, err := idfile.NewIdFileClient(opts)
	if err != nil {
		return nil, fmt.Errorf("could not create id file client: %w", err)
	}
	return &idfileVolumeCreator{
		volume:              vol,
		processInfoFilename: processInfoFilename,
		idfile:              client,
		coordinatorPort:     coordinatorPort,
	}, nil
}

func (v *idfileVolumeCreator) CreateLocalVolume(ctx context.Context, podUID types.UID) (localvolume.LocalVolume, error) {
	klog.Infof("Starting idfile creation at %s", v.volume.Path())

	// Ensure we don't have stale data.
	err := v.removeProcessInfoFile()
	if err != nil {
		klog.Errorf("Error removing process info file (ignored): %v", err)
	}
	if v.currentCancel != nil {
		klog.Info("Canceling existing idfile creation")
		v.currentCancel()
	}
	// Make a new context in order not to time out with the CSI volume creation context.
	ctx, cancel := context.WithCancel(context.Background())
	v.currentCancel = cancel
	go func() {
		if err := v.updateProcessInfo(ctx, podUID); err != nil {
			klog.Errorf("Could not update local volume creation: %v", err)
		}
		cancel()
		// It's okay for cancel() to be called multiple times, but v.currentCancel can't
		// be touched as we're racing with a future mount.
	}()
	return v.volume, nil
}

func (_ *idfileVolumeCreator) Unmount(_ context.Context, _ types.UID) {
	// nothing to do
}

func (v *idfileVolumeCreator) updateProcessInfo(ctx context.Context, podUID types.UID) error {
	mapping, coord, err := v.idfile.FetchNodeInformation(ctx, podUID)
	if err != nil {
		return err
	}

	// Do an atomic write first to a tempfile, then move.
	dir, file := filepath.Split(v.processInfoPath())
	tmpfile := filepath.Join(dir, ".tmp."+file)
	contents := fmt.Sprintf("%d\n%s:%d\n", mapping.ProcessIndex, coord.Address, v.coordinatorPort)
	if err := os.WriteFile(tmpfile, []byte(contents), 0555); err != nil {
		return err
	}
	if err := os.Rename(tmpfile, v.processInfoPath()); err != nil {
		return err
	}
	klog.Infof("Wrote process info at %s: %v, %v", v.processInfoPath(), mapping, coord)
	return nil
}

func (v *idfileVolumeCreator) removeProcessInfoFile() error {
	err := os.Remove(v.processInfoPath())
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return nil
}

func (v *idfileVolumeCreator) processInfoPath() string {
	return filepath.Join(v.volume.Path(), v.processInfoFilename)
}
