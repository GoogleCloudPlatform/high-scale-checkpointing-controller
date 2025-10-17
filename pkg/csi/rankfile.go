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
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/idfile"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/localvolume"
)

const (
	clientDir = "client"
)

type ranksVolumeCreator struct {
	volume              localvolume.LocalVolume
	ranks               idfile.RanksClient
	processInfoFilename string
	coordinatorPort     int
	currentCancel       context.CancelFunc
}

var _ VolumeCreatorForPod = &ranksVolumeCreator{}

func NewRanksVolumeCreator(vol localvolume.LocalVolume, opts idfile.RanksClientOpts) (VolumeCreatorForPod, error) {
	if opts.ProcessInfoFilename == "" {
		return nil, fmt.Errorf("Missing ProcessInfoFilename in opts %+v", opts)
	}
	if opts.CoordinatorPort <= 0 {
		return nil, fmt.Errorf("Missing CoordinatorPort in opts %+v", opts)
	}
	if opts.ServerTarget == "" {
		return nil, fmt.Errorf("Missing ServerTarget in opts %+v", opts)
	}

	clientPath := filepath.Join(vol.Path(), clientDir)
	if _, err := os.Stat(clientPath); os.IsNotExist(err) {
		if err := os.Mkdir(clientPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create %s/: %w", clientPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("could not check for %s/: %w", clientPath, err)
	}

	if !filepath.IsLocal(opts.ProcessInfoFilename) {
		return nil, fmt.Errorf("process info file %s is not local", opts.ProcessInfoFilename)
	}
	opts.StateDir = vol.Path()
	opts.IdFileDir = clientPath
	ranks, err := idfile.NewRanksClient(opts)
	if err != nil {
		return nil, fmt.Errorf("could not create id file client: %w", err)
	}

	return &ranksVolumeCreator{
		volume:              vol,
		processInfoFilename: opts.ProcessInfoFilename,
		ranks:               ranks,
		coordinatorPort:     opts.CoordinatorPort,
	}, nil
}

func (v *ranksVolumeCreator) CreateLocalVolume(ctx context.Context, podUID types.UID) (localvolume.LocalVolume, error) {
	klog.Infof("Starting idfile creation at %s", v.volume.Path())

	// Use a background context since the mount process will far outlive the CSI RPC that
	// created the context for this call.
	if err := v.ranks.NewMount(context.Background(), podUID); err != nil {
		return nil, fmt.Errorf("Error starting ranks client: %w", err)
	}

	return localvolume.NewPathVolume(filepath.Join(v.volume.Path(), clientDir)), nil
}

func (v *ranksVolumeCreator) Unmount(ctx context.Context, uid types.UID) {
	v.ranks.Unmount(ctx, uid)
}
