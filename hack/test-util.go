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

package test_util

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"path/filepath"
)

func SetupEnviron(ctx context.Context) {
	log := log.FromContext(ctx)
	kubeRoot := os.Getenv("KUBE_ROOT")
	if kubeRoot == "" {
		setDefaultKubeEnv(ctx)
		kubeRoot = os.Getenv("KUBE_ROOT")
		log.Info("KUBE_ROOT should be set, and should point to a kubernetes installation with etcd and api server built, from hack/install-etcd.sh and make quick-release. If they aren't present, testing will fail with errors about not being able to find those binaries. Defaulting to REPO_ROOT/kubernetes")
	}
	fmt.Printf("kube root is %s\n", kubeRoot)
	os.Setenv("TEST_ASSET_ETCD", filepath.Join(kubeRoot, "third_party/etcd/etcd"))
	os.Setenv("TEST_ASSET_KUBE_APISERVER", filepath.Join(kubeRoot, "_output/release-stage/server/linux-amd64/kubernetes/server/bin/kube-apiserver"))
}

func setDefaultKubeEnv(ctx context.Context) {
	log := log.FromContext(ctx)
	// Find the repository root using Git
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := cmd.Output()
	if err != nil {
		// Handle the error appropriately, e.g., log it and exit
		log.Error(err, "failed to set default kube env")
		os.Exit(1)
	}

	repoRoot := string(output)
	repoRoot = filepath.Clean(repoRoot)
	repoRoot = strings.TrimSuffix(repoRoot, "\n") // Remove trailing newline

	// Set the environment variable
	kubernetesPath := filepath.Join(repoRoot, "kubernetes")
	os.Setenv("KUBE_ROOT", kubernetesPath)
}
