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

package util

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

const (
	EnvGKEComponentVersion = "GKE_COMPONENT_VERSION"
	CpcAnnotation          = "highscalecheckpointing.gke.io/cpc"
)

func BuildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func ParseEndpoint(endpoint string) (string, string) {
	u, err := url.Parse(endpoint)
	if err != nil {
		klog.Fatal(err.Error())
	}

	var addr string
	switch u.Scheme {
	case "unix":
		addr = u.Path

		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			klog.Fatalf("Failed to remove %s, error: %s", addr, err)
		}
		listenDir := filepath.Dir(addr)
		if _, err := os.Stat(listenDir); err != nil {
			if os.IsNotExist(err) {
				klog.Fatalf("expected Kubelet plugin watcher to create parent dir %s but did not find such a dir", listenDir)
			} else {
				klog.Fatalf("failed to stat %s: %v", listenDir, err)
			}
		}
	case "tcp":
		addr = u.Host
	default:
		klog.Fatalf("%v endpoint scheme not supported", u.Scheme)
	}

	return u.Scheme, addr
}

func GetAnnotationInt(annotations map[string]string, key string) (int, error) {
	if annotations == nil {
		return 0, fmt.Errorf("nil annotations")
	}
	str, found := annotations[key]
	if !found {
		return 0, fmt.Errorf("missing annotation %s", key)
	}
	val, err := strconv.Atoi(str)
	if err != nil {
		return 0, fmt.Errorf("bad annotation integer %s=%s", key, str)
	}
	return val, nil
}

func GetEnvVar(envVarName string) string {
	v, ok := os.LookupEnv(envVarName)
	if !ok {
		klog.Warningf("%q env not set", envVarName)
		return ""
	}
	return v
}
