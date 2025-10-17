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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	infoFile          = flag.String("info-file", "", "info to watch for")
	doneFile          = flag.String("done-file", "", "file to watch for to exit")
	failFile          = flag.String("fail-file", "", "file to watch to force failure")
	finishImmediately = flag.Bool("finish-immediately", false, "if true, exit successfully after idfile")
	podName           = flag.String("pod-name", "", "pod name to use for configmap")
	namespace         = flag.String("namespace", "", "namespace to register configmap")
	ip                = flag.String("ip", "", "ip of pod")
	node              = flag.String("node", "", "the node the pod is running on")
)

func init() {
	klog.InitFlags(flag.CommandLine)
	flag.Set("logtostderr", "true")
}

func readInfo(filename string) (int, string, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return 0, "", err
	}
	info := strings.TrimSpace(string(contents))
	parts := strings.Split(info, "\n")
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("bad info file contents: %s", info)
	}
	idx, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, "", err
	}
	coordinator := strings.TrimSpace(parts[1])
	if !strings.Contains(coordinator, ":") {
		return 0, "", fmt.Errorf("bad coordinator: %s", info)
	}
	return idx, coordinator, nil
}

func main() {
	flag.Parse()

	ctx := context.Background()

	startTime := time.Now()

	kubeClient, err := kubernetes.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		klog.Fatalf("Could not create kubeclient: %v", err)
	}

	if *ip == "" || *infoFile == "" || *podName == "" || *namespace == "" || *node == "" {
		klog.Fatalf("Missing flags")
	}

	var index int
	var coordinator string
	for {
		if *failFile != "" {
			_, err := os.Stat(*failFile)
			if err == nil {
				klog.Infof("Found fail file %s, exiting badly", *failFile)
				os.Exit(1)
			}
		}

		var err error
		index, coordinator, err = readInfo(*infoFile)
		if err == nil {
			break
		}
		klog.Infof("Can't read info file: %v", err)
		time.Sleep(time.Second)
	}

	cmName := "worker-" + *podName
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: *namespace,
		},
		Data: map[string]string{
			"index":       strconv.Itoa(index),
			"coordinator": coordinator,
			"ip":          *ip,
			"start-time":  startTime.Format(time.RFC3339Nano),
			"idx-time":    time.Now().Format(time.RFC3339Nano),
			"node":        *node,
		},
	}
	kubeClient.CoreV1().ConfigMaps(*namespace).Delete(ctx, cmName, metav1.DeleteOptions{})
	for {
		_, err := kubeClient.CoreV1().ConfigMaps(*namespace).Create(ctx, &cm, metav1.CreateOptions{})
		if err == nil {
			break
		}
		klog.Errorf("could not update config map %s, retrying: %v", cm.GetName(), err)
		time.Sleep(time.Second)
	}

	if *finishImmediately {
		klog.Infof("idfile found, --finish-immediately")
		os.Exit(0)
	}

	klog.Infof("Updated %s/%s, sleeping", *namespace, cmName)

	for {
		if *doneFile != "" {
			_, err := os.Stat(*doneFile)
			if err == nil {
				klog.Infof("Found done file %s, exiting gracefully", *doneFile)
				os.Exit(0)
			}
		}
		if *failFile != "" {
			_, err := os.Stat(*failFile)
			if err == nil {
				klog.Infof("Found fail file %s, exiting badly", *failFile)
				os.Exit(1)
			}
		}

		time.Sleep(time.Second)
	}
}
