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

package idfile

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

func TestParseNodeMapping(t *testing.T) {
	testCases := []struct {
		inputName     string
		input         map[string]string
		output        MappingInfo
		expectedError string
	}{
		{
			inputName: "foo",
			input: map[string]string{
				"process-index": "0",
				"address":       "192.145",
				"generation":    "3",
				"job":           "foo",
				"job-namespace": "bar",
			},
			output: MappingInfo{
				NodeName:     "foo",
				ProcessIndex: 0,
				Address:      "192.145",
				Generation:   3,
				Job:          types.NamespacedName{Namespace: "bar", Name: "foo"},
			},
		},
		{
			inputName: "foo",
			input: map[string]string{
				"address":    "192.145",
				"generation": "3",
			},
			expectedError: "no process index",
		},
		{
			inputName: "foo",
			input: map[string]string{
				"address":       "192.145",
				"process-index": "foo",
				"generation":    "3",
			},
			expectedError: "bad process index",
		},
		{
			inputName: "foo",
			input: map[string]string{
				"process-index": "3",
			},
			output: MappingInfo{
				NodeName:     "foo",
				ProcessIndex: 3,
				Generation:   -1,
				Job:          types.NamespacedName{Namespace: "default"},
			},
		},
		{
			inputName: "foo",
			input: map[string]string{
				"address":       "192.145",
				"process-index": "7",
				"generation":    "foo",
			},
			expectedError: "bad generation",
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			configMap := corev1.ConfigMap{Data: tc.input}
			configMap.SetName(tc.inputName)
			info, err := parseNodeMapping(&configMap)
			if tc.expectedError != "" {
				assert.ErrorContains(t, err, tc.expectedError)
			} else {
				assert.DeepEqual(t, tc.output, info)
			}
		})
	}
}

func TestParseCoordinatorInfo(t *testing.T) {
	testCases := []struct {
		name          string
		input         map[string]string
		output        CoordinatorInfo
		expectedError string
	}{
		{
			name: "foo",
			input: map[string]string{
				"address":    "192.168.0.1",
				"generation": "3",
			},
			output: CoordinatorInfo{
				Address:    "192.168.0.1",
				Generation: 3,
				Job:        types.NamespacedName{Name: "foo"},
			},
		},
		{
			name: "foo",
			input: map[string]string{
				"address": "192.168.0.1",
			},
			output: CoordinatorInfo{
				Address:    "192.168.0.1",
				Generation: -1,
				Job:        types.NamespacedName{Name: "foo"},
			},
		},
		{
			name: "foo",
			input: map[string]string{
				"address":    "192.168.0.1",
				"generation": "foo",
			},
			expectedError: "bad generation",
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			configMap := corev1.ConfigMap{Data: tc.input}
			configMap.SetName(tc.name)
			info, err := parseCoordinatorInfo(&configMap, types.NamespacedName{Name: tc.name})
			if tc.expectedError != "" {
				assert.ErrorContains(t, err, tc.expectedError)
			} else {
				assert.DeepEqual(t, tc.output, info)
			}
		})
	}
}

func TestMappingOperations(t *testing.T) {
	svr, err := NewIdFileServer(IdFileServerOpts{NumWorkers: 6, SliceSize: 2})
	assert.NilError(t, err)

	_, err = svr.AddNode("a", "p", 0)
	assert.NilError(t, err)
	svr.RemoveNode("a", "p")
	_, found := svr.GetNode("a")
	assert.Equal(t, found, false)
	info, err := svr.AddNode("a", "p", 0)
	assert.NilError(t, err)
	assert.DeepEqual(t, MappingInfo{"a", "", 0, -1, types.NamespacedName{}, "", ""}, info)

	assert.Equal(t, svr.GetFreeIndex(0), 1)
	idx := svr.GetFreeIndex(2)
	if idx != 4 && idx != 5 {
		t.Errorf("expected slice 2 free index to be 4 or 5, but got %d", idx)
	}
	assert.Equal(t, svr.GetFreeIndex(3), -1)
	_, err = svr.AddNode("b", "p", 1)
	assert.NilError(t, err)
	assert.Equal(t, svr.GetFreeIndex(0), -1)

	info, found = svr.GetIndex(0)
	assert.Equal(t, found, true)
	assert.Equal(t, info.NodeName, "a")
	_, err = svr.AddNode("c", "p", 0)
	assert.ErrorContains(t, err, "node a already at index 0")
	svr.RemoveNode("a", "p")
	_, err = svr.AddNode("c", "p", 0)
	assert.NilError(t, err)
	info, found = svr.GetIndex(0)
	assert.Equal(t, found, true)
	assert.Equal(t, info.NodeName, "c")

	_, err = svr.AddNode("c", "p", 4)
	assert.ErrorContains(t, err, "tried to add node c in slice 2 but pool p is slice 0")
	info, found = svr.GetIndex(0)
	assert.Equal(t, found, true)
	assert.Equal(t, info.ProcessIndex, 0)
}

func TestIndicesInSlice(t *testing.T) {
	svr, err := NewIdFileServer(IdFileServerOpts{NumWorkers: 6, SliceSize: 2})
	assert.NilError(t, err)

	// test fallback slice
	i, err := svr.FindIndexInSlice("a", "0", 0, 1)
	assert.NilError(t, err)
	assert.Equal(t, i, 1)
	_, err = svr.AddNode("a", "0", i)
	assert.NilError(t, err)
	info, found := svr.GetNode("a")
	assert.Equal(t, found, true)
	assert.Equal(t, info.ProcessIndex, 1)

	// test fallback slice not available
	i, err = svr.FindIndexInSlice("b", "0", 0, 1)
	assert.NilError(t, err)
	assert.Equal(t, i, 0)
	_, err = svr.AddNode("b", "0", i)

	// test existing node
	_, err = svr.AddNode("c", "1", 2)
	assert.NilError(t, err)
	i, err = svr.FindIndexInSlice("c", "1", 1, 3)
	assert.NilError(t, err)
	assert.Equal(t, i, 2)

	// test no fallback
	i, err = svr.FindIndexInSlice("d", "1", 1, -1)
	assert.NilError(t, err)
	assert.Equal(t, i, 3)
	_, err = svr.AddNode("d", "1", 3)
	assert.NilError(t, err)

	i, err = svr.FindIndexInSlice("e", "1", 1, -1)
	assert.NilError(t, err)
	assert.Equal(t, i, -1)
}

func TestInconsistentSlices(t *testing.T) {
	svr, err := NewIdFileServer(IdFileServerOpts{NumWorkers: 6, SliceSize: 2})

	_, err = svr.AddNode("a", "p1", 0)
	assert.NilError(t, err)
	_, err = svr.AddNode("b", "p2", 1)
	assert.ErrorContains(t, err, "slice collision")
}

func TestJobsetNaming(t *testing.T) {
	assert.Equal(t, jobsetMappingName(types.NamespacedName{Namespace: "foo", Name: "bar"}), "foo-bar")
	assert.Equal(t, jobsetMappingName(types.NamespacedName{Namespace: "", Name: "bar"}), "default-bar")
}

func TestFetchNodeInformation(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)
	const namespace = "default"

	c, err := NewIdFileClient(IdFileClientOpts{kubeClient, namespace, "node-0"})
	assert.NilError(t, err)

	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-0",
		},
		Data: map[string]string{
			jobKey:          "job",
			jobNamespaceKey: "job-namespace",
			addressKey:      "192.168.0.1",
			processIndexKey: "2",
			generationKey:   "1",
			podKey:          "job-slice-01",
			podUIDKey:       "job-slice-01.0",
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err)
	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
		},
		Data: map[string]string{
			jobKey:          "job",
			jobNamespaceKey: "job-namespace",
			addressKey:      "192.168.0.2",
			processIndexKey: "0",
			generationKey:   "1",
			podKey:          "job-slice-02",
			podUIDKey:       "job-slice-02.0",
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err)
	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "job-namespace-job",
		},
		Data: map[string]string{
			addressKey:    "192.168.0.3",
			generationKey: "1",
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err)

	// FetchNodeInformation waits 60m to timeout. That's too long for the test, so we timeout here.
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, _, err := c.FetchNodeInformation(ctx, "bad-uid")
		return false, err
	})
	assert.ErrorContains(t, err, "context deadline exceeded")

	info, coord, err := c.FetchNodeInformation(ctx, "job-slice-01.0")
	assert.NilError(t, err)
	assert.Equal(t, info.ProcessIndex, 2)
	assert.Equal(t, coord.Address, "192.168.0.3")

	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		_, err = kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, "node-0", metav1.GetOptions{})
		return strings.Contains(err.Error(), "not found"), nil
	})
	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, "node-1", metav1.GetOptions{})
	assert.NilError(t, err)
	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, "job-namespace-job", metav1.GetOptions{})
	assert.NilError(t, err)
}

func TestFetchNodeInformationNoCoordinator(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)
	const namespace = "default"

	c, err := NewIdFileClient(IdFileClientOpts{kubeClient, namespace, "node-0"})
	assert.NilError(t, err)

	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-0",
		},
		Data: map[string]string{
			jobKey:          "job",
			jobNamespaceKey: "job-namespace",
			addressKey:      "192.168.0.1",
			processIndexKey: "2",
			generationKey:   "1",
			podKey:          "job-slice-01",
			podUIDKey:       "job-slice-01.0",
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err)

	getInfo := func() error {
		// FetchNodeInformation waits 60m to timeout. That's too long for the test, so we timeout here.
		return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
			_, _, err := c.FetchNodeInformation(ctx, "job-slice-01.0")
			return true, err
		})
	}
	err = getInfo()
	assert.ErrorContains(t, err, "context deadline exceeded")

	t.Log("creating coordinator info")
	// After adding the coordinator info, the same get succeeds.
	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "job-namespace-job",
		},
		Data: map[string]string{
			addressKey:    "192.168.0.3",
			generationKey: "1",
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err)

	err = getInfo()
	assert.NilError(t, err)
}

func TestFetchNodeInformationBadGeneration(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)
	const namespace = "default"

	c, err := NewIdFileClient(IdFileClientOpts{kubeClient, namespace, "node-0"})
	assert.NilError(t, err)

	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-0",
		},
		Data: map[string]string{
			jobKey:          "job",
			jobNamespaceKey: "job-namespace",
			addressKey:      "192.168.0.1",
			processIndexKey: "2",
			generationKey:   "1",
			podKey:          "job-slice-01",
			podUIDKey:       "job-slice-01.0",
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err)
	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "job-namespace-job",
		},
		Data: map[string]string{
			addressKey:    "192.168.0.3",
			generationKey: "0",
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err)

	// FetchNodeInformation waits 60m to timeout. That's too long for the test, so we timeout here.
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, _, err := c.FetchNodeInformation(ctx, "job-slice-01.0")
		return false, err
	})
	assert.ErrorContains(t, err, "context deadline exceeded")
	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, "node-0", metav1.GetOptions{})
	assert.NilError(t, err)
}
