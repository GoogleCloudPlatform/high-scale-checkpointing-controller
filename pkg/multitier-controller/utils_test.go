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

package multitiercontroller

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"

	checkpoint "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/apis/checkpointing.gke.io/v1"
)

func TestNewPreparer(t *testing.T) {
	const mapName = "image-config-map"

	type testcase struct {
		name                      string
		imageMap                  *corev1.ConfigMap
		customMap                 *corev1.ConfigMap
		cpc                       *checkpoint.CheckpointConfiguration
		errString                 string
		expectedRegistrar         string
		expectedCsi               string
		expectedReplicationWorker string
		expectedNfsServer         string
		expectedMetricsCollector  string
		expectedTokenBroker       string
		metricsConfig             *MetricsCollectorConfig
	}

	runTest := func(t *testing.T, tc testcase) {
		k8sClient, _, cleanup := createClient(t)
		defer cleanup()

		s := scheme.Scheme
		checkpoint.AddToScheme(s)
		ctx := context.Background()

		config := &csiconfig{
			csiNamespace:       testCsiNamespace,
			imageConfigMapName: mapName,
		}

		if tc.imageMap != nil {
			_, err := k8sClient.CoreV1().ConfigMaps(tc.imageMap.GetNamespace()).Create(ctx, tc.imageMap, metav1.CreateOptions{})
			assert.NilError(t, err)
		}
		if tc.customMap != nil {
			_, err := k8sClient.CoreV1().ConfigMaps(tc.customMap.GetNamespace()).Create(ctx, tc.customMap, metav1.CreateOptions{})
			assert.NilError(t, err)
		}
		cpc := &checkpoint.CheckpointConfiguration{
			Spec: checkpoint.CheckpointConfigurationSpec{
				CloudStorageBucketName: "gcs-bucket",
				InMemoryVolumeSize:     "64Gi",
			},
		}
		if tc.cpc != nil {
			cpc = tc.cpc
		}
		p, err := newPreparer(ctx, k8sClient, config, cpc, tc.metricsConfig)
		if tc.errString == "" {
			assert.NilError(t, err)
			assert.Equal(t, p.registrarImage, tc.expectedRegistrar)
			assert.Equal(t, p.csiImage, tc.expectedCsi)
			assert.Equal(t, p.replicationWorkerImage, tc.expectedReplicationWorker)
			assert.Equal(t, p.nfsServerImage, tc.expectedNfsServer)
			assert.Equal(t, p.metricsCollectorImage, tc.expectedMetricsCollector)
			assert.Equal(t, p.tokenBrokerImage, tc.expectedTokenBroker)
		} else {
			assert.ErrorContains(t, err, tc.errString)
		}
	}

	for _, tc := range []testcase{
		{
			name: "success no override",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
		},
		{
			name: "success missing override",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "a-missing-map",
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
		},
		{
			name: "success empty override",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "custom",
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
		},
		{
			name: "success override registrar",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "custom",
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom",
				},
				Data: map[string]string{
					"registrar-image": "custom",
				},
			},
			expectedRegistrar:         "custom",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
		},
		{
			name: "success override csi",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "custom",
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom",
				},
				Data: map[string]string{
					"csi-image": "custom",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "custom",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
		},
		{
			name: "success override replication",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "custom",
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom",
				},
				Data: map[string]string{
					"replication-worker-image": "custom",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "custom",
			expectedNfsServer:         "nfs",
		},
		{
			name: "success override nfs",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "custom",
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom",
				},
				Data: map[string]string{
					"nfs-server-image": "custom",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "custom",
		},
		{
			name: "success override all",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "custom",
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom",
				},
				Data: map[string]string{
					"registrar-image":          "custom-registrar",
					"csi-image":                "custom-csi",
					"replication-worker-image": "custom-replication",
					"nfs-server-image":         "custom-nfs",
				},
			},
			expectedRegistrar:         "custom-registrar",
			expectedCsi:               "custom-csi",
			expectedReplicationWorker: "custom-replication",
			expectedNfsServer:         "custom-nfs",
		},
		{
			name: "success default override name",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom-checkpoint-image-config",
				},
				Data: map[string]string{
					"registrar-image": "custom-registrar",
				},
			},
			expectedRegistrar:         "custom-registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
		},
		{
			name: "success missing image got override",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"custom-image-config-name": "custom",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			customMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "custom",
				},
				Data: map[string]string{
					"registrar-image": "custom-registrar",
				},
			},
			expectedRegistrar:         "custom-registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
		},
		{
			name: "missing image",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image": "registrar",
					"csi-image":       "csi",
					// missing replication worker
					"nfs-server-image": "nfs",
				},
			},
			errString: "missing image",
		},
		{
			name: "missing map",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      "some-other-map",
				},
			},
			errString: "not found",
		},
		{
			name: "missing cloudStorageBucketName",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			cpc: &checkpoint.CheckpointConfiguration{
				Spec: checkpoint.CheckpointConfigurationSpec{
					InMemoryVolumeSize: "64Gi",
				},
			},
			errString: "cloudStorageBucketName need to be specified in the checkpointconfiguration",
		},
		{
			name: "missing InMemoryVolumeSize",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			cpc: &checkpoint.CheckpointConfiguration{
				Spec: checkpoint.CheckpointConfigurationSpec{
					CloudStorageBucketName: "gcs-bucket",
				},
			},
			errString: "invalid inMemoryVolumeSize format:  quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
		},
		{
			name: "wrong InMemoryVolumeSize format",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
				},
			},
			cpc: &checkpoint.CheckpointConfiguration{
				Spec: checkpoint.CheckpointConfigurationSpec{
					CloudStorageBucketName: "gcs-bucket",
					InMemoryVolumeSize:     "64GB",
				},
			},
			errString: "invalid inMemoryVolumeSize format:  quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
		},
		{
			name: "success with metrics collector turned on",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
					"metrics-collector-image":  "metrics-collector",
					"token-broker-image":       "token-broker",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
			expectedMetricsCollector:  "metrics-collector",
			expectedTokenBroker:       "token-broker",
			metricsConfig:             &MetricsCollectorConfig{},
		},
		{
			name: "missing token broker image",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"replication-worker-image": "replication",
					"nfs-server-image":         "nfs",
					"metrics-collector-image":  "metrics-collector",
				},
			},
			expectedRegistrar:         "registrar",
			expectedCsi:               "csi",
			expectedReplicationWorker: "replication",
			expectedNfsServer:         "nfs",
			expectedMetricsCollector:  "metrics-collector",
			errString:                 "node metrics collection is on but missing metrics-collector-image or token-broker-image",
			metricsConfig:             &MetricsCollectorConfig{},
		},
		{
			name: "missing metrics collector related images",
			imageMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testCsiNamespace,
					Name:      mapName,
				},
				Data: map[string]string{
					"registrar-image":          "registrar",
					"csi-image":                "csi",
					"nfs-server-image":         "nfs",
					"replication-worker-image": "replication",
				},
			},
			errString:     "node metrics collection is on but missing metrics-collector-image or token-broker-image",
			metricsConfig: &MetricsCollectorConfig{},
		},
	} {
		thisTC := tc
		t.Run(tc.name, func(t *testing.T) { runTest(t, thisTC) })
	}
}

func TestDaemonsetIsReasonable(t *testing.T) {
	k8sClient, _, cleanup := createClient(t)
	defer cleanup()

	s := scheme.Scheme
	checkpoint.AddToScheme(s)
	ctx := context.Background()

	mapName := "image-map"
	config := &csiconfig{
		csiNamespace:       testCsiNamespace,
		imageConfigMapName: mapName,
	}

	cpc := &checkpoint.CheckpointConfiguration{
		Spec: checkpoint.CheckpointConfigurationSpec{
			CloudStorageBucketName: "gcs-bucket",
			InMemoryVolumeSize:     "64Gi",
			GcsFuseMountOptions: []string{
				"option=1",
				"alternative=2",
			},
		},
	}

	imageMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testCsiNamespace,
			Name:      mapName,
		},
		Data: map[string]string{
			"registrar-image":          "registrar",
			"csi-image":                "csi",
			"replication-worker-image": "replication",
			"nfs-server-image":         "nfs",
		},
	}

	_, err := k8sClient.CoreV1().ConfigMaps(testCsiNamespace).Create(ctx, imageMap, metav1.CreateOptions{})
	assert.NilError(t, err)

	p, err := newPreparer(ctx, k8sClient, config, cpc, nil)
	assert.NilError(t, err)
	ds := p.prepareCSIDaemonSet("test")

	assert.Equal(t, ds.GetNamespace(), testCsiNamespace)
	assert.Equal(t, ds.GetName(), "test")
	assert.Equal(t, len(ds.Spec.Template.Spec.Containers), 4)
	found := map[string]bool{}
	for _, c := range ds.Spec.Template.Spec.Containers {
		found[c.Image] = true
	}
	assert.Equal(t, len(found), 4)
	assert.Assert(t, found["registrar"])
	assert.Assert(t, found["csi"])
	assert.Assert(t, found["replication"])
	assert.Assert(t, found["nfs"])

	foundOptions := false
	for _, vol := range ds.Spec.Template.Spec.Volumes {
		if vol.VolumeSource.CSI == nil || vol.VolumeSource.CSI.Driver != "gcsfuse.csi.storage.gke.io" {
			continue
		}
		assert.Equal(t, vol.VolumeSource.CSI.VolumeAttributes["mountOptions"], "option=1,alternative=2")
		foundOptions = true
	}
	assert.Assert(t, foundOptions)
}

func TestConvertQuantityToInt64(t *testing.T) {
	tests := []struct {
		desc        string
		inputStr    string
		expInt64    int64
		expectError bool
	}{
		{
			desc:        "valid number string",
			inputStr:    "10000",
			expInt64:    1,
			expectError: false,
		},
		{
			desc:        "round Ki to MiB",
			inputStr:    "1000Ki",
			expInt64:    1,
			expectError: false,
		},
		{
			desc:        "round k to MiB",
			inputStr:    "1000k",
			expInt64:    1,
			expectError: false,
		},
		{
			desc:        "round Mi to MiB",
			inputStr:    "1000Mi",
			expInt64:    1000,
			expectError: false,
		},
		{
			desc:        "round M to MiB",
			inputStr:    "1000M",
			expInt64:    954,
			expectError: false,
		},
		{
			desc:        "round G to MiB",
			inputStr:    "1000G",
			expInt64:    953675,
			expectError: false,
		},
		{
			desc:        "round Gi to MiB",
			inputStr:    "10000Gi",
			expInt64:    10240000,
			expectError: false,
		},
		{
			desc:        "round decimal to MiB",
			inputStr:    "1.2Gi",
			expInt64:    1229,
			expectError: false,
		},
		{
			desc:        "round big value to MiB",
			inputStr:    "8191Pi",
			expInt64:    8795019280384,
			expectError: false,
		},
		{
			desc:        "invalid empty string",
			inputStr:    "",
			expInt64:    0,
			expectError: true,
		},
		{
			desc:        "invalid KiB string",
			inputStr:    "10KiB",
			expInt64:    10000,
			expectError: true,
		},
		{
			desc:        "invalid GB string",
			inputStr:    "10GB",
			expInt64:    0,
			expectError: true,
		},
		{
			desc:        "invalid string",
			inputStr:    "ew%65",
			expInt64:    0,
			expectError: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			actualInt64, err := convertQuantityToInt64(tc.inputStr)
			if err != nil && !tc.expectError {
				t.Errorf("Got error %v converting string to int64 %s; expect no error", err, tc.inputStr)
			}
			if err == nil && tc.expectError {
				t.Errorf("Got no error converting string to int64 %s; expect an error", tc.inputStr)
			}
			if err == nil && actualInt64 != tc.expInt64 {
				t.Errorf("Got %d for converting string to int64; expect %d", actualInt64, tc.expInt64)
			}
		})
	}
}
