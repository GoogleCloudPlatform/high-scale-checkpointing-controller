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
	"fmt"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	checkpoint "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/apis/checkpointing.gke.io/v1"
	testutil "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/hack"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	testApiVersion             = "v1"
	testCsiNamespace           = "checkpoint-namespace"
	testCsiServiceAccountName  = "checkpoint-csi-sa"
	testRegistrarImage         = "registrar-image"
	testCsiImage               = "csi-image"
	testReplicationWorkerImage = "replication-worker-image"
	testNfsServerImage         = "nfs-server-image"
	testImageConfigMapName     = "image-config-map"

	testCPCName            = "cpc"
	testCPCNamespace       = "default"
	testCpcUid             = "cpc-uuid"
	testGCSbucket          = "gcs-bucket"
	testInMemoryVolumeSize = "64Gi"
	CSIDriverName          = "multitier-checkpoint.csi.storage.gke.io"
)

var (
	testNodeSelector = map[string]string{"key": "value"}
	testTolerations  = []corev1.Toleration{{
		Key: "google.com/tpu",
	}}

	testFinalizer = cpcGK.Group + "/" + finalizerSuffix

	csicfg = csiconfig{
		csiNamespace:          testCsiNamespace,
		csiServiceAccountName: testCsiServiceAccountName,
		csiDriverName:         CSIDriverName,
		imageConfigMapName:    testImageConfigMapName,
	}
)

func createClient(t *testing.T) (*kubernetes.Clientset, client.Client, func()) {
	testEnv := &envtest.Environment{
		UseExistingCluster: ptr.To(false),
		CRDDirectoryPaths:  []string{"../../test/crds", "../../crd"},
	}
	var err error
	testCfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("cannot start testenv: %v", err)
	}

	checkpoint.AddToScheme(scheme.Scheme)
	k8sClient, err := kubernetes.NewForConfig(testCfg)
	if err != nil {
		t.Fatalf("cannot get kubeclient: %v", err)

	}
	kubeClient, err := client.New(testCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("cannot create client: %v", err)
	}

	ctx := context.Background()
	if _, err := k8sClient.CoreV1().Namespaces().Get(ctx, testCsiNamespace, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		ns := &corev1.Namespace{}
		ns.SetName(testCsiNamespace)
		if _, err := k8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Can't create namespace %s: %v", testCsiNamespace, err)
		}
	}
	return k8sClient, kubeClient, func() {
		if err := testEnv.Stop(); err != nil {
			t.Fatalf("couldn't tear down testenv: %v", err)
		}
	}
}

func initReconciler(t *testing.T) (*reconciler, func(), error) {
	k8sClient, clientset, cleanup := createClient(t)

	s := scheme.Scheme
	checkpoint.AddToScheme(s)
	createImageMap(t, k8sClient)
	rec := &reconciler{
		scheme:    s,
		Client:    clientset,
		k8sClient: k8sClient,
		gvr:       CPCGVR,
		csiConfig: csicfg,
		finalizer: testFinalizer,
	}

	return rec, cleanup, nil
}

func createImageMap(t *testing.T, k8sClient *kubernetes.Clientset) {
	imageMap := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testCsiNamespace,
			Name:      testImageConfigMapName,
		},
		Data: map[string]string{
			"registrar-image":          testRegistrarImage,
			"csi-image":                testCsiImage,
			"replication-worker-image": testReplicationWorkerImage,
			"nfs-server-image":         testNfsServerImage,
		},
	}
	_, err := k8sClient.CoreV1().ConfigMaps(testCsiNamespace).Create(context.Background(), &imageMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cannot create image map: %v", err)
	}
}

func TestMain(m *testing.M) {
	ctrl.SetLogger(zap.New())
	testutil.SetupEnviron(context.TODO())

	m.Run()
}

func TestReconcile(t *testing.T) {
	// Create a testenv in order to create a reference daemonset.
	k8sClient, _, cleanup := createClient(t)
	createImageMap(t, k8sClient)
	p, err := newPreparer(context.Background(), k8sClient, &csicfg, &checkpoint.CheckpointConfiguration{
		Spec: checkpoint.CheckpointConfigurationSpec{
			CloudStorageBucketName: testGCSbucket,
			InMemoryVolumeSize:     testInMemoryVolumeSize,
		},
	}, nil)
	if err != nil {
		cleanup()
		t.Fatalf("can't create preparer: %v", err)
	}
	baseDS := p.prepareCSIDaemonSet("baseDS")
	cleanup()

	tests := []struct {
		name           string
		cpc            *checkpoint.CheckpointConfiguration
		expectedCPC    *checkpoint.CheckpointConfiguration
		markCPCDeleted bool

		createDS bool

		expectDS              bool
		expectedDSSelector    map[string]string
		expectedDSTolerations []corev1.Toleration
		expectedResult        ctrl.Result
		expectedError         error
	}{
		{
			name:           "checkpointconfiguration no longer exists",
			expectedResult: ctrl.Result{},
		},
		{
			name:           "deleting the checkpointconfiguration and corresponding CSI daemonset",
			cpc:            cpc(testGCSbucket, testFinalizer, testInMemoryVolumeSize, testNodeSelector, testTolerations),
			expectedCPC:    cpc(testGCSbucket, testFinalizer, testInMemoryVolumeSize, testNodeSelector, testTolerations),
			markCPCDeleted: true,
			createDS:       true,
			expectedResult: ctrl.Result{Requeue: true},
		},
		{
			name:           "deleting the checkpointconfiguration remove finalizer",
			cpc:            cpc(testGCSbucket, testFinalizer, testInMemoryVolumeSize, testNodeSelector, testTolerations),
			expectedCPC:    nil,
			markCPCDeleted: true,
			expectedResult: ctrl.Result{},
		},
		{
			name:                  "create CSI DaemonSet successfully",
			cpc:                   cpc(testGCSbucket, "", testInMemoryVolumeSize, testNodeSelector, testTolerations),
			expectedCPC:           cpc(testGCSbucket, testFinalizer, testInMemoryVolumeSize, testNodeSelector, testTolerations),
			expectDS:              true,
			expectedDSSelector:    testNodeSelector,
			expectedDSTolerations: testTolerations,
			expectedResult:        ctrl.Result{},
		},
		{
			name:           "create CSI DaemonSet, empty cloudStorageBucketName",
			cpc:            cpc("", "", testInMemoryVolumeSize, testNodeSelector, testTolerations),
			expectedCPC:    cpc("", "", testInMemoryVolumeSize, testNodeSelector, testTolerations),
			expectedResult: ctrl.Result{},
			expectedError:  fmt.Errorf("could not create preparer: cloudStorageBucketName need to be specified in the checkpointconfiguration. checkpointconfiguration %s", fmt.Sprintf("%s/%s", testCPCNamespace, testCPCName)),
		},
		{
			name:           "create CSI DaemonSet, empty inMemoryVolumeSize",
			cpc:            cpc(testGCSbucket, "", "", testNodeSelector, testTolerations),
			expectedCPC:    cpc(testGCSbucket, "", "", testNodeSelector, testTolerations),
			expectedResult: ctrl.Result{},
			expectedError:  fmt.Errorf("could not create preparer: invalid inMemoryVolumeSize format:  quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'. checkpointconfiguration %s", fmt.Sprintf("%s/%s", testCPCNamespace, testCPCName)),
		},
		{
			name:           "create CSI DaemonSet, invalid inMemoryVolumeSize wrong suffix",
			cpc:            cpc(testGCSbucket, "", "64AIB", testNodeSelector, testTolerations),
			expectedCPC:    cpc(testGCSbucket, "", "", testNodeSelector, testTolerations),
			expectedResult: ctrl.Result{},
			expectedError:  fmt.Errorf("could not create preparer: invalid inMemoryVolumeSize format:  quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'. checkpointconfiguration %s", fmt.Sprintf("%s/%s", testCPCNamespace, testCPCName)),
		},
		{
			name:           "create CSI DaemonSet, invalid inMemoryVolumeSize number",
			cpc:            cpc(testGCSbucket, "", "abMIB", testNodeSelector, testTolerations),
			expectedCPC:    cpc(testGCSbucket, "", "", testNodeSelector, testTolerations),
			expectedResult: ctrl.Result{},
			expectedError:  fmt.Errorf("could not create preparer: invalid inMemoryVolumeSize format:  quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'. checkpointconfiguration %s", fmt.Sprintf("%s/%s", testCPCNamespace, testCPCName)),
		},
		{
			name:                  "create CSI DaemonSet, nil nodeselector",
			cpc:                   cpc(testGCSbucket, "", testInMemoryVolumeSize, nil, testTolerations),
			expectedCPC:           cpc(testGCSbucket, testFinalizer, testInMemoryVolumeSize, testNodeSelector, testTolerations),
			expectDS:              true,
			expectedDSTolerations: testTolerations,
			expectedResult:        ctrl.Result{},
		},
		{
			name:               "create CSI DaemonSet, nil tolerations",
			cpc:                cpc(testGCSbucket, "", testInMemoryVolumeSize, testNodeSelector, nil),
			expectedCPC:        cpc(testGCSbucket, testFinalizer, testInMemoryVolumeSize, testNodeSelector, nil),
			expectDS:           true,
			expectedDSSelector: testNodeSelector,
			expectedResult:     ctrl.Result{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, cleanup, err := initReconciler(t)
			defer cleanup()
			if err != nil {
				t.Fatalf("Failed initialize reconciler %v", err)
			}

			if test.cpc != nil {
				err = r.Create(context.TODO(), test.cpc)
				if err != nil {
					t.Fatalf("Error creating checkpointconfiguration: %v", err)
				}
				t.Logf("created CPC %s", test.cpc.GetName())
			}
			// Add deletion timestamp to the CPC
			if test.markCPCDeleted {
				err = r.Delete(context.TODO(), test.cpc)
				if err != nil {
					t.Fatalf("Error deleting checkpointconfiguration: %v", err)
				}
			}
			dsName := ""
			if test.cpc != nil {
				dsName = fmt.Sprintf("multitier-driver-%s", test.cpc.GetUID())
			}
			if test.createDS {
				if dsName == "" {
					t.Fatalf("need cpc to create daemonset")
				}
				ds := baseDS.DeepCopy()
				ds.SetName(dsName)
				ds.Spec.Template.Spec.NodeSelector = test.cpc.Spec.NodeSelector
				ds.Spec.Template.Spec.Tolerations = test.cpc.Spec.Tolerations
				err = r.Create(context.TODO(), ds)
				if err != nil {
					t.Fatalf("Error creating CSI daemonset: %v", err)
				}
				t.Logf("created daemonset %s", ds.Name)
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: testCPCNamespace,
					Name:      testCPCName,
				},
			}
			res, err := r.Reconcile(context.TODO(), req)
			if !compareResult(&test.expectedResult, &res) {
				t.Errorf("Expected result: %+v, actual result: %+v", test.expectedResult, res)
			}
			if !compareErrors(test.expectedError, err) {
				t.Errorf("Expected error: %+v, actual error: %+v", test.expectedError, err)
			}

			var actualCPC checkpoint.CheckpointConfiguration
			err = r.Get(context.TODO(), req.NamespacedName, &actualCPC)
			if actualCPC.GetNamespace() == "" {
				actualCPC.SetNamespace("default")
			}
			if test.expectedCPC != nil {
				// Either the expected CPC is a modified version of the initial CPC, or it's newly
				// created, in which case the UID is generated.
				if test.cpc != nil {
					test.expectedCPC.SetUID(test.cpc.GetUID())
				} else {
					test.expectedCPC.SetUID(actualCPC.GetUID())
				}
				if err != nil {
					t.Errorf("Error getting checkpointconfiguration %v: %v", req.NamespacedName, err)
				} else if !compareCPC(test.expectedCPC, &actualCPC) {
					t.Errorf("Expected checkpointconfiguration: %+v, actual checkpointconfiguration: %+v", test.expectedCPC, &actualCPC)
				}
			} else if !errors.IsNotFound(err) {
				t.Errorf("expected no cpc, but found %s", actualCPC.GetName())
			}

			var actualDS appsv1.DaemonSet
			err = errors.NewNotFound(appsv1.Resource("DaemonSet"), "")
			if dsName != "" {
				err = r.Get(context.TODO(), types.NamespacedName{Namespace: testCsiNamespace, Name: dsName}, &actualDS)
			}
			if test.expectDS {
				if err != nil {
					t.Errorf("Error getting CSI daemonset: %v", err)
				} else {
					expectedDS := baseDS.DeepCopy()
					expectedDS.SetName(dsName)
					expectedDS.Spec.Template.Spec.NodeSelector = test.expectedDSSelector
					expectedDS.Spec.Template.Spec.Tolerations = test.expectedDSTolerations
					// Clear out object meta as it contains system set fields.
					expectedDS.ObjectMeta = metav1.ObjectMeta{}
					actualDS.ObjectMeta = metav1.ObjectMeta{}

					if !compareCSIDaemonset(expectedDS, &actualDS) {
						t.Errorf("Expected DS: %+v, actual DS: %+v", expectedDS, &actualDS)
					}
				}
			} else if !errors.IsNotFound(err) {
				t.Errorf("expected no daemonset, but found %s: %v", actualDS.GetName(), err)
			}
		})
	}
}

func TestConflict(t *testing.T) {
	k8sClient, kubeClient, cleanup := createClient(t)
	defer cleanup()

	const csiNamespace = "driver"
	ctx := context.Background()

	rec := &reconciler{
		scheme:    scheme.Scheme,
		Client:    kubeClient,
		k8sClient: k8sClient,
		gvr:       CPCGVR,
		csiConfig: csiconfig{
			csiNamespace: csiNamespace,
		},
	}

	if err := rec.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: csiNamespace,
		},
	}); err != nil {
		t.Fatalf("can't create namespace: %v", err)
	}

	type testPod struct {
		name string
		node string
	}
	type testNode struct {
		name   string
		labels map[string]string
	}
	for _, tc := range []struct {
		name           string
		pods           []testPod
		nodes          []testNode
		cpc            checkpoint.CheckpointConfiguration
		expectErr      bool
		expectConflict bool
	}{
		{
			name: "emtpy cluster",
			cpc: checkpoint.CheckpointConfiguration{
				Spec: checkpoint.CheckpointConfigurationSpec{
					NodeSelector: map[string]string{"job": "true"},
				},
			},
		},
		{
			name: "one job",
			nodes: []testNode{
				{
					name:   "n1",
					labels: map[string]string{"job": "true"},
				},
				{
					name:   "n2",
					labels: map[string]string{"job": "true"},
				},
			},
			cpc: checkpoint.CheckpointConfiguration{
				Spec: checkpoint.CheckpointConfigurationSpec{
					NodeSelector: map[string]string{"job": "true"},
				},
			},
		},
		{
			name: "two jobs, non-overlapping",
			pods: []testPod{
				{
					name: fmt.Sprintf("%s-1", csiPrefix),
					node: "n1",
				},
				{
					name: fmt.Sprintf("%s-2", csiPrefix),
					node: "n2",
				},
			},
			nodes: []testNode{
				{
					name:   "n1",
					labels: map[string]string{"job": "1"},
				},
				{
					name:   "n2",
					labels: map[string]string{"job": "1"},
				},
				{
					name:   "n3",
					labels: map[string]string{"job": "2"},
				},
				{
					name:   "n4",
					labels: map[string]string{"job": "2"},
				},
			},
			cpc: checkpoint.CheckpointConfiguration{
				Spec: checkpoint.CheckpointConfigurationSpec{
					NodeSelector: map[string]string{"job": "2"},
				},
			},
		},
		{
			name: "two jobs, overlapping",
			pods: []testPod{
				{
					name: fmt.Sprintf("%s-1", csiPrefix),
					node: "n1",
				},
				{
					name: fmt.Sprintf("%s-2", csiPrefix),
					node: "n2",
				},
			},
			nodes: []testNode{
				{
					name:   "n1",
					labels: map[string]string{"job": "1"},
				},
				{
					name:   "n2",
					labels: map[string]string{"job": "2"},
				},
				{
					name:   "n3",
					labels: map[string]string{"job": "2"},
				},
				{
					name:   "n4",
					labels: map[string]string{"job": "2"},
				},
			},
			expectConflict: true,
		},
		{
			name: "two jobs, overlapping non-driver",
			pods: []testPod{
				{
					name: fmt.Sprintf("%s-1", csiPrefix),
					node: "n1",
				},
				{
					name: "p2",
					node: "n2",
				},
			},
			nodes: []testNode{
				{
					name:   "n1",
					labels: map[string]string{"job": "1"},
				},
				{
					name:   "n2",
					labels: map[string]string{"job": "2"},
				},
				{
					name:   "n3",
					labels: map[string]string{"job": "2"},
				},
				{
					name:   "n4",
					labels: map[string]string{"job": "2"},
				},
			},
			cpc: checkpoint.CheckpointConfiguration{
				Spec: checkpoint.CheckpointConfigurationSpec{
					NodeSelector: map[string]string{"job": "2"},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, p := range tc.pods {
				pod := corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      p.name,
						Namespace: csiNamespace,
					},
					Spec: corev1.PodSpec{
						NodeName: p.node,
						Containers: []corev1.Container{
							{
								Name:  "pod",
								Image: "debian",
							},
						},
					},
				}
				err := rec.Create(ctx, &pod)
				if err != nil {
					t.Fatalf("can't create %v: %v", p, err)
				}
			}
			for _, n := range tc.nodes {
				node := corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   n.name,
						Labels: n.labels,
					},
				}
				err := rec.Create(ctx, &node)
				if err != nil {
					t.Fatalf("can't create %v: %v", n, err)
				}
			}
			defer func() {
				if err := rec.DeleteAllOf(ctx, &corev1.Pod{}, client.DeleteAllOfOption(client.InNamespace(csiNamespace)), client.GracePeriodSeconds(0)); err != nil {
					t.Fatalf("can't delete pods for cleanup: %v", err)
				}
				if err := rec.DeleteAllOf(ctx, &corev1.Node{}, client.GracePeriodSeconds(0)); err != nil {
					t.Fatalf("can't delete nodes for cleanup: %v", err)
				}
			}()
			conflict, err := rec.checkDaemonsetConflict(ctx, &tc.cpc)
			if tc.expectErr != (err != nil) {
				t.Errorf("unexpected error for %s: %s", tc.name, err)
			}
			if tc.expectConflict != conflict {
				t.Errorf("unexpected conflict for %s: %t", tc.name, conflict)
			}
		})
	}
}

func TestDeletion(t *testing.T) {
	k8sClient, kubeClient, cleanup := createClient(t)
	defer cleanup()

	const csiNamespace = "controller"
	ctx := context.Background()

	rec := &reconciler{
		scheme:    scheme.Scheme,
		Client:    kubeClient,
		k8sClient: k8sClient,
		gvr:       CPCGVR,
		csiConfig: csiconfig{
			csiNamespace: csiNamespace,
		},
	}

	if err := rec.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: csiNamespace,
		},
	}); err != nil {
		t.Fatalf("can't create namespace: %v", err)
	}

	// Note cpc doesn't need to be created as handleDeletedCheckpointConfiguration doesn't fetch the
	// resource.
	cpc := checkpoint.CheckpointConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cpc",
		},
	}

	type mapInfo struct {
		name           string
		annotations    map[string]string
		expectDeletion bool
	}
	for _, tc := range []struct {
		name          string
		createDS      bool
		maps          []mapInfo
		expectRequeue bool
	}{
		{
			name:          "no maps",
			createDS:      true,
			expectRequeue: true,
		},
		{
			name:     "no ds",
			createDS: false,
			maps: []mapInfo{
				{
					name: "unrelated",
				},
				{
					name: "matching",
					annotations: map[string]string{
						util.CpcAnnotation: "cpc",
					},
					expectDeletion: true,
				},
			},
			expectRequeue: false,
		},
		{
			name:     "ds",
			createDS: true,
			maps: []mapInfo{
				{
					name: "unrelated",
				},
				{
					name: "matching",
					annotations: map[string]string{
						util.CpcAnnotation: "cpc",
					},
					expectDeletion: true,
				},
			},
			expectRequeue: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ds *appsv1.DaemonSet
			if tc.createDS {
				ds = &appsv1.DaemonSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ds",
						Namespace: csiNamespace,
					},
					Spec: appsv1.DaemonSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"app": "driver",
							},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									"app": "driver",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "driver",
										Image: "driver",
									},
								},
							},
						},
					},
				}
				err := rec.Create(ctx, ds)
				if err != nil {
					t.Fatalf("can't create daemonset: %v", err)
				}
			}
			expectedMaps := map[string]bool{}
			for _, m := range tc.maps {
				cm := corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:        m.name,
						Namespace:   csiNamespace,
						Annotations: m.annotations,
					},
				}
				err := rec.Create(ctx, &cm)
				if err != nil {
					t.Fatalf("can't create configmap %s: %v", m.name, err)
				}
				if !m.expectDeletion {
					expectedMaps[m.name] = true
				}
			}
			defer func() {
				if err := rec.DeleteAllOf(ctx, &corev1.ConfigMap{}, client.DeleteAllOfOption(client.InNamespace(csiNamespace)), client.GracePeriodSeconds(0)); err != nil {
					t.Fatalf("can't delete configmaps for cleanup: %v", err)
				}
				if err := rec.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.DeleteAllOfOption(client.InNamespace(csiNamespace)), client.GracePeriodSeconds(0)); err != nil {
					t.Fatalf("can't delete daemonsets for cleanup: %v", err)
				}
			}()

			requeue, err := rec.handleDeletedCheckpointConfiguration(ctx, &cpc, ds)
			if err != nil {
				t.Errorf("Unexpected error in %s: %v", tc.name, err)
			}
			if requeue != tc.expectRequeue {
				t.Errorf("Unexpected requeue %t in %s", requeue, tc.name)
			}
			ds = &appsv1.DaemonSet{}
			err = rec.Get(ctx, types.NamespacedName{Name: "ds", Namespace: csiNamespace}, ds)
			if err != nil && !errors.IsNotFound(err) {
				t.Errorf("Unexpected error confirming ds deletion: %v", err)
			} else if err == nil {
				// The daemonset was found.
				if ds.GetDeletionTimestamp() != nil {
					t.Errorf("Expected daemonset to be deleted, but it wasn't")
				}
			}
			var configMaps corev1.ConfigMapList
			err = rec.List(ctx, &configMaps, client.InNamespace(rec.csiConfig.csiNamespace))
			if err != nil {
				t.Errorf("Unexpected error getting configmaps: %v", err)
			}
			for _, configMap := range configMaps.Items {
				if configMap.GetDeletionTimestamp() != nil {
					continue
				}
				_, found := expectedMaps[configMap.GetName()]
				if !found {
					t.Errorf("Expected %s to be deleted, but it wasn't", configMap.GetName())
				}
				delete(expectedMaps, configMap.GetName())
			}
			if len(expectedMaps) > 0 {
				t.Errorf("Expected undeleted maps not found: %v", expectedMaps)
			}
		})
	}
}

func cpc(gcsBucketName, finalizer, inMemoryVolumeSize string, nodeSelector map[string]string, tolerations []corev1.Toleration) *checkpoint.CheckpointConfiguration {
	cpc := checkpoint.CheckpointConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCPCName,
			Namespace: testCPCNamespace,
		},
		Spec: checkpoint.CheckpointConfigurationSpec{
			CloudStorageBucketName: gcsBucketName,
			NodeSelector:           nodeSelector,
			Tolerations:            tolerations,
			InMemoryVolumeSize:     inMemoryVolumeSize,
		},
	}
	if finalizer != "" {
		cpc.ObjectMeta.Finalizers = []string{}
		cpc.ObjectMeta.Finalizers = append(cpc.ObjectMeta.Finalizers, finalizer)
	}

	return &cpc

}

func compareCPC(expect, actual *checkpoint.CheckpointConfiguration) bool {
	if expect == nil {
		return actual == nil
	}
	if actual == nil {
		return expect == nil
	}

	if expect.Name == "" {
		return actual.Name == ""
	}

	if expect.Name != actual.Name {
		return false
	}

	if expect.Namespace != actual.Namespace {
		return false
	}

	if !reflect.DeepEqual(expect.Finalizers, actual.Finalizers) {
		return false
	}

	return true
}

func compareCSIDaemonset(expect, actual *appsv1.DaemonSet) bool {
	if expect == nil {
		return actual == nil
	}
	if actual == nil {
		return expect == nil
	}

	if expect.Name == "" {
		return actual.Name == ""
	}

	if expect.Name != actual.Name {
		return false
	}

	if expect.Namespace != actual.Namespace {
		return false
	}

	if !reflect.DeepEqual(expect.Spec.Template.Spec.NodeSelector, actual.Spec.Template.Spec.NodeSelector) {
		return false
	}

	if !reflect.DeepEqual(expect.Spec.Template.Spec.Tolerations, actual.Spec.Template.Spec.Tolerations) {
		return false
	}

	var expectVolume *corev1.Volume
	for _, v := range expect.Spec.Template.Spec.Volumes {
		if strings.TrimSpace(v.Name) == gcsVolumeName {
			expectVolume = &v
			break
		}
	}
	var actualVolume *corev1.Volume
	for _, v := range expect.Spec.Template.Spec.Volumes {
		if strings.TrimSpace(v.Name) == gcsVolumeName {
			actualVolume = &v
			break
		}
	}

	if expectVolume.CSI.VolumeAttributes["bucketName"] != actualVolume.CSI.VolumeAttributes["bucketName"] {
		return false
	}

	return true
}

func compareResult(expect, actual *ctrl.Result) bool {
	if expect == nil {
		return actual == nil
	}
	if actual == nil {
		return expect == nil
	}

	return expect.Requeue == actual.Requeue
}

func compareErrors(expect, actual error) bool {
	if expect == nil {
		return actual == nil
	}
	if actual == nil {
		return expect == nil
	}
	return expect.Error() == actual.Error()
}
