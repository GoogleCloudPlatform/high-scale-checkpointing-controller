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

package deploy_test

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	checkpointv1 "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/apis/checkpointing.gke.io/v1"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	// multitierNamespace is from deploy/multitier/controller/kustomize.yaml
	multitierNamespace     = "gke-managed-checkpointing"
	multitierControllerDir = "multitier/controller"
	multitierCRDDir        = "multitier/crd"

	// cpcName is from deploy/multitier/crd/
	cpcName      = "deploy-test"
	cpcNamespace = "default"
)

var (
	extractResponse = regexp.MustCompile(`RESPONSE: (.*)`)
)

// checkMultitierCluster returns a list of MTC driver pods.
func checkMultitierCluster(ctx context.Context, t *testing.T, numPods int) []corev1.Pod {
	t.Helper()

	var podList []corev1.Pod
	// Wait 5 minutes as Pod startup might be slow if AR is unkind to us.
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods, err := K8sClient.CoreV1().Pods(multitierNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, pod := range pods.Items {
			if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
				return false, nil // retry
			}
		}
		podList = []corev1.Pod{}
		for _, pod := range pods.Items {
			if strings.Contains(pod.Name, "multitier-driver") {
				podList = append(podList, pod)
			}
		}
		if len(pods.Items) < numPods {
			return false, nil // retry
		}

		return true, nil
	}); err != nil {
		t.Fatalf("not enough multitier pods started: %v", err)
	}
	return podList
}

func runAPIOnPod(ctx context.Context, t *testing.T, pod *corev1.Pod, cmd string, args ...string) (string, error) {
	output, err := runOnPod(ctx, t, pod, "replication-worker", "multitier_stub", slices.Concat([]string{"--cmd", cmd}, args)...)
	t.Logf("Running %s on %s got %s", cmd, pod.GetName(), output)
	if err != nil {
		return "", err
	}
	matches := extractResponse.FindStringSubmatch(output)
	if len(matches) > 1 {
		return matches[1], nil
	}
	return "", nil
}

func createMultitierJobset(ctx context.Context, t *testing.T, name string, selector map[string]string, numSlices, sliceSize int) *jobsetv1alpha.JobSet {
	t.Helper()
	jobset := jobsetv1alpha.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Annotations: map[string]string{
				"alpha.jobset.sigs.k8s.io/exclusive-topology": "cloud.google.com/gke-nodepool",
			},
		},
		Spec: jobsetv1alpha.JobSetSpec{
			FailurePolicy: &jobsetv1alpha.FailurePolicy{
				MaxRestarts: 20,
			},
			ReplicatedJobs: []jobsetv1alpha.ReplicatedJob{
				{
					Name:     "slice",
					Replicas: int32(numSlices),
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Parallelism:  ptr.To(int32(sliceSize)),
							Completions:  ptr.To(int32(sliceSize)),
							BackoffLimit: ptr.To(int32(0)),
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"job": name,
									},
								},
								Spec: corev1.PodSpec{
									TerminationGracePeriodSeconds: ptr.To(int64(1)),
									NodeSelector:                  selector,
									Affinity: &corev1.Affinity{
										PodAntiAffinity: &corev1.PodAntiAffinity{
											RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
												{
													LabelSelector: &metav1.LabelSelector{
														MatchLabels: map[string]string{
															"job": name,
														},
													},
													TopologyKey: "kubernetes.io/hostname",
												},
											},
										},
									},
									Tolerations: []corev1.Toleration{
										{
											Key:      "checkpoint",
											Operator: corev1.TolerationOpExists,
											Effect:   corev1.TaintEffectNoSchedule,
										},
									},
									Containers: []corev1.Container{
										{
											Name:    "worker",
											Image:   "debian",
											Command: []string{"sleep", "3600"},
											VolumeMounts: []corev1.VolumeMount{
												{
													MountPath: "/local",
													Name:      "checkpoint",
												},
											},
										},
									},
									Volumes: []corev1.Volume{
										{
											Name: "checkpoint",
											VolumeSource: corev1.VolumeSource{
												CSI: &corev1.CSIVolumeSource{
													Driver: "multitier-checkpoint.csi.storage.gke.io",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := CRClient.Create(ctx, &jobset)
	assert.NilError(t, err, "cannot create jobset %s", name)
	t.Logf("created jobset %s/%s", jobset.GetNamespace(), jobset.GetName())
	return &jobset
}

func getMultitierSelector(ctx context.Context, t *testing.T) map[string]string {
	var cpc checkpointv1.CheckpointConfiguration
	err := CRClient.Get(ctx, types.NamespacedName{Namespace: cpcNamespace, Name: cpcName}, &cpc)
	assert.NilError(t, err)
	return cpc.Spec.NodeSelector
}

func createMultitierScaleJobsetExtended(ctx context.Context, t *testing.T, name string, selector map[string]string, numSlices, sliceSize int, finishImmediately bool) *jobsetv1alpha.JobSet {
	t.Helper()
	scaleTestParams := getScaleTestParams(t)
	if scaleTestParams == nil {
		t.Fatalf("no scale test parameters when creating multitier scale test jobset")
	}
	_, err := K8sClient.RbacV1().Roles(testNamespace).Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name: "configmap-writer",
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "list", "update", "delete", "create"},
			},
		},
	}, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		assert.NilError(t, err)
	}
	_, err = K8sClient.RbacV1().RoleBindings(testNamespace).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "configmap-writer",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "default",
				Namespace: testNamespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "Role",
			Name:     "configmap-writer",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		assert.NilError(t, err)
	}

	jobset := jobsetv1alpha.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Annotations: map[string]string{
				"alpha.jobset.sigs.k8s.io/exclusive-topology": "cloud.google.com/gke-nodepool",
			},
		},
		Spec: jobsetv1alpha.JobSetSpec{
			FailurePolicy: &jobsetv1alpha.FailurePolicy{
				MaxRestarts: 20,
			},
			ReplicatedJobs: []jobsetv1alpha.ReplicatedJob{
				{
					Name:     "slice",
					Replicas: int32(numSlices),
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Parallelism:  ptr.To(int32(sliceSize)),
							Completions:  ptr.To(int32(sliceSize)),
							BackoffLimit: ptr.To(int32(0)),
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"job": name,
									},
								},
								Spec: corev1.PodSpec{
									TerminationGracePeriodSeconds: ptr.To(int64(1)),
									NodeSelector:                  selector,
									Affinity: &corev1.Affinity{
										PodAntiAffinity: &corev1.PodAntiAffinity{
											RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
												{
													LabelSelector: &metav1.LabelSelector{
														MatchLabels: map[string]string{
															"job": name,
														},
													},
													TopologyKey: "kubernetes.io/hostname",
												},
											},
										},
									},
									Tolerations: []corev1.Toleration{
										{
											Key:      "checkpoint",
											Operator: corev1.TolerationOpExists,
											Effect:   corev1.TaintEffectNoSchedule,
										},
									},
									Containers: []corev1.Container{
										{
											Name:  "worker",
											Image: scaleTestParams.workerImage,
											Args: []string{
												"--info-file=/local/jax-init-info.txt",
												"--done-file=/control/$(POD)",
												"--fail-file=/control/fail",
												"--namespace=$(NAMESPACE)",
												"--pod-name=$(POD)",
												"--ip=$(IP)",
												"--node=$(NODE)",
											},
											Env: []corev1.EnvVar{
												{
													Name: "NAMESPACE",
													ValueFrom: &corev1.EnvVarSource{
														FieldRef: &corev1.ObjectFieldSelector{
															FieldPath: "metadata.namespace",
														},
													},
												},
												{
													Name: "POD",
													ValueFrom: &corev1.EnvVarSource{
														FieldRef: &corev1.ObjectFieldSelector{
															FieldPath: "metadata.name",
														},
													},
												},
												{
													Name: "IP",
													ValueFrom: &corev1.EnvVarSource{
														FieldRef: &corev1.ObjectFieldSelector{
															FieldPath: "status.podIP",
														},
													},
												},
												{
													Name: "NODE",
													ValueFrom: &corev1.EnvVarSource{
														FieldRef: &corev1.ObjectFieldSelector{
															FieldPath: "spec.nodeName",
														},
													},
												},
											},

											VolumeMounts: []corev1.VolumeMount{
												{
													MountPath: "/local",
													Name:      "checkpoint",
												},
												{
													MountPath: "/control",
													Name:      "control",
												},
											},
										},
									},
									Volumes: []corev1.Volume{
										{
											Name: "checkpoint",
											VolumeSource: corev1.VolumeSource{
												CSI: &corev1.CSIVolumeSource{
													Driver: "multitier-checkpoint.csi.storage.gke.io",
												},
											},
										},
										{
											Name: "control",
											VolumeSource: corev1.VolumeSource{
												EmptyDir: &corev1.EmptyDirVolumeSource{
													Medium: corev1.StorageMediumMemory,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if finishImmediately {
		c := &jobset.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0]
		c.Args = append(c.Args, "--finish-immediately")
	}

	err = CRClient.Create(ctx, &jobset)
	assert.NilError(t, err, "cannot create jobset %s", name)
	t.Logf("created jobset %s/%s", jobset.GetNamespace(), jobset.GetName())
	return &jobset
}

func createMultitierScaleJobset(ctx context.Context, t *testing.T, name string, selector map[string]string, numSlices, sliceSize int) *jobsetv1alpha.JobSet {
	return createMultitierScaleJobsetExtended(ctx, t, name, selector, numSlices, sliceSize, false)
}

func createMultitierScaleJobsetImmediateFinish(ctx context.Context, t *testing.T, name string, selector map[string]string, numSlices, sliceSize int) *jobsetv1alpha.JobSet {
	return createMultitierScaleJobsetExtended(ctx, t, name, selector, numSlices, sliceSize, true)
}

func createEphemeralJob(ctx context.Context, t *testing.T, name string, selector map[string]string, size int) *jobsetv1alpha.JobSet {
	t.Helper()
	jobset := jobsetv1alpha.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: jobsetv1alpha.JobSetSpec{
			FailurePolicy: &jobsetv1alpha.FailurePolicy{
				MaxRestarts: 20,
			},
			ReplicatedJobs: []jobsetv1alpha.ReplicatedJob{
				{
					Name:     "slice",
					Replicas: int32(1),
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Parallelism:  ptr.To(int32(size)),
							Completions:  ptr.To(int32(size)),
							BackoffLimit: ptr.To(int32(0)),
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"job": name,
									},
								},
								Spec: corev1.PodSpec{
									RestartPolicy:                 corev1.RestartPolicyNever,
									TerminationGracePeriodSeconds: ptr.To(int64(1)),
									NodeSelector:                  selector,
									Tolerations: []corev1.Toleration{
										{
											Key:      "checkpoint",
											Operator: corev1.TolerationOpExists,
											Effect:   corev1.TaintEffectNoSchedule,
										},
									},
									Containers: []corev1.Container{
										{
											Name:    "worker",
											Image:   "debian",
											Command: []string{"false"},
											Resources: corev1.ResourceRequirements{
												Requests: map[corev1.ResourceName]resource.Quantity{
													corev1.ResourceCPU:              resource.MustParse("1m"),
													corev1.ResourceMemory:           resource.MustParse("2Mi"),
													corev1.ResourceEphemeralStorage: resource.MustParse("1Mi"),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := CRClient.Create(ctx, &jobset)
	assert.NilError(t, err, "cannot create jobset %s", name)
	t.Logf("created jobset %s/%s", jobset.GetNamespace(), jobset.GetName())
	return &jobset
}

func getCheckpointConfiguration(t *testing.T) *checkpointv1.CheckpointConfiguration {
	bucket := os.Getenv("MULTITIER_TEST_GCS_BUCKET")
	if bucket == "" {
		t.Fatalf("MULTITIER_TEST_GCS_BUCKET needs to be defined as a bucket to be used for testing. Just the bucket name, eg my-test-bucket and not gs://my-test-bucket")
	}

	return &checkpointv1.CheckpointConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deploy-test",
		},
		Spec: checkpointv1.CheckpointConfigurationSpec{
			CloudStorageBucketName: bucket,
			NodeSelector: map[string]string{
				sliceLabel: sliceValue,
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      sliceTaint,
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			InMemoryVolumeSize: "10",
			// The CsiEphemeralLimit needs to be small in order to fit
			// into the small boot disks of our nodes.
			CsiEphemeralLimit: "50Mi",
		},
	}
}

func applyCheckpointConfiguration(ctx context.Context, t *testing.T, cpc *checkpointv1.CheckpointConfiguration) {
	t.Log("deploying checkpointconfiguration/deploy-test")
	err := CRClient.Create(ctx, cpc)
	if !apierrors.IsAlreadyExists(err) {
		assert.NilError(t, err, "cannot create checkpoint configuration")
	}
}

func deleteCheckpointConfigurations(ctx context.Context, t *testing.T) {
	t.Log("deleting all checkpoint configurations")
	err := CRClient.DeleteAllOf(ctx, &checkpointv1.CheckpointConfiguration{})
	assert.NilError(t, err)
	t.Log("waiting for checkpoint configurations to be garbaged collected")
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		var cpcs checkpointv1.CheckpointConfigurationList
		err := CRClient.List(ctx, &cpcs)
		return err == nil && len(cpcs.Items) == 0, nil
	})
}

// MultitierOptions affects deployMultitier. The only option now is for the
// CheckpointConfiguration; more interface methods will need to be added
// if we have more options.
type MultitierOptions interface {
	updateCheckpointConfiguration(*checkpointv1.CheckpointConfiguration)
}

type gcsMountOptions []string

func (opts gcsMountOptions) updateCheckpointConfiguration(cpc *checkpointv1.CheckpointConfiguration) {
	cpc.Spec.GcsFuseMountOptions = []string(opts)
}

// deployMultitier installs everything required for the multitier controller, and
// returns a cleanup func that should be deferred by the caller.
func deployMultitier(ctx context.Context, t *testing.T, opts ...MultitierOptions) func() {
	t.Helper()

	cleanupFns := []func(){}
	runCleanupFns := func() {
		for i := len(cleanupFns) - 1; i >= 0; i-- {
			cleanupFns[i]()
		}
	}
	normalExit := false
	defer func() {
		if !normalExit {
			runCleanupFns()
		}
	}()

	deployCRD(ctx, t)
	cleanupFns = append(cleanupFns, func() { undeployCRD(ctx, t) })

	deployDir(ctx, t, multitierControllerDir)
	cleanupFns = append(cleanupFns, func() { undeployDir(ctx, t, multitierControllerDir) })

	cpc := getCheckpointConfiguration(t)
	for _, opt := range opts {
		opt.updateCheckpointConfiguration(cpc)
	}

	applyCheckpointConfiguration(ctx, t, cpc)
	cleanupFns = append(cleanupFns, func() { deleteCheckpointConfigurations(ctx, t) })

	namespaceCleanup := testNamespaceSetup(ctx, t)
	cleanupFns = append(cleanupFns, namespaceCleanup)

	normalExit = true
	return runCleanupFns
}

// perturbNodeMap takes a map of node -> rank index, and returns a list of nodes in rank
// order that does match, but is still consistent with slices. For example, if there are
// two workers per slice, and the original map is A -> (0,0), B -> (0,1), C-> (1,0), etc,
// then we'll return D,C,B,A,E,F (the first two slices are shifted and swapped, the rest
// are unchanged).
func perturbNodeMap(t *testing.T, nodeMap map[string]int, sliceSize int) []string {
	if len(nodeMap) < 2 {
		t.Fatalf("need two or more nodes in %v", nodeMap)
	}
	numSlices := len(nodeMap) / sliceSize
	slices := make([][]string, numSlices)
	for i := range numSlices {
		slices[i] = make([]string, sliceSize)
	}
	for node, idx := range nodeMap {
		if idx < 0 || idx >= len(nodeMap) {
			t.Fatalf("bad index for %s: %d", node, idx)
		}
		slices[idx/sliceSize][idx%sliceSize] = node
	}
	if len(slices) == 1 {
		return append(slices[0][1:], slices[0][0])
	}
	perturbed := []string{}
	perturbed = append(perturbed, append(slices[1][1:], slices[1][0])...)
	perturbed = append(perturbed, append(slices[0][1:], slices[0][0])...)
	for i := range numSlices - 2 {
		slice := i + 2
		perturbed = append(perturbed, append(slices[slice][1:], slices[slice][0])...)
	}
	return perturbed
}

// forceScheduling deploys a webhook to force a jobset to schedule to nodes in order of their rank.
func forceScheduling(ctx context.Context, t *testing.T, jobset string, nodes []string, sliceSize int) {
	t.Log("creating webhook to force scheduling")
	out, err := util.RunCommand("./data/webhook_util.sh", "--jobset-base", jobset+"-slice", "--target-namespace", testNamespace, "--slice-size", strconv.Itoa(sliceSize), "--target-nodes", strings.Join(nodes, ","))
	assert.NilError(t, err, "webhook creation failed")
	t.Log(string(out))
}

// relaxScheduling removes any running webhook for forceScheduling.
func relaxScheduling(ctx context.Context, t *testing.T) {
	t.Log("removing force scheduling webhook")
	out, err := util.RunCommand("./data/webhook_util.sh", "--purge")
	assert.NilError(t, err, "webhook shutdown failed")
	t.Log(string(out))
}

func TestMultitierCoordinator(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 1, 4)
	testPods := checkMultitierCluster(ctx, t, 4)

	t.Log("checking registration of multiple jobs")
	_, err := runAPIOnPod(ctx, t, &testPods[0], "register-coordinator", "--job", "j1", "--ip", "pod1")
	assert.NilError(t, err)
	_, err = runAPIOnPod(ctx, t, &testPods[0], "register-coordinator", "--job", "j2", "--ip", "pod2")
	assert.NilError(t, err)
	coord, err := runAPIOnPod(ctx, t, &testPods[1], "get-coordinator", "--job", "j1")
	assert.NilError(t, err)
	assert.Equal(t, coord, "pod1")
	coord, err = runAPIOnPod(ctx, t, &testPods[2], "get-coordinator", "--job", "j2")
	assert.NilError(t, err)
	assert.Equal(t, coord, "pod2")

	t.Log("checking unregistration")
	_, err = runAPIOnPod(ctx, t, &testPods[1], "unregister-coordinator", "--job", "j1", "--ip", "pod1")
	assert.NilError(t, err)
	_, err = runAPIOnPod(ctx, t, &testPods[1], "unregister-coordinator", "--job", "j2", "--ip", "unknown")
	assert.NilError(t, err)

	t.Log("checking unknown registration")
	_, err = runAPIOnPod(ctx, t, &testPods[1], "get-coordinator", "--job", "j1")
	assert.ErrorContains(t, err, "rpc error")

	t.Log("checking continuing registration")
	coord, err = runAPIOnPod(ctx, t, &testPods[1], "get-coordinator", "--job", "j2")
	assert.NilError(t, err)
	assert.Equal(t, coord, "pod2")

	t.Log("checking updated registration")
	_, err = runAPIOnPod(ctx, t, &testPods[0], "register-coordinator", "--job", "j1", "--ip", "updated-pod")
	assert.NilError(t, err)
	coord, err = runAPIOnPod(ctx, t, &testPods[2], "get-coordinator", "--job", "j1")
	assert.NilError(t, err)
	assert.Equal(t, coord, "updated-pod")

	t.Log("checking coordinator configmap deleted with checkpoint configurations")
	_, err = K8sClient.CoreV1().ConfigMaps(multitierNamespace).Get(ctx, "j1", metav1.GetOptions{})
	assert.NilError(t, err)
	deleteCheckpointConfigurations(ctx, t)
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		_, err := K8sClient.CoreV1().ConfigMaps(multitierNamespace).Get(ctx, "j1", metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
}

func TestMultitierRamdisk(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 1, 4)
	testPods := checkMultitierCluster(ctx, t, 4)

	client := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "client",
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: ptr.To(int64(1)),
			NodeSelector:                  getMultitierSelector(ctx, t),
			Tolerations: []corev1.Toleration{
				{
					Key:      "checkpoint",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			Containers: []corev1.Container{
				{
					Name:    "main",
					Image:   "debian",
					Command: []string{"sleep", "900"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "checkpoint",
							MountPath: "/checkpoint",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "checkpoint",
					VolumeSource: corev1.VolumeSource{
						CSI: &corev1.CSIVolumeSource{
							Driver: "multitier-checkpoint.csi.storage.gke.io",
						},
					},
				},
			},
		},
	}
	client, err := K8sClient.CoreV1().Pods(testNamespace).Create(ctx, client, metav1.CreateOptions{})
	assert.NilError(t, err)
	client, err = waitForPodRunning(ctx, client)
	assert.NilError(t, err)
	targetNode := client.Spec.NodeName
	targetDriver := -1
	for i, pod := range testPods {
		if pod.Spec.NodeName == targetNode {
			targetDriver = i
			break
		}
	}
	assert.Assert(t, targetDriver >= 0)

	t.Log("checking local ramdisk")
	_, err = runOnPod(ctx, t, client, "main", "touch", "/checkpoint/mark")
	assert.NilError(t, err)
	out, err := runOnPod(ctx, t, &testPods[targetDriver], "replication-worker", "bash", "-c", "ls /app/local/client/*")
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(out, "/app/local/client/mark"), out)

	t.Log("checking ramdisk after restart")
	err = K8sClient.CoreV1().Pods(testNamespace).Delete(ctx, client.GetName(), metav1.DeleteOptions{})
	assert.NilError(t, err)
	client.SetName("client2")
	client.ObjectMeta.ResourceVersion = ""
	client.Spec.NodeName = targetNode
	client.Status = corev1.PodStatus{} // This probably doesn't matter but better safe then sorry.
	client, err = K8sClient.CoreV1().Pods(testNamespace).Create(ctx, client, metav1.CreateOptions{})
	assert.NilError(t, err)
	client, err = waitForPodRunning(ctx, client)
	assert.NilError(t, err)
	out, _ = runOnPod(ctx, t, client, "main", "bash", "-c", "ls /checkpoint/*")
	assert.Assert(t, strings.Contains(out, "/checkpoint/mark"), out)
}

func TestMultitierPeer(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 1, 4)
	testPods := checkMultitierCluster(ctx, t, 4)

	_, err := runAPIOnPod(ctx, t, &testPods[0], "set-peer", "--mountpoint", "m1", "--ip", testPods[2].Status.PodIP)
	assert.NilError(t, err)
	_, err = runAPIOnPod(ctx, t, &testPods[1], "set-peer", "--mountpoint", "m1", "--ip", testPods[2].Status.PodIP)
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[0], "replication-worker", "touch", "/app/repl/m1/mark")
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[1], "replication-worker", "ls", "/app/repl/m1/mark")
	assert.NilError(t, err)

	_, err = runAPIOnPod(ctx, t, &testPods[0], "unmount", "--mountpoint", "m1")
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[0], "replication-worker", "ls", "/app/repl/m1")
	assert.ErrorContains(t, err, "No such file or directory")

	_, err = runAPIOnPod(ctx, t, &testPods[0], "set-peer", "--mountpoint", "m1", "--ip", testPods[2].Status.PodIP)
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[0], "replication-worker", "ls", "/app/repl/m1/mark")
	assert.NilError(t, err)

	// test umountAll for one of the pods.
	_, err = runAPIOnPod(ctx, t, &testPods[0], "unmount-all")
	assert.NilError(t, err)
}

func TestMultitierGCSBucket(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 1, 4)
	testPods := checkMultitierCluster(ctx, t, 4)

	defer func() {
		_, err := runOnPod(ctx, t, &testPods[0], "csi", "rm", "-rf", "/persistent/m1")
		if err != nil {
			t.Logf("warning: ignored error during teardown: %v", err)
		}
		_, err = runOnPod(ctx, t, &testPods[0], "csi", "rm", "-rf", "/persistent/m2")
		if err != nil {
			t.Logf("warning: ignored error during teardown: %v", err)
		}
	}()

	mark := fmt.Sprintf("mark-%d", time.Now().Unix())
	mark2 := fmt.Sprintf("mark2-%d", time.Now().Unix())

	errors := make(chan error)
	for _, pod := range testPods {
		go func() {
			_, err := runAPIOnPod(ctx, t, &pod, "mount-gcs", "--mountpoint", "m1")
			errors <- err
		}()
	}

	for range testPods {
		assert.NilError(t, <-errors)
	}

	_, err := runOnPod(ctx, t, &testPods[0], "replication-worker", "touch", fmt.Sprintf("/app/backup/gcs/%s", mark))
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[1], "replication-worker", "ls", fmt.Sprintf("/app/backup/gcs/%s", mark))
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[1], "csi", "ls", fmt.Sprintf("/persistent/m1/%s", mark))
	assert.NilError(t, err)

	for _, pod := range testPods {
		go func() {
			_, err := runAPIOnPod(ctx, t, &pod, "mount-gcs", "--mountpoint", "m2")
			errors <- err
		}()
	}

	for range testPods {
		assert.NilError(t, <-errors)
	}

	_, err = runOnPod(ctx, t, &testPods[0], "replication-worker", "touch", fmt.Sprintf("/app/backup/gcs/%s", mark2))
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[1], "replication-worker", "ls", fmt.Sprintf("/app/backup/gcs/%s", mark2))
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[1], "csi", "ls", fmt.Sprintf("/persistent/m2/%s", mark2))
	assert.NilError(t, err)

	_, err = runAPIOnPod(ctx, t, &testPods[0], "mount-gcs", "--mountpoint", "m1")
	assert.NilError(t, err)
	_, err = runOnPod(ctx, t, &testPods[0], "replication-worker", "ls", fmt.Sprintf("/app/backup/gcs/%s", mark))
	assert.NilError(t, err)

}

func TestMultitierGCSMountOptions(t *testing.T) {
	ctx := context.Background()

	expectedOpts := []string{
		"implicit-dirs",
		"metadata-cache:negative-ttl-secs:0",
		"metadata-cache:ttl-secs:-1",
	}

	cleanup := deployMultitier(ctx, t, gcsMountOptions(expectedOpts))
	defer cleanup()

	initializeTestSlices(ctx, t, 1, 4)
	testPods := checkMultitierCluster(ctx, t, 4)

	found := false
	for _, vol := range testPods[0].Spec.Volumes {
		if vol.VolumeSource.CSI == nil || vol.VolumeSource.CSI.Driver != "gcsfuse.csi.storage.gke.io" {
			continue
		}
		assert.Equal(t, vol.VolumeSource.CSI.VolumeAttributes["mountOptions"], strings.Join(expectedOpts, ","))
		found = true
		break
	}
	assert.Assert(t, found)
}

func TestMultitierAutoRecycle(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 1, 4)
	testPods := checkMultitierCluster(ctx, t, 4)

	_, err := runAPIOnPod(ctx, t, &testPods[0], "set-peer", "--mountpoint", "m1", "--ip", testPods[2].Status.PodIP)
	assert.NilError(t, err)
	_, err = runAPIOnPod(ctx, t, &testPods[1], "set-peer", "--mountpoint", "m1", "--ip", testPods[2].Status.PodIP)
	assert.NilError(t, err)

	for _, pod := range testPods {
		_, err := runAPIOnPod(ctx, t, &pod, "mount-gcs", "--mountpoint", "c1")
		assert.NilError(t, err)
	}
}

func TestMultitierCustomImage(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 1, 4)
	checkMultitierCluster(ctx, t, 4)

	imageMap, err := K8sClient.CoreV1().ConfigMaps(multitierNamespace).Get(ctx, "checkpoint-image-config", metav1.GetOptions{})
	assert.NilError(t, err)
	customMapName := imageMap.Data["custom-image-config-name"]
	assert.Assert(t, customMapName != "")

	customMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: customMapName,
		},
		Data: map[string]string{
			"replication-worker-image": "debian",
		},
	}
	customMap, err = K8sClient.CoreV1().ConfigMaps(multitierNamespace).Create(ctx, customMap, metav1.CreateOptions{})
	assert.NilError(t, err)

	deleteCheckpointConfigurations(ctx, t)
	applyCheckpointConfiguration(ctx, t, getCheckpointConfiguration(t))

	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		pods, err := K8sClient.CoreV1().Pods(multitierNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, pod := range pods.Items {
			if strings.HasPrefix(pod.GetName(), "multitier-driver") {
				for _, c := range pod.Spec.Containers {
					if c.Name == "replication-worker" {
						if c.Image == "debian" {
							t.Log("Found overridden replication-worker image!")
							return true, nil
						}
					}
				}
			}
		}
		return false, nil
	})
}

func TestMultitierConflictEvent(t *testing.T) {
	ctx := context.Background()
	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 3, 2)
	createMultitierJobset(ctx, t, "job", getMultitierSelector(ctx, t), 3, 2)

	cpc2 := checkpointv1.CheckpointConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cpc2",
		},
		Spec: checkpointv1.CheckpointConfigurationSpec{
			NodeSelector: map[string]string{
				sliceLabel: sliceValue,
			},
		},
	}
	applyCheckpointConfiguration(ctx, t, &cpc2)
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		ds, err := K8sClient.AppsV1().DaemonSets(multitierNamespace).List(ctx, metav1.ListOptions{})
		return err == nil && len(ds.Items) > 1, nil
	})
	if err == nil {
		t.Fatalf("expected timeout looking for second daemonset")
	}
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		selector := "involvedObject.name=cpc2,involvedObject.kind=CheckpointConfiguration"
		events, err := K8sClient.CoreV1().Events("default").List(ctx, metav1.ListOptions{FieldSelector: selector})
		if err == nil {
			for _, event := range events.Items {
				if event.Reason == "Conflict" && event.InvolvedObject.Name == "cpc2" {
					t.Log("conflict on cpc2 found")
					return true, nil
				}
			}
			t.Logf("event not found; known events are: %+v", events.Items)
		}
		return false, nil
	})
	assert.NilError(t, err)
}

func TestMultitierRanks(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 3, 2)

	selector := getMultitierSelector(ctx, t)

	t.Logf("using nodes matching %v", selector)

	nodeToPool := map[string]string{}
	pools := map[string]bool{}
	nodes, err := K8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
NodeLoop:
	for _, node := range nodes.Items {
		labels := node.GetLabels()
		assert.Assert(t, labels != nil)
		for key, target := range selector {
			label, found := labels[key]
			if !found || label != target {
				continue NodeLoop
			}
			pool := labels[nodePoolLabel]
			assert.Assert(t, pool != "")
			pools[pool] = true
			nodeToPool[node.Name] = pool
		}
	}
	t.Logf("%d nodes and %d pools", len(nodeToPool), len(pools))

	jobset := createMultitierJobset(ctx, t, "job", selector, 3, 2)

	nodeMap := verifyRankIndicies(ctx, t, "job", 3, 2, nil)

	// The worker ranks should match the jobset.
	pods, err := K8sClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{})
	assert.NilError(t, err)
	for _, pod := range pods.Items {
		assert.Assert(t, strings.HasPrefix(pod.GetName(), "job-"), "unexpected pod in test cluster: %s/%s", testNamespace, pod.GetName())
		ann := pod.GetAnnotations()
		assert.Assert(t, ann != nil)
		slice, err := util.GetAnnotationInt(ann, "jobset.sigs.k8s.io/job-index")
		assert.NilError(t, err)
		worker, err := util.GetAnnotationInt(ann, "batch.kubernetes.io/job-completion-index")
		assert.NilError(t, err)
		index := slice*2 + worker
		assert.Equal(t, nodeMap[index], pod.Spec.NodeName)
	}

	// Delete and recreate the jobset, confirm the ranks stay the same.
	t.Logf("Deleting the jobset")
	err = CRClient.Delete(ctx, jobset)
	assert.NilError(t, err)
	// If pods linger, they can block the scheduling of new pods.
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		pods, err := K8sClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{})
		assert.NilError(t, err)
		return len(pods.Items) == 0, nil
	})
	t.Logf("Recreating the jobset")
	createMultitierJobset(ctx, t, "job", selector, 3, 2)
	verifyRankIndicies(ctx, t, "job", 3, 2, nodeMap)
}

func TestMultitierNewJob(t *testing.T) {
	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, 3, 2)

	selector := getMultitierSelector(ctx, t)

	t.Logf("using nodes matching %v", selector)

	jobset := createMultitierJobset(ctx, t, "job", selector, 3, 2)
	nodeMap := verifyRankIndicies(ctx, t, "job", 3, 2, nil)

	// Create a new 1x2 jobset with a different name that must discard the old ranks.
	t.Logf("Deleting the jobset")
	err := CRClient.Delete(ctx, jobset)
	assert.NilError(t, err)

	// Delete the pool with index 0, so that it's not possible to succeed with the ranks
	// of the old job.
	pool := poolOfNode(ctx, t, nodeMap[0])
	deleteNodePool(ctx, pool)

	// If pods linger, they can block the scheduling of new pods.
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		pods, err := K8sClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{})
		assert.NilError(t, err)
		return len(pods.Items) == 0, nil
	})

	t.Logf("Creating job2")
	createMultitierJobset(ctx, t, "job2", selector, 1, 2)
	verifyRankIndicies(ctx, t, "job2", 1, 2, nil)
}

func TestScaleMultitierEphemeralConflicts(t *testing.T) {
	params := getScaleTestParams(t)

	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice)

	selector := getMultitierSelector(ctx, t)

	// Create several ephemeral jobsets with the same number of pods order-of-magnitude.
	const numEphemeralJobsets = 10
	for i := 0; i < numEphemeralJobsets; i++ {
		createEphemeralJob(ctx, t, fmt.Sprintf("ephermeral-%d", i), selector, params.slices*params.nodesPerSlice)
	}
	numEphemeralPods := numEphemeralJobsets * params.slices * params.nodesPerSlice
	// Wait for all ephemeral jobset pods to exist. They will complete quickly, but it's
	// okay if they overlap with the multitier jobset pods.
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		pods, err := K8sClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{})
		assert.NilError(t, err)
		t.Logf("saw %d/%d pods", len(pods.Items), numEphemeralPods)
		return len(pods.Items) >= numEphemeralPods/5, nil
	})
	svr := startScaleTestServer(t)
	defer svr.close()

	createMultitierScaleJobset(ctx, t, "scale-job", selector, params.slices, params.nodesPerSlice)

	svr.verify(ctx, t, params.slices*params.nodesPerSlice)
}

func TestScaleMultitierNodeContention(t *testing.T) {
	params := getScaleTestParams(t)

	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	selector := getMultitierSelector(ctx, t)

	svr := startScaleTestServer(t)
	defer svr.close()

	numNodes := params.slices * params.nodesPerSlice

	createMultitierScaleJobsetImmediateFinish(ctx, t, "job1", selector, params.slices, params.nodesPerSlice)
	svr.verify(ctx, t, numNodes)
	svr.deleteMaps(ctx, t)

	createMultitierScaleJobset(ctx, t, "job2", selector, params.slices, params.nodesPerSlice)
	svr.verify(ctx, t, numNodes)
}

func TestScaleMultitierJobRecreate(t *testing.T) {
	params := getScaleTestParams(t)

	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice)

	selector := getMultitierSelector(ctx, t)

	svr := startScaleTestServer(t)
	defer svr.close()

	numNodes := params.slices * params.nodesPerSlice

	jobset := createMultitierScaleJobset(ctx, t, "job", selector, params.slices, params.nodesPerSlice)
	nodeToIdx := svr.verify(ctx, t, numNodes)

	svr.deleteMaps(ctx, t)

	forceScheduling(ctx, t, "job", perturbNodeMap(t, nodeToIdx, params.nodesPerSlice), params.nodesPerSlice)
	defer relaxScheduling(ctx, t)

	err := CRClient.Delete(ctx, jobset)
	assert.NilError(t, err)

	createMultitierScaleJobset(ctx, t, "job", selector, params.slices, params.nodesPerSlice)
	newNodeToIdx := svr.verify(ctx, t, numNodes)

	for node, idx := range nodeToIdx {
		newIdx, found := newNodeToIdx[node]
		assert.Assert(t, found && newIdx == idx, "bad idx after recreate: for %s got (%t) %d, expected %d", node, found, newIdx, idx)
	}
	t.Logf("verified consistent indicies post-restart!")
}

func TestScaleMultitierPodFailures(t *testing.T) {
	params := getScaleTestParams(t)

	ctx := context.Background()

	cleanup := deployMultitier(ctx, t)
	defer cleanup()

	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice)

	selector := getMultitierSelector(ctx, t)

	svr := startScaleTestServer(t)
	defer svr.close()

	numNodes := params.slices * params.nodesPerSlice

	createMultitierScaleJobset(ctx, t, "job", selector, params.slices, params.nodesPerSlice)
	nodeToIdx := svr.verify(ctx, t, numNodes)
	svr.deleteMaps(ctx, t)

	forceScheduling(ctx, t, "job", perturbNodeMap(t, nodeToIdx, params.nodesPerSlice), params.nodesPerSlice)
	defer relaxScheduling(ctx, t)

	pods, err := K8sClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{})
	assert.NilError(t, err)
	err = wait.PollUntilContextTimeout(ctx, time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := runOnPod(ctx, t, &pods.Items[0], "worker", "touch", fmt.Sprintf("/control/fail"))
		return err == nil, nil
	})
	assert.NilError(t, err)

	newNodeToIdx := svr.verify(ctx, t, numNodes)

	for node, idx := range nodeToIdx {
		newIdx, found := newNodeToIdx[node]
		assert.Assert(t, found && newIdx == idx, "bad idx after recreate: for %s got (%t) %d, expected %d", node, found, newIdx, idx)
	}
	t.Logf("verified consistent indicies post-restart!")
}
