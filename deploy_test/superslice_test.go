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

package deploy_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"gotest.tools/v3/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

var (
	supersliceNodePoolOpts = []NodePoolOption{
		MachineType("e2-standard-2"),
		DiskSizeGb(50),
		NodePoolLabel("cloud.google.com/gke-tpu-accelerator=tpu7x"),
		MaxPodsPerNode(18), // the tpu7x label makes some tpu driver pods get added to the node.
	}
)

func createSupersliceJobsetSpec(t *testing.T, name string, selector map[string]string) *jobsetv1alpha.JobSet {
	t.Helper()
	params := getSupersliceTestParams(t)
	if params == nil {
		t.Fatalf("no superslice parameters")
	}
	nodes := params.slices * params.nodesPerSlice
	jobset := &jobsetv1alpha.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: jobsetv1alpha.JobSetSpec{
			FailurePolicy: &jobsetv1alpha.FailurePolicy{
				MaxRestarts: 0,
			},
			ReplicatedJobs: []jobsetv1alpha.ReplicatedJob{
				{
					Name:     "superslice",
					Replicas: 1,
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Parallelism:  ptr.To(int32(nodes)),
							Completions:  ptr.To(int32(nodes)),
							BackoffLimit: ptr.To(int32(0)),
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"job": name,
									},
									Annotations: map[string]string{
										"gke-gcsfuse/volumes": "true",
									},
								},
								Spec: corev1.PodSpec{
									TerminationGracePeriodSeconds: ptr.To(int64(1)),
									RestartPolicy:                 corev1.RestartPolicyNever,
									ServiceAccountName:            getEmulatorKSA(t),
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
											Name:  "fake-ml-model",
											Image: params.workerImage,
											Command: []string{
												"python3",
												"fake-ml-model-entrypoint.py",
												"--no-use_redis",
											},
											Env: []corev1.EnvVar{
												{
													Name: "POD_INDEX",
													ValueFrom: &corev1.EnvVarSource{
														FieldRef: &corev1.ObjectFieldSelector{
															FieldPath: "metadata.labels['batch.kubernetes.io/job-completion-index']",
														},
													},
												},
												{
													Name: "JOB_INDEX",
													ValueFrom: &corev1.EnvVarSource{
														FieldRef: &corev1.ObjectFieldSelector{
															FieldPath: "metadata.labels['jobset.sigs.k8s.io/job-index']",
														},
													},
												},
												{
													Name:  "NODE_COUNT",
													Value: strconv.Itoa(nodes),
												},
												{
													Name:  "WORKER_COUNT",
													Value: "1",
												},
												{
													Name:  "NODE_COUNT_PER_JOB",
													Value: strconv.Itoa(params.replicaSize),
												},
												{
													Name:  "NO_DEBUG_BACKUP",
													Value: "true",
												},
												{
													Name:  "JOB_NAME",
													Value: "dima",
												},
												{
													Name:  "WORKLOAD_UUID",
													Value: name,
												},
											},
											VolumeMounts: []corev1.VolumeMount{
												{
													MountPath: "/app/local",
													Name:      "checkpoint",
												},
												{
													MountPath: "/app/backup",
													Name:      "backup",
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
											Name: "backup",
											VolumeSource: corev1.VolumeSource{
												CSI: &corev1.CSIVolumeSource{
													Driver: "gcsfuse.csi.storage.gke.io",
													VolumeAttributes: map[string]string{
														"bucketName": getEmulatorBucket(t),
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
		},
	}
	if params.nodesPerSlice != 16 {
		if jobset.Annotations == nil {
			jobset.Annotations = map[string]string{}
		}
		// See pkg/idfile/ranks_server.go.
		jobset.Annotations["multitier-checkpoint.gke.io/workload-kind"] = "emulated-superslice"
	}
	return jobset
}

func cleanPreviousJobset(ctx context.Context, t *testing.T, name string) {
	t.Helper()
	err := CRClient.Delete(ctx, &jobsetv1alpha.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("Cannot cleanup previous jobset %s/%s: %v", testNamespace, name, err)
	}
}

func createSupersliceJobset(ctx context.Context, t *testing.T, name string, selector map[string]string) *jobsetv1alpha.JobSet {
	t.Helper()
	cleanPreviousJobset(ctx, t, name)
	jobset := createSupersliceJobsetSpec(t, name, selector)
	err := CRClient.Create(ctx, jobset)
	assert.NilError(t, err, "cannot create jobset %s", name)
	t.Logf("created jobset %s/%s", jobset.GetNamespace(), jobset.GetName())
	return jobset
}

func createTwoWorkerSupersliceJobset(ctx context.Context, t *testing.T, name string, selector map[string]string) *jobsetv1alpha.JobSet {
	t.Helper()
	cleanPreviousJobset(ctx, t, name)

	jobset := createSupersliceJobsetSpec(t, name, selector)

	podSpec := &jobset.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec
	podSpec.Containers = append(podSpec.Containers, podSpec.Containers[0])
	for i := 0; i < 2; i++ {
		podSpec.Containers[i].Name = fmt.Sprintf("fake-ml-model-%d", i)
		for j, env := range podSpec.Containers[i].Env {
			if env.Name == "WORKER_COUNT" {
				podSpec.Containers[i].Env[j].Value = "2"
				break
			}
		}
		podSpec.Containers[i].Env = append(podSpec.Containers[i].Env, corev1.EnvVar{
			Name:  "WORKER_INDEX",
			Value: fmt.Sprintf("%d", i),
		})
	}

	err := CRClient.Create(ctx, jobset)
	assert.NilError(t, err, "cannot create jobset %s", name)
	t.Logf("created jobset %s/%s", jobset.GetNamespace(), jobset.GetName())
	return jobset
}

func TestSupersliceDataParallel(t *testing.T) {
	params := getSupersliceTestParams(t)
	if params == nil {
		t.Skip("No superslice parameters")
	}
	ctx := context.Background()
	// The gcs account used for the emulated jobset is in the default namespace; it needs
	// to be set up bucket reads which is done manually as described in the README. Maybe
	// a test namespace KSA can be created in this test and we can run the gcloud IAM
	// commands; it seems a little dangerous to be updating IAM from a test, though.
	cleanup := deployMultitier(ctx, t, useDefaultNamespace{}, ramdiskSize("50Mi"), tpuKind("tpu7x"))
	defer cleanup()

	uuid := "superslice"

	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice, supersliceNodePoolOpts...)

	selector := getMultitierSelector(ctx, t)

	setEmulatorConfig(ctx, t, uuid, 0, 4, waitForDisruption)

	waitForDaemonset(ctx, t, params.slices, params.nodesPerSlice, supersliceNodePoolOpts...)

	jobset := createSupersliceJobset(ctx, t, uuid, selector)
	defer cleanupJobset(ctx, t, jobset.GetNamespace(), jobset.GetName())

	t.Log("jobset created, waiting to disrupt")
	assert.Assert(t, waitForDisruptionReady(ctx, t, uuid, 4))
	assert.Assert(t, clearDistruptionFiles(ctx, t, uuid))
	t.Log("starting disruption")
	pools := getTestSlices(ctx, t, supersliceNodePoolOpts...)
	for pool := range pools {
		deleteNodePool(ctx, pool)
		break
	}
	waitForFailedJobset(ctx, t, jobset)
	assert.NilError(t, CRClient.Delete(ctx, jobset))
	t.Log("detected failed jobset, recreating nodes")

	// This will recreate the missing pool
	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice, supersliceNodePoolOpts...)

	t.Log("creating new emulated and jobset")
	setEmulatorConfig(ctx, t, uuid, 4, 7)

	jobset = createSupersliceJobset(ctx, t, uuid, selector)

	t.Log("waiting for second run to succeed")
	waitForJobsetFinished(ctx, t, jobset)
}

func TestSupersliceTwoWorker(t *testing.T) {
	t.Skip("Orbax doesn't support two workers per node, and hence our emulated job doesn't know how to write the checkpoints for this case")
	params := getSupersliceTestParams(t)
	if params == nil {
		t.Skip("No superslice parameters")
	}
	ctx := context.Background()
	cleanup := deployMultitier(ctx, t, useDefaultNamespace{}, ramdiskSize("50Mi"))
	defer cleanup()

	uuid := "superslice"

	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice, supersliceNodePoolOpts...)

	selector := getMultitierSelector(ctx, t)

	setEmulatorConfig(ctx, t, uuid, 0, 4, waitForDisruption)

	waitForDaemonset(ctx, t, params.slices, params.nodesPerSlice, supersliceNodePoolOpts...)

	jobset := createTwoWorkerSupersliceJobset(ctx, t, uuid, selector)
	defer cleanupJobset(ctx, t, jobset.GetNamespace(), jobset.GetName())

	t.Log("jobset created, waiting to disrupt")
	assert.Assert(t, waitForDisruptionReady(ctx, t, uuid, 4))
	assert.Assert(t, clearDistruptionFiles(ctx, t, uuid))
	t.Log("starting disruption")
	pools := getTestSlices(ctx, t, supersliceNodePoolOpts...)
	for pool := range pools {
		deleteNodePool(ctx, pool)
		break
	}
	waitForFailedJobset(ctx, t, jobset)
	assert.NilError(t, CRClient.Delete(ctx, jobset))
	t.Log("detected failed jobset, recreating nodes")

	// This will recreate the missing pool
	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice, supersliceNodePoolOpts...)

	t.Log("creating new emulated and jobset")
	setEmulatorConfig(ctx, t, uuid, 4, 7)

	jobset = createTwoWorkerSupersliceJobset(ctx, t, uuid, selector)

	t.Log("waiting for second run to succeed")
	waitForJobsetFinished(ctx, t, jobset)
}
