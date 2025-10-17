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
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	testutil "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/hack"
)

const (
	MappingName         = "process-mapping"
	ControllerNamespace = "gkecheckpoint"

	WaitInterval = 1 * time.Second
	driverName   = "checkpoint.csi.storage.gke.io"
)

var (
	kubeClient *kubernetes.Clientset
	k8sClient  client.Client
	testCfg    *rest.Config

	globalCancel context.CancelFunc

	uidCounter int

	controllerOptions ControllerOpts

	WaitTimeout = 10 * time.Second

	testControllerOpts = config.Controller{SkipNameValidation: ptr.To(true)}
)

func TestMain(m *testing.M) {
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	testutil.SetupEnviron(context.TODO())

	Init() // Setup the scheme

	m.Run()
}

func mustSetupIdFileCluster() (context.Context, func(context.Context)) {
	return mustSetupCluster(func(cfg *rest.Config) (ctrl.Manager, error) {
		controllerOptions = ControllerOpts{
			Namespace:  ControllerNamespace,
			DriverName: driverName,
		}
		return NewIdFileManager(testCfg, controllerOptions, testControllerOpts)
	})
}

func mustSetupCluster(createMgr func(*rest.Config) (ctrl.Manager, error)) (context.Context, func(context.Context)) {
	var ctx context.Context
	ctx, globalCancel = context.WithCancel(context.TODO())
	log := log.FromContext(ctx)

	testEnv := &envtest.Environment{
		UseExistingCluster: ptr.To(false),
		CRDDirectoryPaths:  []string{"../../test/crds"},
	}
	var err error
	testCfg, err = testEnv.Start()
	if err != nil {
		log.Error(err, "cannot start testenv")
		os.Exit(1)
	}

	kubeClient, err = kubernetes.NewForConfig(testCfg)
	if err != nil {
		log.Error(err, "cannot get kubeclient")
		os.Exit(1)
	}
	k8sClient, err = client.New(testCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		log.Error(err, "cannot create client")
		os.Exit(1)
	}

	manager, err := createMgr(testCfg)
	if err != nil {
		log.Error(err, "cannot setup manager")
		os.Exit(1)
	}

	managerShutdown := make(chan struct{})
	go func() {
		err := manager.Start(ctx)
		log.Error(err, "manager shutdown")
		close(managerShutdown)
	}()

	var ns corev1.Namespace
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: ControllerNamespace}, &ns); apierrors.IsNotFound(err) {
		ns.SetName(ControllerNamespace)
		if err := k8sClient.Create(ctx, &ns); err != nil {
			log.Error(err, "Can't create namespace", "namespace", ControllerNamespace)
			os.Exit(1)
		}
	}

	return ctx, func(_ context.Context) {
		globalCancel()
		if err := testEnv.Stop(); err != nil {
			log.Error(err, "testenv shutdown")
			os.Exit(1)
		}
		log.Info("waiting for manager shutdown")
		<-managerShutdown
		log.Info("manager shutdown successful")
	}
}

func nextUid() int {
	id := uidCounter
	uidCounter++
	return id
}

func createJobSet(ctx context.Context, t *testing.T, name string, slices, workersPerSlice int) *jobsetv1alpha.JobSet {
	t.Helper()
	jobset := jobsetv1alpha.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: jobsetv1alpha.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha.ReplicatedJob{
				{
					Name:     "slice",
					Replicas: int32(slices),
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Parallelism: ptr.To(int32(workersPerSlice)),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "container",
											Image: "debian",
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
													Driver: driverName,
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
	err := k8sClient.Create(ctx, &jobset)
	assert.NilError(t, err)

	return &jobset
}

func createWorker(ctx context.Context, t *testing.T, jobName string, slice, worker int, node string, restart int) *corev1.Pod {
	t.Helper()
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d-%d-xxx%d", jobName, slice, worker, nextUid()),
			Namespace: "default",
			Annotations: map[string]string{
				"jobset.sigs.k8s.io/jobset-name":           jobName,
				"jobset.sigs.k8s.io/job-index":             strconv.Itoa(slice),
				"jobset.sigs.k8s.io/restart-attempt":       strconv.Itoa(restart),
				"batch.kubernetes.io/job-completion-index": strconv.Itoa(worker),
			},
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{
				{
					Name:  "pod",
					Image: "debian",
					VolumeMounts: []corev1.VolumeMount{
						{
							MountPath: "/local",
							Name:      "ramdisk",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "ramdisk",
					VolumeSource: corev1.VolumeSource{
						CSI: &corev1.CSIVolumeSource{
							Driver: driverName,
						},
					},
				},
			},
		},
	}
	err := k8sClient.Create(ctx, &pod)
	assert.NilError(t, err)
	return &pod
}

func createNoVolumeWorker(ctx context.Context, t *testing.T, jobName string, slice, worker int, node string, restart int) *corev1.Pod {
	t.Helper()
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d-%d-xxx%d", jobName, slice, worker, nextUid()),
			Namespace: "default",
			Annotations: map[string]string{
				"jobset.sigs.k8s.io/jobset-name":           jobName,
				"jobset.sigs.k8s.io/job-index":             strconv.Itoa(slice),
				"jobset.sigs.k8s.io/restart-attempt":       strconv.Itoa(restart),
				"batch.kubernetes.io/job-completion-index": strconv.Itoa(worker),
			},
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{
				{
					Name:  "pod",
					Image: "debian",
				},
			},
		},
	}
	err := k8sClient.Create(ctx, &pod)
	assert.NilError(t, err)
	return &pod
}

func createNode(ctx context.Context, t *testing.T, name, pool string) *corev1.Node {
	t.Helper()
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				nodePoolLabel: pool,
			},
		},
	}
	err := k8sClient.Create(ctx, &node)
	assert.NilError(t, err)
	return &node
}

// Retry until the condition function is true; assert no timeout.
func waitFor(ctx context.Context, t *testing.T, condition func(ctx context.Context) (bool, error)) {
	t.Helper()
	err := wait.PollUntilContextTimeout(ctx, WaitInterval, WaitTimeout, true, condition)
	assert.NilError(t, err)
}

func waitForNodeMapping(ctx context.Context, t *testing.T, job, node string, generation int) MappingInfo {
	t.Helper()
	var info MappingInfo
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		var err error
		info, err = getNodeMapping(ctx, kubeClient, ControllerNamespace, node)
		if apierrors.IsNotFound(err) {
			return false, nil // retry
		}
		if err != nil {
			return false, err
		}
		if info.Job.Name != job {
			return false, nil // retry
		}
		if info.Generation != generation {
			return false, nil // retry
		}
		return true, nil
	})
	return info
}

func waitForNullGeneration(ctx context.Context, t *testing.T, job, node string) MappingInfo {
	t.Helper()
	var info MappingInfo
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		var err error
		info, err = getNodeMapping(ctx, kubeClient, ControllerNamespace, node)
		if err != nil {
			return false, err
		}
		return info.Job.Name == "job" && info.Generation == -1, nil
	})
	return info
}

func waitForCoordinator(ctx context.Context, t *testing.T, job string, targetGeneration int) CoordinatorInfo {
	t.Helper()
	var info CoordinatorInfo
	err := wait.PollUntilContextTimeout(ctx, WaitInterval, WaitTimeout, true, func(ctx context.Context) (bool, error) {
		var err error
		info, err = getCoordinatorInfo(ctx, kubeClient, ControllerNamespace, types.NamespacedName{Namespace: "default", Name: job})
		if apierrors.IsNotFound(err) {
			return false, nil // retry
		}
		if err != nil {
			return false, err
		}
		if info.Generation >= targetGeneration {
			return true, nil
		}
		return false, nil // retry
	})
	assert.NilError(t, err)
	return info
}

func waitForMissingConfigMap(ctx context.Context, t *testing.T, name string) {
	t.Helper()
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		var configMap corev1.ConfigMap
		for i := 0; i < 6; i++ {
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ControllerNamespace, Name: name}, &configMap)
			if !apierrors.IsNotFound(err) {
				return false, nil // retry
			}
		}
		return true, nil
	})
}

func TestControllerSingleWorker(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	createNode(ctx, t, "node-a", "pool-0")
	worker := createWorker(ctx, t, "job", 2, 1, "node-a", 0)
	worker.Status = corev1.PodStatus{
		PodIP: "192.168.0.33",
	}
	err := k8sClient.Status().Update(ctx, worker)
	assert.NilError(t, err)

	info := waitForNodeMapping(ctx, t, "job", "node-a", 0)
	fmt.Println(info)
	assert.Equal(t, info.Address, "192.168.0.33")
	assert.Equal(t, info.ProcessIndex, 2*2+1)
}

func TestControllerFullStartup(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		workers = append(workers, createWorker(ctx, t, "job", slice, worker, node, 0))
	}

	// Assign IPs in reverse order to avoid accidentally assigning to expected nodes.
	for i := 5; i >= 0; i-- {
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}
	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}

	cleanup(ctx)
}

func TestControllerFailedSlice(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}

	// Delete slice 1
	for i := 2; i < 4; i++ {
		err := k8sClient.Delete(ctx, nodes[i])
		assert.NilError(t, err)
	}

	// Recreate workers, shuffling who goes where and updating the IPs.
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		nodeIndex := (i + 2) % 6
		node := fmt.Sprintf("node-%d", nodeIndex)
		if nodeIndex >= 2 && nodeIndex < 4 {
			// Replace the deleted node
			node = fmt.Sprintf("new-node-%d", nodeIndex)
			nodes[i] = createNode(ctx, t, node, "new-pool")
		}
		workers[i] = createWorker(ctx, t, "job", slice, worker, node, 1)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", nodeIndex),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
		time.Sleep(50 * time.Millisecond) // Sleep to get the update sequence deterministic.
	}

	for i := 0; i < 6; i++ {
		node := fmt.Sprintf("node-%d", i)
		if i >= 2 && i < 4 {
			node = fmt.Sprintf("new-node-%d", i)
		}
		info := waitForNodeMapping(ctx, t, "job", node, 1)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.1.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
}

func TestControllerFailureInSlice(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}

	// Delete one node of slice 1
	err := k8sClient.Delete(ctx, nodes[3])
	assert.NilError(t, err)

	// Delete the workers.
	for i := 0; i < 6; i++ {
		err := k8sClient.Delete(ctx, workers[i])
		assert.NilError(t, err)
	}

	nodes[3] = createNode(ctx, t, "new-node-3", "pool-1")

	// Recreate workers, moving them up a pool (0,1 -> pool 1, etc). Create
	// them in reverse order so the coordinator is created last this time.
	for i := 5; i >= 0; i-- {
		n := (i + 2) % 6
		workers[i] = createWorker(ctx, t, "job", i/2, i%2, nodes[n].GetName(), 1)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", n),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		node := fmt.Sprintf("node-%d", i)
		if i == 3 {
			node = "new-node-3"
		}
		info := waitForNodeMapping(ctx, t, "job", node, 1)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.1.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
}

func TestControllerRecreatedSlice(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}

	// Delete nodes in slice 1
	for i := 2; i < 4; i++ {
		err := k8sClient.Delete(ctx, nodes[i])
		assert.NilError(t, err)
	}

	// Recreate workers, shuffling who goes where and updating the IPs.  The
	// shuffling will done means that new-node-2 will get process 3.  Nodes in
	// the deleted slice 1 will have new names; other nodes are the same.
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		nodeIndex := (i + 1) % 6
		node := fmt.Sprintf("node-%d", nodeIndex)
		if nodeIndex >= 2 && nodeIndex < 4 {
			// Replace the deleted node
			node = fmt.Sprintf("new-node-%d", nodeIndex)
			nodes[i] = createNode(ctx, t, node, "pool-1")
		}
		workers[i] = createWorker(ctx, t, "job", slice, worker, node, 1)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", nodeIndex),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
		time.Sleep(50 * time.Millisecond) // Sleep to get the update sequence deterministic.
	}

	for i := 0; i < 6; i++ {
		node := fmt.Sprintf("node-%d", i)
		if i >= 2 && i < 4 {
			node = fmt.Sprintf("new-node-%d", i)
		}
		info := waitForNodeMapping(ctx, t, "job", node, 1)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.1.%d", i))
		if i == 2 {
			assert.Equal(t, info.ProcessIndex, 3)
		} else if i == 3 {
			assert.Equal(t, info.ProcessIndex, 2)
		} else {
			assert.Equal(t, info.ProcessIndex, i)
		}
	}
}

func TestControllerNodeFailureAndRestartWithSameJobName(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}

	// Delete slice 0 by first deleting pods, then nodes.
	for i := 0; i < 2; i++ {
		err := k8sClient.Delete(ctx, workers[i])
		assert.NilError(t, err)
	}
	for i := 0; i < 2; i++ {
		err := k8sClient.Delete(ctx, nodes[i])
		assert.NilError(t, err)
	}

	// Delete remaining workers (as a jobset would do)
	for i := 2; i < 6; i++ {
		err := k8sClient.Delete(ctx, workers[i])
		assert.NilError(t, err)
	}

	// Create new node pool for slice 0.
	for i := 0; i < 2; i++ {
		nodes[i] = createNode(ctx, t, fmt.Sprintf("new-node-%d", i), "new-pool")
	}

	// Recreate workers on their same nodes, but with new IPs. To simulate the entire
	// jobset being recreated, the workers are created with generation 0.
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		workers[i] = createWorker(ctx, t, "job", slice, worker, nodes[i].GetName(), 0)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", i),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", nodes[i].GetName(), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.1.%d", i))
	}
}

func TestControllerScheduleAcrossSlices(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)

	// Schedule so that the jobset slices fall across the node slices.  The
	// first pod scheduled will match the jobset ids, but the others won't. The
	// node mapping is checked at each schedule so that it's deterministic.
	for i := 0; i < 6; i++ {
		nodeIndex := (i + 1) % 6
		jobsetSlice := i / 2
		nodeSlice := nodeIndex / 2
		jobsetWorker := i % 2
		node := fmt.Sprintf("node-%d", nodeIndex)
		pool := fmt.Sprintf("pool-%d", nodeSlice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", jobsetSlice, jobsetWorker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", nodeIndex),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
		info := waitForNodeMapping(ctx, t, "job", node, 0)
		switch i {
		case 0:
			assert.Equal(t, info.ProcessIndex, 0)
		case 1:
			assert.Equal(t, info.ProcessIndex, 3)
		case 2:
			assert.Equal(t, info.ProcessIndex, 2)
		case 3:
			assert.Equal(t, info.ProcessIndex, 5)
		case 4:
			assert.Equal(t, info.ProcessIndex, 4)
		case 5:
			assert.Equal(t, info.ProcessIndex, 1)
		}
	}
}

func TestControllerFailedCoordinator(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// Make the coordinator node unready
	nodes[0].Status.Conditions = []corev1.NodeCondition{
		{
			Type:   corev1.NodeMemoryPressure,
			Status: corev1.ConditionFalse,
		},
		{
			Type:   corev1.NodeReady,
			Status: corev1.ConditionUnknown,
		},
	}
	err := k8sClient.Status().Update(ctx, nodes[0])
	assert.NilError(t, err)

	// The coordinator should be unchanged
	coord = waitForCoordinator(ctx, t, "job", 0)
	assert.Assert(t, coord.Address != "", "coordinator configmap never appeared")
	assert.Equal(t, coord.Address, "192.168.0.0")

	// Delete all the pods and wait for the node mappings to be reset.
	for i := 0; i < 6; i++ {
		k8sClient.Delete(ctx, workers[i])
	}
	// The mapping for slice 0 is already deleted.
	for i := 2; i < 6; i++ {
		waitForNullGeneration(ctx, t, "job", fmt.Sprintf("node-%d", i))
	}

	// Recreate slice 0 pods on new nodes, with shuffled jobset indices.
	oldNode0 := nodes[0]
	oldNode1 := nodes[1]
	for i := 0; i < 2; i++ {
		slice := (i + 5) / 2
		worker := (i + 5) % 2
		node := fmt.Sprintf("new-node-%d", i)
		nodes[i] = createNode(ctx, t, node, "new-pool")
		workers[i] = createWorker(ctx, t, "job", slice, worker, node, 1)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", i),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}

	// The old nodes must be deleted to release the old slice indicies. Confirm
	// this will work if the delete happens after the new pods are scheduled.
	err = k8sClient.Delete(ctx, oldNode0)
	assert.NilError(t, err)
	err = k8sClient.Delete(ctx, oldNode1)
	assert.NilError(t, err)

	// Confirm the coordinator has updated to generation 1
	coord = waitForCoordinator(ctx, t, "job", 1)
	t.Logf("actual coord: %v", coord)
	assert.Assert(t, coord.Address == "192.168.1.0" || coord.Address == "192.168.1.1")

	// Check the new process indices
	i0 := waitForNodeMapping(ctx, t, "job", "new-node-0", 1)
	i1 := waitForNodeMapping(ctx, t, "job", "new-node-1", 1)
	assert.Assert(t, (i0.ProcessIndex == 0 && i1.ProcessIndex == 1) || (i0.ProcessIndex == 1 && i1.ProcessIndex == 0))
	assert.Equal(t, i0.Generation, 1)
	assert.Equal(t, i1.Generation, 1)

	// Recreate (different) pods on the other slices
	for i := 2; i < 6; i++ {
		slice := (i + 5) / 2
		worker := (i + 5) % 2
		node := fmt.Sprintf("node-%d", i)
		workers[i] = createWorker(ctx, t, "job", slice, worker, node, 1)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.2.%d", i),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}
	// Check the new process indices.
	for i := 2; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 1)
		assert.Equal(t, info.ProcessIndex, i)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.2.%d", i))
	}
}

func TestControllerRestartViaPodDelete(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// Delete all the pods and wait for the node mappings to be reset.
	for i := 0; i < 6; i++ {
		k8sClient.Delete(ctx, workers[i])
	}
	for i := 0; i < 6; i++ {
		waitForNullGeneration(ctx, t, "job", fmt.Sprintf("node-%d", i))
	}

	// Recreate jobs on shuffled nodes
	for i := 0; i < 6; i++ {
		slice := (i + 5) / 2
		worker := (i + 5) % 2
		node := fmt.Sprintf("node-%d", (i+1)%6)
		workers[i] = createWorker(ctx, t, "job", slice, worker, node, 1)
		workers[i].Status = corev1.PodStatus{
			// Match the IP index to the node index.
			PodIP: fmt.Sprintf("192.168.1.%d", (i+1)%6),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}

	// Confirm the coordinator has updated to generation 1.
	coord = waitForCoordinator(ctx, t, "job", 1)
	assert.Equal(t, coord.Address, "192.168.1.0")

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 1)
		assert.Equal(t, info.ProcessIndex, i)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.1.%d", i))
	}
}

func TestControllerRestartBeforePodDelete(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// Restart the coordinator
	k8sClient.Delete(ctx, workers[0])
	waitForNullGeneration(ctx, t, "job", "node-0")
	workers[0] = createWorker(ctx, t, "job", 0, 0, "node-0", 1)
	workers[0].Status = corev1.PodStatus{
		PodIP: "192.168.1.0",
	}
	err := k8sClient.Status().Update(ctx, workers[0])
	assert.NilError(t, err)
	waitForNodeMapping(ctx, t, "job", "node-0", 1)

	// Confirm the coordinator has updated to generation 1.
	coord = waitForCoordinator(ctx, t, "job", 1)
	assert.Assert(t, coord.Address == "192.168.1.0")

	// Delete & restart other pods.
	for i := 1; i < 6; i++ {
		k8sClient.Delete(ctx, workers[i])
	}
	for i := 1; i < 6; i++ {
		waitForNullGeneration(ctx, t, "job", fmt.Sprintf("node-%d", i))
	}

	// Recreate other jobs.
	for i := 1; i < 6; i++ {
		slice := (i + 5) / 2
		worker := (i + 5) % 2
		node := fmt.Sprintf("node-%d", i)
		workers[i] = createWorker(ctx, t, "job", slice, worker, node, 1)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", i),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}

	// Confirm all the node mappings.
	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 1)
		assert.Equal(t, info.ProcessIndex, i)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.1.%d", i))
	}
}

func TestControllerNoStaleCoordinator(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	nodes := make([]*corev1.Node, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		nodes = append(nodes, createNode(ctx, t, node, pool))
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// Node 5 should be on generation 0
	waitForNodeMapping(ctx, t, "job", "node-5", 0)

	// Delete 5, wait for the mapping to reset, and reschedule it. The coordinator should be unchanged.
	err := k8sClient.Delete(ctx, workers[5])
	assert.NilError(t, err)
	waitForNullGeneration(ctx, t, "job", "node-5")

	workers[5] = createWorker(ctx, t, "job", 2, 1, "node-5", 1)
	workers[5].Status = corev1.PodStatus{
		PodIP: "192.168.1.5",
	}
	err = k8sClient.Status().Update(ctx, workers[5])
	assert.NilError(t, err)
	coord = waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// The node-5 mapping should have an updated generation.
	waitForNodeMapping(ctx, t, "job", "node-5", 1)

	// Schedule a new coordinator (any pod on node 0). The coordinator generation should increase.
	w := createWorker(ctx, t, "job", 1, 0, "node-0", 1)
	w.Status = corev1.PodStatus{
		PodIP: "192.168.2.0",
	}
	err = k8sClient.Status().Update(ctx, w)
	assert.NilError(t, err)
	coord = waitForCoordinator(ctx, t, "job", 1)
	assert.Equal(t, coord.Address, "192.168.2.0")
}

func TestControllerWorkerRestart(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	info := waitForNodeMapping(ctx, t, "job", "node-5", 0)
	assert.Equal(t, info.Generation, 0)

	// Restart by deleting & creating a new pod, but on the same generation.
	err := k8sClient.Delete(ctx, workers[5])
	assert.NilError(t, err)

	// The mapping should get marked as unused.
	waitForNullGeneration(ctx, t, "job", "node-5")

	workers[5] = createWorker(ctx, t, "job", 2, 1, "node-5", 0)
	workers[5].Status = corev1.PodStatus{
		PodIP: "192.168.1.5",
	}
	err = k8sClient.Status().Update(ctx, workers[5])
	assert.NilError(t, err)

	// The coordinator and node mapping should be back on generation 0.
	coord = waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")
	info = waitForNodeMapping(ctx, t, "job", "node-5", 0)
	assert.Equal(t, info.Address, "192.168.1.5")
}

func TestControllerMultipleJobs(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// New job, overlapping on some nodes.
	createJobSet(ctx, t, "job2", 2, 2)
	workers2 := make([]*corev1.Pod, 0, 4)
	createNode(ctx, t, "node2-0", "pool2-0")
	createNode(ctx, t, "node2-1", "pool2-0")
	for i := 0; i < 2; i++ {
		w := createWorker(ctx, t, "job2", 0, i, fmt.Sprintf("node2-%d", i), 0)
		workers2 = append(workers2, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.2.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}
	info := waitForNodeMapping(ctx, t, "job", "node-5", 0)
	assert.Equal(t, info.Generation, 0)
	coord = waitForCoordinator(ctx, t, "job2", 0)
	assert.Equal(t, coord.Address, "192.168.2.0")

	coord = waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")
}

func TestControllerJobsetRecreate(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	jobset := createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// Delete all pods and jobset
	for _, worker := range workers {
		err := k8sClient.Delete(ctx, worker)
		assert.NilError(t, err)
	}
	err := k8sClient.Delete(ctx, jobset)
	assert.NilError(t, err)

	// All the mappings should disappear after the jobset delete.
	for i := 0; i < 6; i++ {
		waitForMissingConfigMap(ctx, t, fmt.Sprintf("node-%d", i))
	}
	waitForMissingConfigMap(ctx, t, jobsetMappingName(types.NamespacedName{Namespace: ControllerNamespace, Name: jobset.GetName()}))

	// Start a new jobset with the same name, assigned to different nodes.
	createJobSet(ctx, t, "job", 3, 2)
	for i := 0; i < 6; i++ {
		nodeIdx := (i + 2) % 6 // Respect the node slicing
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", nodeIdx)
		workers[i] = createWorker(ctx, t, "job", slice, worker, node, 0)
		workers[i].Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, workers[i])
		assert.NilError(t, err)
	}

	// Since the jobset was recreated, the indicies should match that of the job.
	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", (i+2)%6), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
}

func TestControllerDelayedDelete(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	// Delete all but one worker
	for i := 0; i < 5; i++ {
		err := k8sClient.Delete(ctx, workers[i])
		assert.NilError(t, err)
	}

	// Create new workers, with IPs 192.168.1.*.
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		w := createWorker(ctx, t, "job", slice, worker, node, 1)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	// Make sure the new worker is updated
	info := waitForNodeMapping(ctx, t, "job", "node-5", 1)
	assert.Equal(t, info.Address, "192.168.1.5")

	// Delete the straggling worker.
	err := k8sClient.Delete(ctx, workers[5])
	assert.NilError(t, err)

	// Let the controller win the race to do any update.
	time.Sleep(250 * time.Millisecond)

	// But the node mapping shouldn't have updated.
	info = waitForNodeMapping(ctx, t, "job", "node-5", 1)
	assert.Equal(t, info.Address, "192.168.1.5")
}

func TestControllerJobsetDelete(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	jobset := createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
		assert.Equal(t, info.Address, fmt.Sprintf("192.168.0.%d", i))
		assert.Equal(t, info.ProcessIndex, i)
	}
	coord := waitForCoordinator(ctx, t, "job", 0)
	assert.Equal(t, coord.Address, "192.168.0.0")

	err := k8sClient.Delete(ctx, jobset)
	assert.NilError(t, err)

	for i := 0; i < 6; i++ {
		waitForMissingConfigMap(ctx, t, fmt.Sprintf("node-%d", i))
	}
	waitForMissingConfigMap(ctx, t, jobsetMappingName(types.NamespacedName{Namespace: ControllerNamespace, Name: jobset.GetName()}))
}

func TestControllerDelayedJobsetDelete(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
	}

	// create new pods for the jobset, simulating a restart where the
	// delete events have come out-of-order.
	newWorkers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", (i+2)%6)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		newWorkers = append(newWorkers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.1.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}
	for i := 0; i < 6; i++ {
		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			info := waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
			// Process indicies should be equal to the first job.
			return info.PodName == newWorkers[(i+4)%6].GetName() && info.ProcessIndex == i, nil
		})
	}
	waitFor(ctx, t, func(ctx context.Context) (bool, error) {
		coord := waitForCoordinator(ctx, t, "job", 0)
		return coord.Address == "192.168.1.4", nil
	})
}

func TestControllerInitialize(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	workers := make([]*corev1.Pod, 0, 6)
	for i := 0; i < 6; i++ {
		slice := i / 2
		worker := i % 2
		node := fmt.Sprintf("node-%d", i)
		pool := fmt.Sprintf("pool-%d", slice)
		createNode(ctx, t, node, pool)
		w := createWorker(ctx, t, "job", slice, worker, node, 0)
		workers = append(workers, w)
		w.Status = corev1.PodStatus{
			PodIP: fmt.Sprintf("192.168.0.%d", i),
		}
		err := k8sClient.Status().Update(ctx, w)
		assert.NilError(t, err)
	}

	for i := 0; i < 6; i++ {
		waitForNodeMapping(ctx, t, "job", fmt.Sprintf("node-%d", i), 0)
	}

	// Delete one slice
	for i := 0; i < 2; i++ {
		err := k8sClient.Delete(ctx, workers[i])
		assert.NilError(t, err)
	}
	waitForNodeMapping(ctx, t, "job", "node-0", -1)
	waitForNodeMapping(ctx, t, "job", "node-1", -1)

	t.Log("restarting manager")

	globalCancel()
	// TODO: the new manager won't start correctly if the old one isn't torn down. Is
	// there a better way than sleeping to do this?
	time.Sleep(5 * time.Second)

	ctx, globalCancel = context.WithCancel(context.TODO())
	mgr, err := NewIdFileManager(testCfg, controllerOptions, testControllerOpts)
	assert.NilError(t, err)

	go func() {
		if err := mgr.Start(ctx); err != nil {
			log.FromContext(ctx).Error(err, "manager startup")
			os.Exit(1)
		}
	}()

	t.Log("manager restarted, scheduling nodes")

	// Schedule the deleted nodes in a different order. If the cluster isn't
	// initialized correctly, the mappings will be backwards.
	workers[0] = createWorker(ctx, t, "job", 0, 0, "node-1", 1)
	workers[0].Status = corev1.PodStatus{
		PodIP: "192.168.1.2",
	}
	err = k8sClient.Status().Update(ctx, workers[0])
	assert.NilError(t, err)
	workers[1] = createWorker(ctx, t, "job", 0, 0, "node-0", 1)
	workers[1].Status = corev1.PodStatus{
		PodIP: "192.168.0.2",
	}
	err = k8sClient.Status().Update(ctx, workers[1])
	assert.NilError(t, err)

	info := waitForNodeMapping(ctx, t, "job", "node-0", 1)
	assert.Equal(t, info.ProcessIndex, 0)
	info = waitForNodeMapping(ctx, t, "job", "node-1", 1)
	assert.Equal(t, info.ProcessIndex, 1)
}

func TestControllerNoVolumeJobset(t *testing.T) {
	ctx, cleanup := mustSetupIdFileCluster()
	defer cleanup(ctx)

	createJobSet(ctx, t, "job", 3, 2)
	createNode(ctx, t, "node-a", "pool-0")
	worker := createNoVolumeWorker(ctx, t, "job", 2, 1, "node-a", 0)
	worker.Status = corev1.PodStatus{
		PodIP: "192.168.0.33",
	}
	err := k8sClient.Status().Update(ctx, worker)
	assert.NilError(t, err)

	// Wait for the controller to do its thing (which should be nothing).
	time.Sleep(5 * time.Second)

	_, err = getNodeMapping(ctx, kubeClient, ControllerNamespace, "node-a")
	assert.Assert(t, apierrors.IsNotFound(err))
}
