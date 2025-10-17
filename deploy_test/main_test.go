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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
	"gotest.tools/v3/assert"
	ctrl "sigs.k8s.io/controller-runtime"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	rest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	checkpointv1 "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/apis/checkpointing.gke.io/v1"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	machineTypeLabel = "node.kubernetes.io/instance-type"
	nodePoolLabel    = "cloud.google.com/gke-nodepool"

	waitInterval = time.Second

	gcloudMaxOperationCount   = 10
	gcloudHeavyOperationCount = 500
)

var (
	kubeconfig = func() *string {
		if home := homedir.HomeDir(); home != "" {
			return flag.String("kubeconfig-path", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		}
		return flag.String("kubeconfig-path", "", "absolute path to the kubeconfig file")
	}()

	CRClient      client.Client
	K8sClient     *kubernetes.Clientset
	KubeConfig    *rest.Config
	testNamespace string

	// localNodePools tracks node pools we've created in this run.
	localNodePools = map[string]bool{}

	// gcloudOperationMutex is used to control simultaneous gcloud operations
	gcloudOperationMutex  sync.Mutex
	gcloudOperations      = semaphore.NewWeighted(gcloudMaxOperationCount)
	gcloudHeavyOperations = semaphore.NewWeighted(gcloudHeavyOperationCount)

	gcloudOpRunning = regexp.MustCompile(`status:.*StatusValueValuesEnum\(RUNNING, \d+\)`)

	// waitTimeout and verifyTimeout are tuned for long-running real cluster operations, and overridden for scale tests.
	waitTimeout   = 10 * time.Minute
	verifyTimeout = 5 * time.Minute
)

// Retry until the condition function is true; assert no timeout.
func waitFor(ctx context.Context, t *testing.T, condition func(ctx context.Context) (bool, error)) {
	t.Helper()
	err := wait.PollUntilContextTimeout(ctx, waitInterval, waitTimeout, true, condition)
	assert.NilError(t, err, time.Now())
}

func gcloudErrorIsRunning(err error) bool {
	return err != nil && gcloudOpRunning.FindString(err.Error()) != ""
}

func repoBase(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	assert.Assert(t, ok, "Could not retrieve caller information")
	// Assume this is called from <repo-base>/e2e.
	e2eDir := filepath.Dir(filename)
	base := filepath.Dir(e2eDir)
	t.Logf("Using %s as repo base", base)
	return base
}

func deployCRD(ctx context.Context, t *testing.T) {
	base := repoBase(t)
	t.Logf("deploying crd")
	out, err := util.RunCommand("kubectl", "apply", "-f", filepath.Join(base, "crd"))
	assert.NilError(t, err, "Could not deploy crd")
	t.Log(string(out))
}

func undeployCRD(ctx context.Context, t *testing.T) {
	base := repoBase(t)
	t.Logf("%v undeploying crd", time.Now())
	out, err := util.RunCommand("kubectl", "delete", "-f", filepath.Join(base, "crd"))
	assert.NilError(t, err, "Could not undeploy crd")
	t.Log(string(out))
}

func deployDir(ctx context.Context, t *testing.T, dir string) {
	base := repoBase(t)
	t.Logf("deploying %s", dir)
	out, err := util.RunCommand("kubectl", "apply", "-k", filepath.Join(base, "deploy", dir))
	assert.NilError(t, err, "Could not deploy %s", dir)
	t.Log(string(out))
}

func undeployDir(ctx context.Context, t *testing.T, dir string) {
	base := repoBase(t)
	t.Logf("%v undeploying %s", time.Now(), dir)
	out, err := util.RunCommand("kubectl", "delete", "-k", filepath.Join(base, "deploy", dir))
	assert.NilError(t, err, "Could not undeploy %s", dir)
	t.Log(string(out))
}

func getClusterName(ctx context.Context, t *testing.T) string {
	t.Helper()
	out, err := util.RunCommand("gcloud", "config", "get", "container/cluster")
	assert.NilError(t, err, "Could not get cluster")
	return strings.TrimSpace(string(out))
}

type NodePoolOptions struct {
	machineType    string
	numNodes       int
	label          string
	taint          string
	diskSizeGb     int
	maxPodsPerNode int
}

type NodePoolOption interface {
	Update(opts *NodePoolOptions)
}

type MachineType string

func (m MachineType) Update(opts *NodePoolOptions) {
	opts.machineType = string(m)
}

type NumNodes int

func (n NumNodes) Update(opts *NodePoolOptions) {
	opts.numNodes = int(n)
}

type NodePoolLabel string

func (l NodePoolLabel) Update(opts *NodePoolOptions) {
	opts.label = string(l)
}

type NodePoolTaint string

func (t NodePoolTaint) Update(opts *NodePoolOptions) {
	opts.taint = string(t)
}

type DiskSizeGb int

func (s DiskSizeGb) Update(opts *NodePoolOptions) {
	opts.diskSizeGb = int(s)
}

type MaxPodsPerNode int

func (n MaxPodsPerNode) Update(opts *NodePoolOptions) {
	opts.maxPodsPerNode = int(n)
}

// createUniqueNodePool is called from a goroutine so that creation can be parallelized, so we can't use testing.T.
func createUniqueNodePool(ctx context.Context, opts ...NodePoolOption) string {
	gcloudOperationMutex.Lock()

	listing, err := util.RunCommand("gcloud", "container", "node-pools", "list")
	if err != nil {
		panic(fmt.Sprintf("couldn't list nodes: %v", err))
	}
	names := map[string]bool{}
	for lineNum, line := range strings.Split(string(listing), "\n") {
		if lineNum == 0 {
			continue // skip the header
		}
		parts := strings.Split(line, " ")
		if len(parts) == 0 {
			continue
		}
		names[parts[0]] = true
	}
	collides := func(n string) bool {
		_, listed := names[n]
		_, local := localNodePools[n]
		return listed || local
	}
	idx := 0
	candidate := "pool-0"
	for collides(candidate) {
		idx++
		candidate = fmt.Sprintf("pool-%d", idx)
	}
	localNodePools[candidate] = true
	gcloudOperationMutex.Unlock()

	if err := gcloudOperations.Acquire(ctx, 1); err != nil {
		panic(fmt.Sprintf("coudl not acquire operation semaphore: %v", err))
	}
	defer gcloudOperations.Release(1)

	klog.Infof("creating pool %s", candidate)
	args := []string{
		"container",
		"node-pools",
		"create",
		candidate,
		"--no-enable-autoupgrade",
		"--quiet",
	}
	var options NodePoolOptions
	for _, o := range opts {
		o.Update(&options)
	}
	if options.machineType != "" {
		args = append(args, "--machine-type", options.machineType)
	}
	if options.numNodes > 0 {
		args = append(args, "--num-nodes", strconv.Itoa(options.numNodes))
	}
	if options.label != "" {
		args = append(args, "--node-labels", options.label)
	}
	if options.taint != "" {
		args = append(args, "--node-taints", fmt.Sprintf("%s=true:NoSchedule", options.taint))
	}
	if options.diskSizeGb > 0 {
		args = append(args, fmt.Sprintf("--disk-size=%dG", options.diskSizeGb))
	}
	if options.maxPodsPerNode > 0 {
		args = append(args, "--max-pods-per-node", strconv.Itoa(options.maxPodsPerNode))
	}
	_, err = util.RunCommand("gcloud", args...)
	if err != nil {
		panic(fmt.Sprintf("could not create pool %s: %v", candidate, err))
	}
	return candidate
}

// deleteNodePool is called from a goroutine so it can be parallelized, meaning we can't use testing.T.
func deleteNodePool(ctx context.Context, pool string) {
	if err := gcloudOperations.Acquire(ctx, 1); err != nil {
		panic(fmt.Sprintf("coudl not acquire operation semaphore: %v", err))
	}
	defer gcloudOperations.Release(1)
	klog.Infof("deleting pool %s", pool)
	_, err := util.RunCommand("gcloud", "container", "node-pools", "delete", pool, "--quiet")
	if err != nil {
		panic(fmt.Sprintf("could not delete pool %s: %v", pool, err))
	}
	gcloudOperationMutex.Lock()
	defer gcloudOperationMutex.Unlock()
	delete(localNodePools, pool)
}

func getNodesOfPool(ctx context.Context, t *testing.T, targetPool string) []string {
	t.Helper()
	nodes, err := K8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NilError(t, err, "couldn't list nodes")
	poolNodes := []string{}
	for _, node := range nodes.Items {
		pool, found := node.GetLabels()[nodePoolLabel]
		assert.Assert(t, found, "no pool label on %s", node.GetName())
		if pool == targetPool {
			poolNodes = append(poolNodes, node.GetName())
		}
	}
	return poolNodes
}

func getNodeZone(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	node, err := K8sClient.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	assert.NilError(t, err, "couldn't get node")
	zone := node.GetLabels()["topology.kubernetes.io/zone"]
	assert.Assert(t, zone != "")
	return zone
}

func recreateNodePool(ctx context.Context, t *testing.T, pool string) {
	t.Helper()
	poolSize := len(getNodesOfPool(ctx, t, pool))
	t.Logf("%v: scaling pool %s of %d nodes down to zero", time.Now(), pool, poolSize)
	clusterName := getClusterName(ctx, t)
	_, err := util.RunCommand("gcloud", "container", "clusters", "resize", clusterName, "--node-pool", pool, "--num-nodes", "0", "--quiet")
	assert.NilError(t, err, "couldn't resize node pool")
	t.Logf("%v: scaling pool %s back to %d", time.Now(), pool, poolSize)
	_, err = util.RunCommand("gcloud", "container", "clusters", "resize", clusterName, "--node-pool", pool, "--num-nodes", strconv.Itoa(poolSize), "--quiet")
	if !gcloudErrorIsRunning(err) {
		assert.NilError(t, err, "couldn't resize back node pool")
	}
	if gcloudErrorIsRunning(err) {
		t.Logf("resize exited but still running")
	}
	t.Logf("%v: resize finished", time.Now())
}

// deleteNode can be called from a goroutine, so testing.T cannot be used.
func deleteNode(ctx context.Context, node, zone string) {
	if err := gcloudHeavyOperations.Acquire(ctx, 1); err != nil {
		panic(fmt.Sprintf("couldn't acquire heavy operation semaphore: %v", err))
	}
	defer gcloudHeavyOperations.Release(1)
	var errors []string
	for i := 0; i < 10; i++ {
		_, err := util.RunCommand("gcloud", "compute", "instances", "delete", "--zone", zone, node, "--quiet")
		if err == nil {
			klog.Infof("deleted node %s", node)
			return
		} else if strings.Contains(err.Error(), "was not found") {
			return
		} else if strings.Contains(err.Error(), "Please try again in 30 seconds") {
			klog.Errorf("gcloud delete of %s needs a break, pausing", node)
			time.Sleep(30 * time.Second)
		} else {
			klog.Errorf("gcloud delete node %s failed. %d tries left: %v", node, 9-i, err)
			errors = append(errors, err.Error())
			time.Sleep(5 * time.Second)
		}
	}
	panic(fmt.Sprintf("failed to delete %s: %v", node, errors))
}

// deleteNodesInPool deletes (via GCE) the nodes in a pool. For large clusters,
// this is much faster than scaling the pool down. It assumes the pool is a
// single zone.
//
// While this is faster than scaling down in large clusters, it still has
// problems running consistently past 1k instances (operations time out, API
// quota gets used up, weird transient errors, etc).
func deleteNodesInPool(ctx context.Context, t *testing.T, pool string) {
	t.Helper()

	nodes := getNodesOfPool(ctx, t, pool)
	if len(nodes) == 0 {
		t.Logf("No nodes in pool %s?? Ignoring delete", pool)
		return
	}

	zone := getNodeZone(ctx, t, nodes[0])

	t.Logf("%v: deleting %d nodes in %s", time.Now(), len(nodes), pool)
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deleteNode(ctx, node, zone)
		}()
	}
	wg.Wait()
	t.Logf("%v: deleted nodes of %s", time.Now(), pool)
}

func poolOfNode(ctx context.Context, t *testing.T, nodeName string) string {
	t.Helper()
	node, err := K8sClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	assert.NilError(t, err)
	pool, found := node.GetLabels()[nodePoolLabel]
	assert.Assert(t, found)
	return pool
}

func runOnPod(ctx context.Context, t *testing.T, pod *corev1.Pod, container, cmd string, args ...string) (string, error) {
	t.Helper()
	output, err := util.RunCommand("kubectl", slices.Concat([]string{
		"exec",
		fmt.Sprintf("--namespace=%s", pod.GetNamespace()),
		pod.GetName(),
		"-c",
		container,
		"--",
		cmd,
	}, args)...)
	return string(output), err
}

func runOnNode(ctx context.Context, t *testing.T, node, cmd string, args ...string) (string, error) {
	// TODO: this assumes running from a cloudtop.
	cmd = fmt.Sprintf("--command=sudo %s %s", cmd, strings.Join(args, " "))
	output, err := util.RunCommand("gcloud", "compute", "ssh", node, cmd, "--", "-o", "ProxyCommand=corp-ssh-helper %h %p")
	t.Logf("on %s ran %s %s: %s", node, cmd, strings.Join(args, " "), string(output))
	return string(output), err
}

func waitForPodRunning(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	var err error
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
		pod, err = K8sClient.CoreV1().Pods(testNamespace).Get(ctx, pod.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.Spec.NodeName == "" {
			return false, nil // retry
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("unexpected pod exit: %v", pod.Status)
		}
		if pod.Status.Phase != corev1.PodRunning {
			return false, nil // retry
		}
		return true, nil
	})
	return pod, err
}

// captureLogs pipes the logs from test to a file, as long as the target namespace exists. This
// makes it robust to pods restarting or deployments rescaling.
func captureLogs(ctx context.Context, namespace, target, dest string) {
	captureLogsWithContainerAndNode(ctx, namespace, target, dest, "", "")
}

func captureLogsWithContainerAndNode(ctx context.Context, namespace, target, dest, container, node string) {
	klog.Infof("Capturing logs to %s for %s", dest, target)

	var destFile string
	if node != "" {
		destFile = filepath.Join(dest, fmt.Sprintf("%s-%s-%s.log", strings.ReplaceAll(target, "/", "-"), node, time.Now().Format(time.RFC3339)))
	} else {
		destFile = filepath.Join(dest, fmt.Sprintf("%s-%s.log", strings.ReplaceAll(target, "/", "-"), time.Now().Format(time.RFC3339)))
	}

	f, err := os.OpenFile(destFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		// This may be called from a goroutine, so we can't use t.Fatal.
		panic(fmt.Sprintf("cannot open logs file: %v", err))
	}

	go func() {
	loggingLoop:
		for {
			args := []string{"logs", "-n", namespace, target, "-f"}
			if container != "" {
				args = append(args, "-c", container)
			}
			cmd := exec.Command("kubectl", args...)
			cmd.Stdout = f

			err = cmd.Start()
			if err != nil {
				// Note t cannot be used in a goroutine.
				panic(fmt.Sprintf("cannot stream logs: %v", err))
			}

			cmd.Wait()

			time.Sleep(500 * time.Millisecond)
			_, err := K8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			if err != nil {
				klog.Infof("Logging of %s stopped as namespace %s not available: %v", target, namespace, err)
				break
			}

			if strings.HasPrefix(target, "pod/") {
				parts := strings.Split(target, "/")
				pod, err := K8sClient.CoreV1().Pods(namespace).Get(ctx, parts[1], metav1.GetOptions{})
				if err != nil {
					klog.Infof("Logging of %s/%s stopped as pod not available: %v", namespace, target, err)
					break
				}
				if pod.Spec.NodeName == "" {
					klog.Infof("pod not yet scheduled, will retry")
					time.Sleep(500 * time.Millisecond)
					continue
				}
				node, err := K8sClient.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
				if err != nil {
					klog.Infof("Node %s not available for %s/%s, ending logging", pod.Spec.NodeName, namespace, target)
					break
				}
				if node.Status.Phase != corev1.NodeRunning {
					klog.Infof("Node %s not running for %s/%s, ending logging: %+v", pod.Spec.NodeName, namespace, target, node.Status)
					break
				}
				for _, cond := range node.Status.Conditions {
					if cond.Type == corev1.NodeReady && cond.Status != corev1.ConditionTrue {
						klog.Infof("Node %s not ready for %s/%s, ending logging: %+v", pod.Spec.NodeName, namespace, target, node.Status)
						break loggingLoop
					}
				}
			}
			klog.Infof("restarting log capture for %s", target)
		}
		f.Close()
		klog.Infof("ended logging for %s", target)
	}()
}

// testNamespaceSetup creates an ephemeral namespace for a test. The returned function which deletes the
// namespace should be deferred.
//
// Note that this should be done after deferring the driver deployment, otherwise any pod using a volume managed
// by the driver will hang and not be able to be terminated after the driver deployment is torn down (recall
// defer()s are called in reverse order).
func testNamespaceSetup(ctx context.Context, t *testing.T) func() {
	t.Helper()
	testNamespace = fmt.Sprintf("ns-%d", rand.Uint32())
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}
	_, err := K8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	assert.NilError(t, err, "Could not create test namespace %s: %v", testNamespace)
	t.Logf("Using test namespace %s", testNamespace)
	return func() {
		if err := K8sClient.CoreV1().Namespaces().Delete(ctx, testNamespace, metav1.DeleteOptions{}); err != nil {
			t.Errorf("%v Error deleting test namespace %s: %v", time.Now(), testNamespace, err)
		} else {
			t.Logf("%v, tore down namespace/%s; waiting for pods to delete", time.Now(), testNamespace)
			waitFor(ctx, t, func(ctx context.Context) (bool, error) {
				pods, err := K8sClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{})
				assert.NilError(t, err)
				for _, pod := range pods.Items {
					if pod.Status.Phase == corev1.PodFailed {
						// Ignore any error.
						K8sClient.CoreV1().Pods(testNamespace).Delete(ctx, pod.GetName(), metav1.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))})
					}
				}
				return len(pods.Items) == 0, nil
			})
			t.Logf("%v all pods deleted for %s", time.Now(), testNamespace)
		}
	}
}

// verifyRankIndicies checks that pods from the given jobset have jax-init-info.txt
// process indicies from 0 to numWorkers-1, consistent with node pools. In addition,
// any nodes in the pinned map should have the given index.  Returns the actual
// mapping. It assumes the pod has a container named "worker" where the checkpoint
// volume is mounted to /local.
func verifyRankIndicies(ctx context.Context, t *testing.T, job string, numSlices, sliceSize int, pinned map[int]string) map[int]string {
	t.Helper()
	numNodes := numSlices * sliceSize
	var nodeMapping map[int]string
	err := wait.PollUntilContextTimeout(ctx, time.Second, verifyTimeout, true, func(ctx context.Context) (bool, error) {
		pods, err := K8sClient.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		indicies := map[int]string{} // index -> coordinator for that pod.
		pools := map[int]string{}    // index -> node pool for that index.
		coordinator := ""
		nodeMapping = map[int]string{}
		for _, pod := range pods.Items {
			annotations := pod.GetAnnotations()
			if annotations == nil {
				continue
			}
			if annotations["jobset.sigs.k8s.io/jobset-name"] != job {
				continue
			}
			if pod.Spec.NodeName == "" {
				continue
			}
			node, err := K8sClient.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
			if err != nil {
				t.Logf("Error getting node %s for %s: %v", pod.Spec.NodeName, pod.GetName(), err)
				return false, nil // retry
			}
			if node.GetLabels() == nil || node.GetLabels()[nodePoolLabel] == "" {
				return false, fmt.Errorf("No node pool label for %s", node.GetName())
			}
			pool := node.GetAnnotations()[nodePoolLabel]
			t.Logf("%v: Examining Pod %s/%s on %s", time.Now(), pod.GetName(), pod.Status.PodIP, pod.Spec.NodeName)
			info, err := runOnPod(ctx, t, &pod, "worker", "cat", "/local/jax-init-info.txt")
			if err != nil {
				t.Logf("cat of info failed: %v", err)
				return false, nil // retry
			}
			parts := strings.Split(info, "\n")
			if len(parts) != 3 {
				t.Logf("partial info file (retrying): %s", info)
				return false, nil // retry
			}
			idx, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			if err != nil {
				t.Logf("bad index in info (retrying): %s", info)
				return false, nil // retry
			}
			if _, found := indicies[idx]; found {
				t.Logf("duplicate index %d (retrying)", idx)
				return false, nil // retry
			}
			indicies[idx] = strings.TrimSpace(parts[1])
			pools[idx] = pool

			if pinned != nil {
				if pinnedNode, found := pinned[idx]; found {
					if pod.Spec.NodeName != pinnedNode {
						t.Logf("%d on %s but expected %s (retrying)", idx, pod.Spec.NodeName, pinnedNode)
						return false, nil
					}
				}
			}
			if idx == 0 {
				coordinator = pod.Status.PodIP
			}
			nodeMapping[idx] = pod.Spec.NodeName

			t.Logf("%v: Pod %s/%s on %s is gen %s and has info %s", time.Now(), pod.GetName(), pod.Status.PodIP, pod.Spec.NodeName, pod.GetLabels()["jobset.sigs.k8s.io/restart-attempt"], strings.Join(strings.Split(info, "\n"), " "))
		}
		if coordinator == "" {
			t.Logf("coordinator not found, retrying")
			return false, nil
		}
		coordinator += ":8476"
		if len(indicies) != numNodes {
			t.Logf("%d infos found out of %d", len(indicies), numNodes)
			return false, nil
		}
		foundError := false
		for i := 0; i < numNodes; i++ {
			targetCoord, found := indicies[i]
			if !found {
				t.Logf("missing %d", i)
				return false, nil
			}
			t.Logf("%d has coordinator %s, looking for %s", i, targetCoord, coordinator)
			if targetCoord != coordinator {
				t.Logf("coordinator mismatch")
				foundError = true
			}
		}
		if foundError {
			return false, nil
		}

		foundError = false
		for slice := 0; slice < numSlices; slice++ {
			targetPool := pools[slice*sliceSize]
			for i := slice*sliceSize + 1; i < (slice+1)*sliceSize; i++ {
				if pools[i] != targetPool {
					t.Logf("%d should be pool %s but had pool %s", i, targetPool, pools[i])
					foundError = true
				}
			}
		}
		if foundError {
			return false, nil
		}

		t.Logf("verification successful")
		return true, nil
	})
	assert.NilError(t, err)
	return nodeMapping
}

func TestMain(m *testing.M) {
	var err error
	KubeConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		klog.Fatalf("Could not build kubeconfig: %v", err)
	}
	KubeConfig.QPS = 300
	KubeConfig.Burst = 600
	K8sClient, err = kubernetes.NewForConfig(KubeConfig)
	if err != nil {
		klog.Fatalf("Could not get k8s client: %v", err)
	}

	utilruntime.Must(scheme.AddToScheme(scheme.Scheme))
	utilruntime.Must(jobsetv1alpha.AddToScheme(scheme.Scheme))
	utilruntime.Must(checkpointv1.AddToScheme(scheme.Scheme))

	// If the logger is not set, client.New will complain.
	ctrl.SetLogger(zap.New())
	CRClient, err = client.New(KubeConfig, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		klog.Fatalf("can't get controller-runtime client: %v", err)
	}

	os.Exit(m.Run())
}
