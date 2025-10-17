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
	"iter"
	"maps"
	"reflect"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	processIndexKey = "process-index"
	generationKey   = "generation"
	addressKey      = "address"
	jobKey          = "job"
	jobNamespaceKey = "job-namespace"
	podKey          = "pod"
	podUIDKey       = "pod-uid"
)

// MappingInfo describes a node instance. It's taken from a config map named for the node.
// It's also used in-memory in the server.
type MappingInfo struct {
	NodeName     string
	Address      string
	ProcessIndex int
	Generation   int
	Job          types.NamespacedName
	PodName      string
	PodUID       types.UID
}

// CoordinatorInfo describes the coordintor for a job. It's taken from a config map named for the job.
type CoordinatorInfo struct {
	Address    string
	Generation int
	Job        types.NamespacedName
}

// IdFileClient maintains an id file on a node. It looks up the process index
// for this node and updates the local id and coordinator files.
type IdFileClient interface {
	// Fetch the node information for the pod with the given UID. If the current node
	// information has a different UID, then it's stale and will be ignored.
	FetchNodeInformation(context.Context, types.UID) (MappingInfo, CoordinatorInfo, error)
}

// IdFileServer maintains the id mapping from the controller.
type IdFileServer interface {
	// AddNode adds the specified node to the mapping. If a node exists at the
	// same index, it is replaced. If a node with the same name exists, it is
	// updated.
	AddNode(name, pool string, index int) (MappingInfo, error)

	// NodeSlice returns the slice (0..NumSlices()-1) for the named node, or -1
	// if the node is unknown. If other nodes in the same pool are known, but
	// the node is not, then the pool's slice is returned.
	NodeSlice(name, pool string) int

	// FindIndexInSlice looks to add a node in the specified slice. If
	// fallbackIndex is unused and consistent with the slice, it is used . If
	// name is already in the mapping, the existing index is returned (which may
	// or may not be in the given slice).
	//
	// If no index can be found, -1 is returned. An error signifies a deeper
	// problem.
	//
	// fallbackIndex is relative to the slice. That is, if there are 3 workers
	// per slice, fallbackIndex is 1, and slice number 4 is chosen, index 13
	// will be used if available.
	//
	// This is intended to be used with the slice taken from the node pool for
	// the scheduled node of a JobSet pod, as this is the actual hardware slice.
	// fallbackIndex is taken from the JobSet index. When a training job first
	// starts up, the JobSet index will be consistent with the slice. There are
	// no existing indices to collide with, and so the node mapping will be the
	// same as using the JobSet indices.
	//
	// When a pod is recreated after a training failure, the JobSet index may
	// not be consistent with the node the pod is scheduled on. If the
	// fallbackIndex is not even in the right slice, it can't be used.  If it is
	// in the right slice, a different pod may have gotten scheduled to the
	// index within the slice, in which case a free index will be used instead
	// (this assumes that the failed node and pod have already been removed from
	// this mapping).
	//
	// No data structures are changed. AddNode should be used to commit the index.
	FindIndexInSlice(name, pool string, slice, fallbackIndex int) (int, error)

	RemoveNode(name, pool string)
	DeleteNodeMapping(ctx context.Context, nodeName string) error
	GetNode(node string) (MappingInfo, bool)
	Nodes() iter.Seq[string]
	UpdateNode(node, address string, generation int, pod string, podUID types.UID) (MappingInfo, error)
	GetIndex(index int) (MappingInfo, bool)
	GetFreeIndex(slice int) int
	NumSlices() int
	SliceSize() int
	UpdateCoordinatorInfo(context.Context) error
	ResetCoordinator(context.Context) error

	// A string of interesting state for debugging.
	DebugState() string
}

type IdFileClientOpts struct {
	KubeClient *kubernetes.Clientset
	Namespace  string
	NodeName   string
}

type IdFileServerOpts struct {
	KubeClient *kubernetes.Clientset
	Namespace  string
	NumWorkers int
	SliceSize  int
	Job        types.NamespacedName
}

type idFileClient struct {
	opts IdFileClientOpts
}

var _ IdFileClient = &idFileClient{}

type idFileServer struct {
	opts           IdFileServerOpts
	numSlices      int
	mapping        map[int]MappingInfo
	nodeToRankMap  map[string]int
	poolNodeCount  map[string]int
	poolToSliceMap map[string]int
}

var _ IdFileServer = &idFileServer{}

func (svr *idFileServer) DebugState() string {
	return fmt.Sprintf("%+v; node ranks %v; pool counts %v; poolSlices %v", svr.mapping, svr.nodeToRankMap, svr.poolNodeCount, svr.poolToSliceMap)
}

func NewIdFileClient(opts IdFileClientOpts) (IdFileClient, error) {
	return &idFileClient{opts}, nil
}

func (c *idFileClient) FetchNodeInformation(ctx context.Context, uid types.UID) (MappingInfo, CoordinatorInfo, error) {
	// TODO: use an informer. We fetch the configmap many times in case the generation updates.
	var mapping MappingInfo
	var coord CoordinatorInfo
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 60*time.Minute, true, func(ctx context.Context) (bool, error) {
		var err error
		mapping, err = getNodeMapping(ctx, c.opts.KubeClient, c.opts.Namespace, c.opts.NodeName)
		if apierrors.IsNotFound(err) {
			klog.Warningf("node configmap %s/%s not found, retrying", c.opts.Namespace, c.opts.NodeName)
			return false, nil // retry
		} else if err != nil {
			klog.Warningf("error retrieving configmap %s/%s, retrying: %v", c.opts.Namespace, c.opts.NodeName, err)
			return false, nil
		}
		if mapping.PodUID != uid {
			klog.Infof("UID mismtach, current pod is %s, mapping is %s", uid, mapping.PodUID)
			return false, nil // retry
		}
		if mapping.Generation < 0 {
			klog.Infof("mapping generation not updated for %s", c.opts.NodeName)
			return false, nil // retry
		}

		coord, err = getCoordinatorInfo(ctx, c.opts.KubeClient, c.opts.Namespace, mapping.Job)
		if apierrors.IsNotFound(err) {
			klog.Warningf("coordinator configmap for %s/%v not found, retrying", c.opts.Namespace, mapping.Job)
			return false, nil // retry
		}
		if err != nil {
			return false, err
		}
		if coord.Address == "" || coord.Generation != mapping.Generation {
			klog.Infof("address %s is empty or coordinator generation %d does not match target %d, retrying", coord.Address, coord.Generation, mapping.Generation)
			return false, nil // retry
		}
		return true, nil
	}); err != nil {
		return MappingInfo{}, CoordinatorInfo{}, err
	}

	klog.Infof("deleting node mapping %s", c.opts.NodeName)
	// Delete the node mapping so that if a new job with the same name is recreated we
	// don't get stale information.
	if err := c.opts.KubeClient.CoreV1().ConfigMaps(c.opts.Namespace).Delete(ctx, c.opts.NodeName, metav1.DeleteOptions{}); err != nil {
		klog.Errorf("couldn't delete node mapping (ignoring): %v", err)
		// Return success so we don't block this iteration.
	}

	return mapping, coord, nil
}

func getNodeMapping(ctx context.Context, kubeClient *kubernetes.Clientset, namespace, nodeName string) (MappingInfo, error) {
	configMap, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return MappingInfo{}, err
	}
	return parseNodeMapping(configMap)
}

func parseNodeMapping(configMap *corev1.ConfigMap) (MappingInfo, error) {
	mapping := MappingInfo{NodeName: configMap.GetName()}

	jobName := configMap.Data[jobKey]
	jobNamespace, found := configMap.Data[jobNamespaceKey]
	if !found {
		jobNamespace = "default"
	}
	mapping.Job = types.NamespacedName{Namespace: jobNamespace, Name: jobName}

	if address, found := configMap.Data[addressKey]; found {
		mapping.Address = address
	}

	indexStr, found := configMap.Data[processIndexKey]
	if !found {
		return MappingInfo{}, fmt.Errorf("Node mapping not ready, no process index: %s", configMap.GetName())
	}
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return MappingInfo{}, fmt.Errorf("bad process index %s in node mapping for %s: %w", indexStr, configMap.GetName(), err)
	}
	mapping.ProcessIndex = index

	generation := -1
	genStr, found := configMap.Data[generationKey]
	if found {
		i, err := strconv.Atoi(genStr)
		if err != nil {
			return MappingInfo{}, fmt.Errorf("bad generation %s in node mapping for %s: %w", genStr, configMap.GetName(), err)
		}
		generation = i
	}
	mapping.Generation = generation

	mapping.PodName = configMap.Data[podKey]
	mapping.PodUID = types.UID(configMap.Data[podUIDKey])

	return mapping, nil
}

func NewIdFileServer(opts IdFileServerOpts) (IdFileServer, error) {
	if opts.NumWorkers < 1 {
		return nil, fmt.Errorf("empty worker count in %v", opts)
	}
	numSlices := opts.NumWorkers / opts.SliceSize
	if numSlices < 1 {
		return nil, fmt.Errorf("invalid worker or slice count in %v", opts)
	}

	return &idFileServer{opts, numSlices, map[int]MappingInfo{}, map[string]int{}, map[string]int{}, map[string]int{}}, nil
}

func (s *idFileServer) AddNode(name, pool string, index int) (MappingInfo, error) {
	// consistency check
	for n, i := range s.nodeToRankMap {
		if i == index {
			return MappingInfo{}, fmt.Errorf("when adding node %s, node %s already at index %d. Should wait for old node to be deleted first", name, n, i)
		}
	}

	slice := index / s.opts.SliceSize
	if slice < 0 || slice >= s.numSlices {
		return MappingInfo{}, fmt.Errorf("bad index %d for %s", index, name)
	}
	poolSlice, found := s.poolToSliceMap[pool]
	if found && poolSlice != slice {
		return MappingInfo{}, fmt.Errorf("tried to add node %s in slice %d but pool %s is slice %d", name, slice, pool, poolSlice)
	}
	for p, s := range s.poolToSliceMap {
		if p != pool && s == slice {
			return MappingInfo{}, fmt.Errorf("slice collision: %s and %s both in slice %d", pool, p, s)
		}
	}
	if _, found := s.mapping[index]; found {
		delete(s.nodeToRankMap, name)
	}
	delete(s.poolToSliceMap, pool)
	if cnt, found := s.poolNodeCount[name]; found {
		s.poolNodeCount[name] = cnt - 1
	}
	if oldIndex, found := s.nodeToRankMap[name]; found {
		delete(s.mapping, oldIndex)
	}

	info := MappingInfo{NodeName: name, ProcessIndex: index, Job: s.opts.Job, Generation: -1}
	s.mapping[index] = info
	s.nodeToRankMap[name] = index
	s.poolNodeCount[pool] = s.poolNodeCount[pool] + 1
	s.poolToSliceMap[pool] = slice

	return info, nil
}

func (s *idFileServer) NodeSlice(name, pool string) int {
	if slice, found := s.poolToSliceMap[pool]; found {
		return slice
	}
	// consistency check.
	if rank, found := s.nodeToRankMap[name]; found {
		klog.Errorf("inconsistency: pool %s not in map, but %s has rank %d", pool, name, rank)
		return -1
	}
	return -1
}

func (s *idFileServer) FindIndexInSlice(name, pool string, slice, fallbackIndex int) (int, error) {
	if rank, found := s.nodeToRankMap[name]; found {
		// Do a consistency check.
		_, found := s.mapping[rank]
		if !found {
			return -1, fmt.Errorf("inconsistency: %d in node map for %s but not info found!", rank, name)
		}
		return rank, nil
	}
	if slice < 0 || slice >= s.numSlices {
		return -1, fmt.Errorf("bad slice %d(%d) for %s", slice, fallbackIndex, name)
	}
	if currSlice, found := s.poolToSliceMap[pool]; found && currSlice != slice {
		klog.Infof("avoided slice collision for %s between %d and %d", pool, slice, currSlice)
		return -1, nil
	}
	for currPool, currSlice := range s.poolToSliceMap {
		if currSlice == slice && currPool != pool {
			klog.Infof("avoided assigning new pool %s to same slice as %s", pool, currPool)
			return -1, nil
		}
	}

	if fallbackIndex >= 0 && fallbackIndex < s.opts.SliceSize {
		candidate := slice*s.opts.SliceSize + fallbackIndex
		if _, found := s.mapping[candidate]; !found {
			return candidate, nil
		}
	}
	return s.GetFreeIndex(slice), nil
}

func (s *idFileServer) RemoveNode(name, pool string) {
	if index, found := s.nodeToRankMap[name]; found {
		delete(s.mapping, index)
		delete(s.nodeToRankMap, name)
		if s.poolNodeCount[pool] <= 1 {
			klog.Infof("pool %s released", pool)
			delete(s.poolNodeCount, pool)
			delete(s.poolToSliceMap, pool)
		} else {
			s.poolNodeCount[pool]--
		}
	}
}

func (s *idFileServer) GetNode(name string) (MappingInfo, bool) {
	if index, found := s.nodeToRankMap[name]; found {
		info, found := s.mapping[index]
		return info, found
	}
	return MappingInfo{}, false
}

func (s *idFileServer) Nodes() iter.Seq[string] {
	return maps.Keys(s.nodeToRankMap)
}

func (s *idFileServer) UpdateNode(node, address string, generation int, pod string, podUID types.UID) (MappingInfo, error) {
	index, found := s.nodeToRankMap[node]
	if !found {
		return MappingInfo{}, fmt.Errorf("node %s not found for address update to %s", node, address)
	}
	info, found := s.mapping[index]
	if !found {
		return MappingInfo{}, fmt.Errorf("internal error! node %s at %d has no info when updating address %s", node, index, address)
	}
	info.Address = address
	info.Generation = generation
	info.PodName = pod
	info.PodUID = podUID
	s.mapping[index] = info
	return info, nil
}

func (s *idFileServer) GetIndex(index int) (MappingInfo, bool) {
	info, ok := s.mapping[index]
	return info, ok
}

func (s *idFileServer) GetFreeIndex(slice int) int {
	if slice < 0 || slice >= s.numSlices {
		return -1
	}
	for i := slice * s.opts.SliceSize; i < (slice+1)*(s.opts.SliceSize); i++ {
		if _, found := s.mapping[i]; !found {
			return i
		}
	}
	return -1
}

func (s *idFileServer) SliceSize() int {
	return s.opts.SliceSize
}

func (s *idFileServer) NumSlices() int {
	return s.numSlices
}

func (s *idFileServer) UpdateCoordinatorInfo(ctx context.Context) error {
	coordinator, found := s.mapping[0]
	if !found || coordinator.Address == "" || coordinator.Generation < 0 {
		klog.Infof("no coordinator: %t %s %d", found, coordinator.Address, coordinator.Generation)
		return nil // Nothing to update
	}

	needCreate := false
	info, err := getCoordinatorInfo(ctx, s.opts.KubeClient, s.opts.Namespace, s.opts.Job)
	if apierrors.IsNotFound(err) {
		needCreate = true
	} else if err != nil {
		return err
	}

	if coordinator.Address == info.Address && coordinator.Generation == info.Generation {
		// Coordinator info is already updated, nothing further to do.
		klog.Infof("coordinator already updated: %v", info)
		return nil
	}

	var configMap corev1.ConfigMap
	configMap.SetName(jobsetMappingName(s.opts.Job))
	configMap.SetNamespace(s.opts.Namespace)
	configMap.Data = make(map[string]string)

	info.Address = coordinator.Address
	info.Generation = coordinator.Generation

	configMap.Data[addressKey] = info.Address
	configMap.Data[generationKey] = strconv.Itoa(info.Generation)

	if needCreate {
		if _, err := s.opts.KubeClient.CoreV1().ConfigMaps(s.opts.Namespace).Create(ctx, &configMap, metav1.CreateOptions{}); err != nil {
			return err
		}
	} else {
		_, err = s.opts.KubeClient.CoreV1().ConfigMaps(s.opts.Namespace).Update(ctx, &configMap, metav1.UpdateOptions{})
		if err != nil {
			// There should not be any update errors since we are the only writer, so no retry is done.
			return err
		}
	}
	return nil
}

func (s *idFileServer) ResetCoordinator(ctx context.Context) error {
	klog.Infof("Resetting coordinator for %s", jobsetMappingName(s.opts.Job))
	err := s.opts.KubeClient.CoreV1().ConfigMaps(s.opts.Namespace).Delete(ctx, jobsetMappingName(s.opts.Job), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (s *idFileServer) DeleteNodeMapping(ctx context.Context, nodeName string) error {
	klog.Infof("Deleting node mapping for %s with %s", nodeName, jobsetMappingName(s.opts.Job))
	if err := s.opts.KubeClient.CoreV1().ConfigMaps(s.opts.Namespace).Delete(ctx, nodeName, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func writeNodeMapping(ctx context.Context, kubeClient *kubernetes.Clientset, namespace string, mapping MappingInfo) error {
	start := time.Now()
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 20*time.Minute, true, func(ctx context.Context) (bool, error) {
		var err error
		configMap, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, mapping.NodeName, metav1.GetOptions{})
		needCreate := false
		if apierrors.IsNotFound(err) {
			configMap.SetName(mapping.NodeName)
			configMap.SetNamespace(namespace)
			needCreate = true
		} else if err != nil {
			return false, err
		}
		newData := map[string]string{}
		newData[jobKey] = mapping.Job.Name
		newData[jobNamespaceKey] = mapping.Job.Namespace
		newData[addressKey] = mapping.Address
		newData[generationKey] = strconv.Itoa(mapping.Generation)
		newData[processIndexKey] = strconv.Itoa(mapping.ProcessIndex)
		newData[podKey] = mapping.PodName
		newData[podUIDKey] = string(mapping.PodUID)

		if reflect.DeepEqual(configMap.Data, newData) {
			// Nothing to update
			return true, nil
		}

		configMap.Data = newData
		if needCreate {
			_, err := kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, configMap, metav1.CreateOptions{})
			if err != nil {
				klog.Warningf("Failed to create configmap %s, retrying: %v", configMap.GetName(), err)
				return false, nil
			}
		} else {
			_, err := kubeClient.CoreV1().ConfigMaps(namespace).Update(ctx, configMap, metav1.UpdateOptions{})
			if err != nil {
				klog.Warningf("Failed to update configmap %s, retrying: %v", configMap.GetName(), err)
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	klog.Infof("Wrote node mapping %+v: %v", mapping, time.Now().Sub(start))
	return nil
}

func getCoordinatorInfo(ctx context.Context, kubeClient *kubernetes.Clientset, namespace string, job types.NamespacedName) (CoordinatorInfo, error) {
	var err error
	configMap, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, jobsetMappingName(job), metav1.GetOptions{})
	if err != nil {
		return CoordinatorInfo{}, err
	}
	return parseCoordinatorInfo(configMap, job)
}

func parseCoordinatorInfo(configMap *corev1.ConfigMap, job types.NamespacedName) (CoordinatorInfo, error) {
	address, found := configMap.Data[addressKey]
	if !found {
		address = ""
	}
	generation := -1
	genStr, found := configMap.Data[generationKey]
	if found {
		i, err := strconv.Atoi(genStr)
		if err != nil {
			return CoordinatorInfo{}, fmt.Errorf("bad generation %s in coordinator info for %s: %w", genStr, job, err)
		}
		generation = i
	}

	return CoordinatorInfo{Address: address, Generation: generation, Job: job}, nil
}

// jobsetMappingName turns a namespaced name into a name that can be use for a resource. It is not
// reversible, as the separator may appear in the namespace or job name.
func jobsetMappingName(jobset types.NamespacedName) string {
	if jobset.Name == "" {
		panic("")
	}
	namespace := jobset.Namespace
	if namespace == "" {
		namespace = "default"
	}
	return fmt.Sprintf("%s-%s", namespace, jobset.Name)
}
