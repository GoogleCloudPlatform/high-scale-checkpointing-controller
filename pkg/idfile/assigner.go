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
	"fmt"
	"sort"
	"strings"

	"k8s.io/klog/v2"
)

// assigner extends a rank assignment to all given nodes.
type assigner struct {
	numWorkers int
	sliceSize  int
	numSlices  int

	nodes map[string]*assignerNode
}

type assignerNode struct {
	name        string
	pool        string
	currentRank int
	initialRank int
}

func newAssigner(numSlices, sliceSize int) *assigner {
	numWorkers := numSlices * sliceSize
	return &assigner{
		numWorkers: numWorkers,
		sliceSize:  sliceSize,
		numSlices:  numSlices,
		nodes:      map[string]*assignerNode{},
	}
}

func (a *assigner) dump() string {
	nodes := []string{}
	for n := range a.nodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	strs := []string{}
	for _, n := range nodes {
		nn := a.nodes[n]
		strs = append(strs, fmt.Sprintf("%s/%s %d (%d)", nn.name, nn.pool, nn.currentRank, nn.initialRank))
	}
	return strings.Join(strs, "\n")
}

func (a *assigner) addNode(name, pool string, currentRank, initialRank int) error {
	if currentRank >= a.numWorkers {
		klog.Warningf("Node %s current rank out of range: %d out of %d. Resetting", name, currentRank, a.numWorkers)
		currentRank = -1
	}
	if initialRank >= a.numWorkers {
		klog.Warningf("Node %s initial rank out of range: %d out of %d. Resetting", name, initialRank, a.numWorkers)
		initialRank = -1
	}
	node := &assignerNode{
		name:        name,
		pool:        pool,
		currentRank: currentRank,
		initialRank: initialRank,
	}
	a.nodes[node.name] = node
	return nil
}

func (a *assigner) clearCurrentRanks() {
	for _, node := range a.nodes {
		node.currentRank = -1
	}
}

func (a *assigner) extendFromInitialRanks() ([]*assignerNode, error) {
	rankAssignment := make([]*assignerNode, a.numWorkers)
	for _, node := range a.nodes {
		if node.currentRank >= 0 && node.initialRank >= 0 && node.currentRank != node.initialRank {
			return nil, fmt.Errorf("inconsistent initial ranks")
		}
		if node.currentRank >= 0 {
			rankAssignment[node.currentRank] = node
		} else if node.initialRank >= 0 {
			rankAssignment[node.initialRank] = node
		}
	}
	return a.verifyAssignment(rankAssignment)
}

func (a *assigner) extendFromCurrentRank() ([]*assignerNode, error) {
	slicePool := make([]string, a.numSlices)
	unusedPools := map[string]bool{}
	for _, node := range a.nodes {
		unusedPools[node.pool] = true
	}
	availableInPool := make(map[string][]*assignerNode)
	rankAssignment := make([]*assignerNode, a.numWorkers)
	for _, node := range a.nodes {
		if node.currentRank >= 0 {
			slice := node.currentRank / a.sliceSize
			if slicePool[slice] != "" && slicePool[slice] != node.pool {
				return nil, fmt.Errorf("inconsistent pools for current rank %d/%d/%d node %s %s vs %s", slice, node.currentRank%a.sliceSize, node.currentRank, node.name, slicePool[slice], node.pool)
			}
			slicePool[slice] = node.pool
			delete(unusedPools, node.pool)
			rankAssignment[node.currentRank] = node
		} else {
			availableInPool[node.pool] = append(availableInPool[node.pool], node)
		}
	}

	for i := 0; i < a.numWorkers; i++ {
		if rankAssignment[i] != nil {
			continue
		}
		slice := i / a.sliceSize
		if slicePool[slice] == "" {
			var pool string
			for pool = range unusedPools {
				break
			}
			if pool == "" {
				return nil, fmt.Errorf("exhausted pools")
			}
			slicePool[slice] = pool
			delete(unusedPools, pool)
		}
		pool := slicePool[slice]
		num := len(availableInPool[pool])
		if num == 0 {
			return nil, fmt.Errorf("exhausted pool %s", pool)
		}
		node := availableInPool[pool][num-1]
		availableInPool[pool] = availableInPool[pool][:num-1]
		rankAssignment[i] = node
	}

	return a.verifyAssignment(rankAssignment)
}

func (a *assigner) existingAssignment() []*assignerNode {
	if len(a.nodes) != a.numWorkers {
		return nil
	}
	rankAssignment := make([]*assignerNode, a.numWorkers)
	for _, node := range a.nodes {
		if node.currentRank >= 0 && node.currentRank < a.numWorkers {
			rankAssignment[node.currentRank] = node
		}
	}
	if _, err := a.verifyAssignment(rankAssignment); err == nil {
		return rankAssignment
	}
	return nil
}

func (a *assigner) verifyAssignment(rankAssignment []*assignerNode) ([]*assignerNode, error) {
	mkerr := func(msg string, args ...any) error {
		nodeStr := []string{}
		for _, n := range rankAssignment {
			if n != nil {
				nodeStr = append(nodeStr, fmt.Sprintf("%+v", *n))
			} else {
				nodeStr = append(nodeStr, "<nil>")
			}
		}
		return fmt.Errorf("%s: %s", fmt.Sprintf(msg, args...), strings.Join(nodeStr, " "))
	}

	if len(rankAssignment) != a.numWorkers {
		return nil, mkerr("incomplete assignment (%d / %d)", len(rankAssignment), a.numWorkers)
	}

	slicePool := make([]string, a.numWorkers)
	for i, node := range rankAssignment {
		if node == nil {
			return nil, mkerr("no assignment for %d", i)
		}
		slice := i / a.sliceSize
		if slicePool[slice] == "" {
			slicePool[slice] = node.pool
		} else if node.pool != slicePool[slice] {
			return nil, mkerr("pool mismatch at %d: %s vs %s", i, node.pool, slicePool[slice])
		}
	}

	// Success!
	return rankAssignment, nil
}
