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
	"testing"

	"gotest.tools/v3/assert"
)

func TestAssignerInitial3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", -1, 0)
	a.addNode("n1", "p0", -1, 1)
	a.addNode("n2", "p1", -1, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", -1, 4)
	a.addNode("n5", "p2", -1, 5)

	asg, err := a.extendFromInitialRanks()
	assert.NilError(t, err)

	assert.Equal(t, asg[0].name, "n0")
	assert.Equal(t, asg[1].name, "n1")
	assert.Equal(t, asg[2].name, "n2")
	assert.Equal(t, asg[3].name, "n3")
	assert.Equal(t, asg[4].name, "n4")
	assert.Equal(t, asg[5].name, "n5")
}

func TestAssignerExtend3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", -1, 0)
	a.addNode("n1", "p0", -1, 1)
	a.addNode("n2", "p1", 2, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", 4, 4)
	a.addNode("n5", "p2", -1, 5)

	asg, err := a.extendFromInitialRanks()
	assert.NilError(t, err)

	assert.Equal(t, asg[0].name, "n0")
	assert.Equal(t, asg[1].name, "n1")
	assert.Equal(t, asg[2].name, "n2")
	assert.Equal(t, asg[3].name, "n3")
	assert.Equal(t, asg[4].name, "n4")
	assert.Equal(t, asg[5].name, "n5")
}

func TestAssignerExtendWithSwap3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", 1, 0)
	a.addNode("n1", "p0", -1, 1)
	a.addNode("n2", "p1", -1, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", -1, 4)
	a.addNode("n5", "p2", -1, 5)

	_, err := a.extendFromInitialRanks()
	assert.ErrorContains(t, err, "inconsistent")

	_, err = a.extendFromCurrentRank()
	assert.NilError(t, err)
	// The assignment depends on hash ordering so we rely on the verification.
}

func TestAssignerOneFailureFromInitial3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", 0, 0)
	a.addNode("n1", "p0", 1, 1)
	a.addNode("n2", "p1", 2, 2)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", 4, 4)
	a.addNode("n5", "p2", 5, 5)

	_, err := a.extendFromInitialRanks()
	assert.NilError(t, err)
}

func TestAssignerOneFailureBadInitial3x2(t *testing.T) {
	a := newAssigner(3, 2)
	assert.Assert(t, a != nil)
	a.addNode("n0", "p0", 0, 1)
	a.addNode("n1", "p0", 1, 2)
	a.addNode("n2", "p1", 2, 4)
	a.addNode("n3", "p1", -1, 3)
	a.addNode("n4", "p2", 4, 5)
	a.addNode("n5", "p2", 5, 0)

	_, err := a.extendFromInitialRanks()
	assert.ErrorContains(t, err, "inconsistent")

	_, err = a.extendFromCurrentRank()
	assert.NilError(t, err)
}

func runManyInitialTest(t *testing.T, slices, sizes []int) {
	runner := func(t *testing.T, numSlices, sliceSize int) {
		t.Parallel()
		a := newAssigner(numSlices, sliceSize)
		for s := 0; s < numSlices; s++ {
			for k := 0; k < sliceSize; k++ {
				i := s*sliceSize + k
				a.addNode(fmt.Sprintf("n%04d", i), fmt.Sprintf("p%04d", s), -1, i)
			}
		}
		asg, err := a.extendFromInitialRanks()
		assert.NilError(t, err)
		for s := 0; s < numSlices; s++ {
			for k := 0; k < sliceSize; k++ {
				i := s*sliceSize + k
				node := fmt.Sprintf("n%04d", i)
				assert.Equal(t, asg[i].name, node, node)
			}
		}
	}

	for _, numSlices := range slices {
		for _, sliceSize := range sizes {
			i := numSlices
			j := sliceSize
			t.Run(fmt.Sprintf("%d-%d", numSlices, sliceSize), func(t *testing.T) { runner(t, i, j) })
		}
	}
}

func TestAssignerManySmallInitial(t *testing.T) {
	runManyInitialTest(t, []int{1, 2, 3, 4, 5, 6, 10}, []int{1, 2, 3, 4, 8, 16})
}

func TestAssignerManyLargeInitial(t *testing.T) {
	// Don't test beyond 1M nodes, which is already unrealistic.
	runManyInitialTest(t, []int{128, 256, 768, 1024}, []int{32, 128, 512, 1024})
}

func TestAssignerManyUnbalancedInitial(t *testing.T) {
	runManyInitialTest(t, []int{128, 256, 512, 1024, 2048}, []int{1, 2, 8, 16})
	runManyInitialTest(t, []int{1, 2, 4, 8, 16}, []int{128, 256, 512, 1024, 2048})
}

func runManyCurrentTest(t *testing.T, slices, sizes []int) {
	runner := func(t *testing.T, numSlices, sliceSize, numCurrent int) {
		t.Parallel()
		a := newAssigner(numSlices, sliceSize)
		numJobs := numSlices * sliceSize
		for s := 0; s < numSlices; s++ {
			for k := 0; k < sliceSize; k++ {
				i := s*sliceSize + k
				rank := -1
				if numCurrent > 0 && (i+1)%numCurrent == 0 {
					rank = i
				}
				// Note the initial index is forced to not match any currentRank, and if the number
				// of slices & slice size is greater than one, will have an invalid pool assignment.
				a.addNode(fmt.Sprintf("n%04d", i), fmt.Sprintf("p%04d", s), rank, (i+1)%numJobs)
			}
		}
		_, err := a.extendFromInitialRanks()
		if numCurrent > 0 || (numSlices > 1 && sliceSize > 1) {
			if !(numCurrent == 1 && numSlices == 1 && sliceSize == 1) {
				assert.Assert(t, err != nil)
			}
		}
		_, err = a.extendFromCurrentRank()
		assert.NilError(t, err)
	}

	for _, numSlices := range slices {
		for _, sliceSize := range sizes {
			n := numSlices * sliceSize
			used := map[int]bool{}
			for _, numCurrent := range []int{0, 1, n / 4, n / 2, 3 * n / 4, n - 5, n - 2, n - 1} {
				if numCurrent < 0 {
					continue
				}
				if _, found := used[numCurrent]; found {
					continue
				}
				used[numCurrent] = true
				i := numSlices
				j := sliceSize
				k := numCurrent
				t.Run(fmt.Sprintf("%d-%d-%d", i, j, k), func(t *testing.T) { runner(t, i, j, k) })
			}
		}
	}
}

func TestAssignerManySmallExtend(t *testing.T) {
	runManyCurrentTest(t, []int{1, 2, 3, 4, 5, 6, 10}, []int{1, 2, 3, 4, 8, 16})
}

func TestAssignerManyLargeExtend(t *testing.T) {
	// Don't test beyond 1M nodes, which is already unrealistic.
	runManyCurrentTest(t, []int{128, 256, 768, 1024}, []int{32, 128, 512, 1024})
}

func TestAssignerManyUnbalancedExtend(t *testing.T) {
	runManyCurrentTest(t, []int{128, 256, 512, 1024, 2048}, []int{1, 2, 8, 16})
	runManyCurrentTest(t, []int{1, 2, 4, 8, 16}, []int{128, 256, 512, 1024, 2048})
}
