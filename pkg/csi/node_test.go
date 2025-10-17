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

package csi

import (
	"testing"

	"gotest.tools/v3/assert"

	"k8s.io/apimachinery/pkg/types"
)

func TestTargetPathUID(t *testing.T) {
	cases := []struct {
		path string
		uid  types.UID
	}{
		{
			path: "/var/lib/kubelet/pods/94710759-e7bb-4fb2-814f-b0e33ce62d6c/volumes/kubernetes.io~csi/ramdisk/mount",
			uid:  types.UID("94710759-e7bb-4fb2-814f-b0e33ce62d6c"),
		},
		{
			path: "/var/lib/kubelet/pods/",
			uid:  types.UID(""),
		},
		{
			path: "/some/path/8495-383ad9/ramdisk/",
			uid:  types.UID(""),
		},
		{
			path: "/var/lib/kubelet/pods/94710759-e7bb-4fb2",
			uid:  types.UID(""),
		},
	}
	for _, tc := range cases {
		uid, err := getUIDFromTarget(tc.path)
		if tc.uid == "" {
			assert.ErrorContains(t, err, "no UID found")
		} else {
			assert.Equal(t, uid, tc.uid, tc.path)
		}
	}
}
