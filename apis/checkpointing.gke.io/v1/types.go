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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CheckpointConfiguration is a specification for a CheckpointConfiguration resource.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type CheckpointConfiguration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CheckpointConfigurationSpec `json:"spec"`
}

// CheckpointConfigurationSpec is the spec for a CheckpointConfiguration resource.
type CheckpointConfigurationSpec struct {
	CloudStorageBucketName      string              `json:"cloudStorageBucketName,omitempty" protobuf:"bytes,2,opt,name=cloudStorageBucketName"`
	EnableExternalJaxProcessIds bool                `json:"enableExternalJaxProcessIds,omitempty" protobuf:"bytes,3,opt,name=enableExternalJaxProcessIds"`
	NodeSelector                map[string]string   `json:"nodeSelector,omitempty" protobuf:"bytes,4,opt,name=nodeSelector"`
	Tolerations                 []corev1.Toleration `json:"tolerations,omitempty" protobuf:"bytes,5,rep,name=tolerations"`
	InMemoryVolumeSize          string              `json:"inMemoryVolumeSize,omitempty" protobuf:"bytes,6,rep,name=inMemoryVolumeSize"`
	CsiEphemeralLimit           string              `json:"csiEphemeralLimit,omitempty" protobuf:"bytes,7,rep,name=csiEphemeralLimit"`

	// GcsFuseMountOptions is a comma-separated list of mount options. This is
	// copied directly into the mountOptions for the driver GCS volume.
	GcsFuseMountOptions []string `json:"gcsFuseMountOptions,omitempty" protobuf:"bytes,8,rep,name=gcsFuseMountOptions"`
}

// CheckpointConfigurationSpeceList is a list of CheckpointConfiguration resources.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CheckpointConfigurationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []CheckpointConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}
