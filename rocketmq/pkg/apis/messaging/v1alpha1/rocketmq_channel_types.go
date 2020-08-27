/*
Copyright 2019 The Knative Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"
)

// +genclient
// +genreconciler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RocketmqChannel is a resource representing a RocketMQ Channel.
type RocketmqChannel struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the Channel.
	Spec RocketmqChannelSpec `json:"spec,omitempty"`

	// Status represents the current state of the RocketmqChannel. This data may be out of
	// date.
	// +optional
	Status RocketmqChannelStatus `json:"status,omitempty"`
}

var (
	// Check that this channel can be validated and defaulted.
	_ apis.Validatable = (*RocketmqChannel)(nil)
	_ apis.Defaultable = (*RocketmqChannel)(nil)

	_ runtime.Object = (*RocketmqChannel)(nil)

	// Check that we can create OwnerReferences to an this channel.
	_ kmeta.OwnerRefable = (*RocketmqChannel)(nil)

	// Check that the type conforms to the duck Knative Resource shape.
	_ duckv1.KRShaped = (*RocketmqChannel)(nil)
)

// RocketmqChannelSpec defines the specification for a RocketmqChannel.
type RocketmqChannelSpec struct {
	// Channel conforms to Duck type Channelable.
	eventingduck.ChannelableSpec `json:",inline"`
}

// RocketmqChannelStatus represents the current state of a RocketmqChannel.
type RocketmqChannelStatus struct {
	// Channel conforms to Duck type Channelable.
	eventingduck.ChannelableStatus `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RocketmqChannelList is a collection of RocketmqChannels.
type RocketmqChannelList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RocketmqChannel `json:"items"`
}

// GetGroupVersionKind returns GroupVersionKind for RocketmqChannels
func (*RocketmqChannel) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("RocketmqChannel")
}

// GetStatus retrieves the duck status for this resource. Implements the KRShaped interface.
func (n *RocketmqChannel) GetStatus() *duckv1.Status {
	return &n.Status.Status
}
