/*
Copyright 2019 The Knative Authors

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

package testing

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-contrib/rocketmq/pkg/apis/messaging/v1alpha1"
	"knative.dev/pkg/apis"
)

// RocketmqChannelOption enables further configuration of a RocketmqChannel.
type RocketmqChannelOption func(*v1alpha1.RocketmqChannel)

// NewRocketmqChannel creates an RocketmqChannel with RocketmqChannelOptions.
func NewRocketmqChannel(name, namespace string, ncopt ...RocketmqChannelOption) *v1alpha1.RocketmqChannel {
	rc := &v1alpha1.RocketmqChannel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1alpha1.RocketmqChannelSpec{},
	}
	for _, opt := range ncopt {
		opt(rc)
	}
	rc.SetDefaults(context.Background())
	return rc
}

func WithInitRocketmqChannelConditions(rc *v1alpha1.RocketmqChannel) {
	rc.Status.InitializeConditions()
}

func WithRocketmqChannelDeleted(rc *v1alpha1.RocketmqChannel) {
	deleteTime := metav1.NewTime(time.Unix(1e9, 0))
	rc.ObjectMeta.SetDeletionTimestamp(&deleteTime)
}

func WithRocketmqChannelTopicReady() RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkTopicTrue()
	}
}

func WithRocketmqChannelConfigReady() RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkConfigTrue()
	}
}

func WithRocketmqChannelDeploymentNotReady(reason, message string) RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkDispatcherFailed(reason, message)
	}
}

func WithRocketmqChannelDeploymentReady() RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.PropagateDispatcherStatus(&appsv1.DeploymentStatus{Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}}})
	}
}

func WithRocketmqChannelServicetNotReady(reason, message string) RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkServiceFailed(reason, message)
	}
}

func WithRocketmqChannelServiceReady() RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkServiceTrue()
	}
}

func WithRocketmqChannelChannelServicetNotReady(reason, message string) RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkChannelServiceFailed(reason, message)
	}
}

func WithRocketmqChannelChannelServiceReady() RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkChannelServiceTrue()
	}
}

func WithRocketmqChannelEndpointsNotReady(reason, message string) RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkEndpointsFailed(reason, message)
	}
}

func WithRocketmqChannelEndpointsReady() RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.MarkEndpointsTrue()
	}
}

func WithRocketmqChannelAddress(a string) RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		rc.Status.SetAddress(&apis.URL{
			Scheme: "http",
			Host:   a,
		})
	}
}

func WithRocketmqFinalizer(finalizerName string) RocketmqChannelOption {
	return func(rc *v1alpha1.RocketmqChannel) {
		finalizers := sets.NewString(rc.Finalizers...)
		finalizers.Insert(finalizerName)
		rc.SetFinalizers(finalizers.List())
	}
}
