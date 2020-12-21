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

package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/admin"

	"go.uber.org/zap"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"

	eventingClient "knative.dev/eventing/pkg/client/injection/client"
	"knative.dev/eventing/pkg/utils"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-contrib/rocketmq/pkg/apis/messaging/v1alpha1"
	fakerocketmqclient "knative.dev/eventing-contrib/rocketmq/pkg/client/injection/client/fake"
	"knative.dev/eventing-contrib/rocketmq/pkg/client/injection/reconciler/messaging/v1alpha1/rocketmqchannel"
	"knative.dev/eventing-contrib/rocketmq/pkg/reconciler/controller/resources"
	reconcilerocketmqtesting "knative.dev/eventing-contrib/rocketmq/pkg/reconciler/testing"
	reconcilertesting "knative.dev/eventing-contrib/rocketmq/pkg/reconciler/testing"
	. "knative.dev/eventing-contrib/rocketmq/pkg/utils"
)

const (
	testNS                = "test-namespace"
	rcName                = "test-rc"
	testDispatcherImage   = "test-image"
	channelServiceAddress = "test-rc-rn-channel.test-namespace.svc.cluster.local"
	brokerName            = "172.17.0.1:10911"
	finalizerName         = "rocketmqchannels.messaging.knative.dev"
)

var (
	finalizerUpdatedEvent = Eventf(corev1.EventTypeNormal, "FinalizerUpdate", `Updated "test-rc" finalizers`)
	testAdmin             admin.Admin
)

func init() {
	// Add types to scheme
	_ = v1alpha1.AddToScheme(scheme.Scheme)
	_ = duckv1.AddToScheme(scheme.Scheme)
}

func TestAllCases(t *testing.T) {
	rcKey := testNS + "/" + rcName
	table := TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		}, {
			Name: "key not found",
			// Make sure Reconcile handles good keys that don't exist.
			Key: "foo/not-found",
		}, {
			Name: "deployment does not exist, automatically created and patching finalizers",
			Key:  rcKey,
			Objects: []runtime.Object{
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady()),
			},
			WantErr: true,
			WantCreates: []runtime.Object{
				makeDeployment(),
				makeService(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsNotReady("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist")),
			}},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(testNS, rcName),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(corev1.EventTypeNormal, dispatcherDeploymentCreated, "Dispatcher deployment created"),
				Eventf(corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created"),
				Eventf(corev1.EventTypeWarning, "InternalError", `endpoints "rocketmq-ch-dispatcher" not found`),
			},
		}, {
			Name: "Service does not exist, automatically created",
			Key:  rcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName)),
			},
			WantErr: true,
			WantCreates: []runtime.Object{
				makeService(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName),
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelDeploymentReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsNotReady("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist")),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created"),
				Eventf(corev1.EventTypeWarning, "InternalError", `endpoints "rocketmq-ch-dispatcher" not found`),
			},
		}, {
			Name: "Endpoints does not exist",
			Key:  rcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName)),
			},
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName),
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelDeploymentReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsNotReady("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", `endpoints "rocketmq-ch-dispatcher" not found`),
			},
		}, {
			Name: "Endpoints not ready",
			Key:  rcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeEmptyEndpoints(),
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName)),
			},
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName),
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelDeploymentReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsNotReady("DispatcherEndpointsNotReady", "There are no endpoints ready for Dispatcher service"),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", `there are no endpoints ready for Dispatcher service rocketmq-ch-dispatcher`),
			},
		}, {
			Name: "Works, creates new channel",
			Key:  rcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName)),
			},
			WantErr: false,
			WantCreates: []runtime.Object{
				makeChannelService(reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS)),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName),
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelDeploymentReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsReady(),
					reconcilerocketmqtesting.WithRocketmqChannelChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelAddress(channelServiceAddress),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, "RocketmqChannelReconciled", `RocketmqChannel reconciled: "test-namespace/test-rc"`),
			},
		}, {
			Name: "Works, channel exists",
			Key:  rcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName)),
				makeChannelService(reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS)),
			},
			WantErr: false,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName),
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelDeploymentReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsReady(),
					reconcilerocketmqtesting.WithRocketmqChannelChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelAddress(channelServiceAddress),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, "RocketmqChannelReconciled", `RocketmqChannel reconciled: "test-namespace/test-rc"`),
			},
		}, {
			Name: "channel exists, not owned by us",
			Key:  rcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName)),
				makeChannelServiceNotOwnedByUs(),
			},
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName),
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelDeploymentReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsReady(),
					reconcilerocketmqtesting.WithRocketmqChannelChannelServicetNotReady("ChannelServiceFailed", "Channel Service failed: rocketmqchannel: test-namespace/test-rc does not own Service: \"test-rc-rn-channel\""),
				),
			}},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", `rocketmqchannel: test-namespace/test-rc does not own Service: "test-rc-rn-channel"`),
			},
		}, {
			Name: "channel does not exist, fails to create",
			Key:  rcKey,
			Objects: []runtime.Object{
				makeReadyDeployment(),
				makeService(),
				makeReadyEndpoints(),
				reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName)),
			},
			WantErr: true,
			WithReactors: []clientgotesting.ReactionFunc{
				InduceFailure("create", "Services"),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS,
					reconcilerocketmqtesting.WithInitRocketmqChannelConditions,
					reconcilerocketmqtesting.WithRocketmqFinalizer(finalizerName),
					reconcilerocketmqtesting.WithRocketmqChannelConfigReady(),
					reconcilerocketmqtesting.WithRocketmqChannelTopicReady(),
					reconcilerocketmqtesting.WithRocketmqChannelDeploymentReady(),
					reconcilerocketmqtesting.WithRocketmqChannelServiceReady(),
					reconcilerocketmqtesting.WithRocketmqChannelEndpointsReady(),
					reconcilerocketmqtesting.WithRocketmqChannelChannelServicetNotReady("ChannelServiceFailed", "Channel Service failed: inducing failure for create services"),
				),
			}},
			WantCreates: []runtime.Object{
				makeChannelService(reconcilerocketmqtesting.NewRocketmqChannel(rcName, testNS)),
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", "inducing failure for create services"),
			},
			// TODO add UTs for topic creation and deletion.
		},
	}
	defer logtesting.ClearAll()

	table.Test(t, reconcilertesting.MakeFactory(func(ctx context.Context, listers *reconcilerocketmqtesting.Listers, cmw configmap.Watcher) controller.Reconciler {

		r := &Reconciler{
			systemNamespace: testNS,
			dispatcherImage: testDispatcherImage,
			rocketmqConfig: &RocketmqConfig{
				Brokers: []string{brokerName},
			},
			rocketmqchannelLister: listers.GetRocketmqChannelLister(),
			// TODO fix
			rocketmqchannelInformer: nil,
			deploymentLister:        listers.GetDeploymentLister(),
			serviceLister:           listers.GetServiceLister(),
			endpointsLister:         listers.GetEndpointsLister(),
			//rocketmqClusterAdmin:    &mockAdmin{},
			rocketmqClusterAdmin: createClient(t),
			rocketmqClientSet:    fakerocketmqclient.Get(ctx),
			KubeClientSet:        kubeclient.Get(ctx),
			EventingClientSet:    eventingClient.Get(ctx),
		}
		return rocketmqchannel.NewReconciler(ctx, logging.FromContext(ctx), r.rocketmqClientSet, listers.GetRocketmqChannelLister(), controller.GetEventRecorder(ctx), r)
	}, zap.L()))
}

/*
type mockAdmin struct{}

func (ma *mockAdmin) CreateTopic(ctx context.Context, opts ...admin.OptionCreate) error {
	return nil
}

func (ma *mockAdmin) Close() error {
	return nil
}

func (ma *mockAdmin) DeleteTopic(ctx context.Context, opts ...admin.OptionDelete) error {
	return nil

var _ admin.Admin = (*mockAdmin)(nil)
}
*/
// Use real client to test
func createClient(t *testing.T) admin.Admin {
	if testAdmin == nil {
		var err error
		testAdmin, err = resources.MakeClient(controllerAgentName, []string{brokerName})
		if err != nil {
			t.Errorf("Create Rocketmq client Error: %v", err)
		}
	}
	return testAdmin
}

func makeDeploymentWithImage(image string) *appsv1.Deployment {
	return resources.MakeDispatcher(resources.DispatcherArgs{
		DispatcherNamespace: testNS,
		Image:               image,
	})
}

func makeDeployment() *appsv1.Deployment {
	return makeDeploymentWithImage(testDispatcherImage)
}

func makeReadyDeployment() *appsv1.Deployment {
	d := makeDeployment()
	d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}}
	return d
}

func makeService() *corev1.Service {
	return resources.MakeDispatcherService(testNS)
}

func makeChannelService(nc *v1alpha1.RocketmqChannel) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      fmt.Sprintf("%s-rn-channel", rcName),
			Labels: map[string]string{
				resources.MessagingRoleLabel: resources.MessagingRole,
			},
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(nc),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: fmt.Sprintf("%s.%s.svc.%s", dispatcherName, testNS, utils.GetClusterDomainName()),
		},
	}
}

func makeChannelServiceNotOwnedByUs() *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      fmt.Sprintf("%s-rn-channel", rcName),
			Labels: map[string]string{
				resources.MessagingRoleLabel: resources.MessagingRole,
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: fmt.Sprintf("%s.%s.svc.%s", dispatcherName, testNS, utils.GetClusterDomainName()),
		},
	}
}

func makeEmptyEndpoints() *corev1.Endpoints {
	return &corev1.Endpoints{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Endpoints",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      dispatcherName,
		},
	}
}

func makeReadyEndpoints() *corev1.Endpoints {
	e := makeEmptyEndpoints()
	e.Subsets = []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "1.1.1.1"}}}}
	return e
}

func patchFinalizers(namespace, name string) clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = name
	action.Namespace = namespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
