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
	"reflect"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/channel/fanout"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	pkgreconciler "knative.dev/pkg/reconciler"

	"knative.dev/eventing-contrib/rocketmq/pkg/apis/messaging/v1alpha1"
	rocketmqclientset "knative.dev/eventing-contrib/rocketmq/pkg/client/clientset/versioned"
	rocketmqScheme "knative.dev/eventing-contrib/rocketmq/pkg/client/clientset/versioned/scheme"
	rocketmqclientsetinjection "knative.dev/eventing-contrib/rocketmq/pkg/client/injection/client"
	"knative.dev/eventing-contrib/rocketmq/pkg/client/injection/informers/messaging/v1alpha1/rocketmqchannel"
	rocketmqchannelreconciler "knative.dev/eventing-contrib/rocketmq/pkg/client/injection/reconciler/messaging/v1alpha1/rocketmqchannel"
	listers "knative.dev/eventing-contrib/rocketmq/pkg/client/listers/messaging/v1alpha1"
	"knative.dev/eventing-contrib/rocketmq/pkg/dispatcher"
	"knative.dev/eventing-contrib/rocketmq/pkg/utils"
)

func init() {
	// Add run types to the default Kubernetes Scheme so Events can be
	// logged for run types.
	_ = rocketmqScheme.AddToScheme(scheme.Scheme)
}

// Reconciler reconciles Rocketmq Channels.
type Reconciler struct {
	rocketmqDispatcher *dispatcher.RocketmqDispatcher

	rocketmqClientSet       rocketmqclientset.Interface
	rocketmqchannelLister   listers.RocketmqChannelLister
	rocketmqchannelInformer cache.SharedIndexInformer
	impl                    *controller.Impl
}

// Check that our Reconciler implements controller.Reconciler.
var _ rocketmqchannelreconciler.Interface = (*Reconciler)(nil)

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	logger := logging.FromContext(ctx)

	configMap, err := configmap.Load("/etc/config-rocketmq")
	if err != nil {
		logger.Fatal("error loading configuration", zap.Error(err))
	}

	rocketmqConfig, err := utils.GetRocketmqConfig(configMap)
	if err != nil {
		logger.Fatal("Error loading rocketmq config", zap.Error(err))
	}
	connectionArgs := &kncloudevents.ConnectionArgs{
		MaxIdleConns:        int(rocketmqConfig.MaxIdleConns),
		MaxIdleConnsPerHost: int(rocketmqConfig.MaxIdleConnsPerHost),
	}

	rocketmqChannelInformer := rocketmqchannel.Get(ctx)
	args := &dispatcher.RocketmqDispatcherArgs{
		KnCEConnectionArgs: connectionArgs,
		BrokerAddr:         rocketmqConfig.Brokers,
		TopicFunc:          utils.TopicName,
		Logger:             logger,
	}
	rocketmqDispatcher, err := dispatcher.NewDispatcher(ctx, args)
	if err != nil {
		logger.Fatal("Unable to create rocketmq dispatcher", zap.Error(err))
	}
	logger.Info("Starting the Rocketmq dispatcher")
	logger.Info("Rocketmq broker configuration", zap.Strings(utils.BrokerConfigMapKey, rocketmqConfig.Brokers))

	r := &Reconciler{
		rocketmqDispatcher:      rocketmqDispatcher,
		rocketmqClientSet:       rocketmqclientsetinjection.Get(ctx),
		rocketmqchannelLister:   rocketmqChannelInformer.Lister(),
		rocketmqchannelInformer: rocketmqChannelInformer.Informer(),
	}
	r.impl = rocketmqchannelreconciler.NewImpl(ctx, r)

	logger.Info("Setting up event handlers")

	// Watch for rocketmq channels.
	rocketmqChannelInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: filterWithAnnotation(injection.HasNamespaceScope(ctx)),
			Handler:    controller.HandleAll(r.impl.Enqueue),
		})

	logger.Info("Starting dispatcher.")
	go func() {
		if err := rocketmqDispatcher.Start(ctx); err != nil {
			logger.Error("Cannot start dispatcher", zap.Error(err))
		}
	}()

	return r.impl
}

func filterWithAnnotation(namespaced bool) func(obj interface{}) bool {
	if namespaced {
		return pkgreconciler.AnnotationFilterFunc(eventing.ScopeAnnotationKey, "namespace", false)
	}
	return pkgreconciler.AnnotationFilterFunc(eventing.ScopeAnnotationKey, "cluster", true)
}

func (r *Reconciler) ReconcileKind(ctx context.Context, rc *v1alpha1.RocketmqChannel) pkgreconciler.Event {
	channels, err := r.rocketmqchannelLister.List(labels.Everything())
	if err != nil {
		logging.FromContext(ctx).Error("Error listing rocketmq channels")
		return err
	}

	// TODO: revisit this code. Instead of reading all channels and updating consumers and hostToChannel map for all
	// why not just reconcile the current channel. With this the UpdateRocketmqConsumers can now return SubscribableStatus
	// for the subscriptions on the channel that is being reconciled.
	rocketmqChannels := make([]*v1alpha1.RocketmqChannel, 0)
	for _, channel := range channels {
		if channel.Status.IsReady() {
			rocketmqChannels = append(rocketmqChannels, channel)
		}
	}
	config := r.newConfigFromRocketmqChannels(rocketmqChannels)
	if err := r.rocketmqDispatcher.UpdateHostToChannelMap(config); err != nil {
		logging.FromContext(ctx).Error("Error updating host to channel map in dispatcher")
		return err
	}

	failedSubscriptions, err := r.rocketmqDispatcher.UpdateRocketmqConsumers(config)
	if err != nil {
		logging.FromContext(ctx).Error("Error updating rocketmq consumers in dispatcher")
		return err
	}
	rc.Status.SubscribableStatus = r.createSubscribableStatus(&rc.Spec.SubscribableSpec, failedSubscriptions)
	if len(failedSubscriptions) > 0 {
		logging.FromContext(ctx).Error("Some rocketmq subscriptions failed to subscribe")
		return fmt.Errorf("Some rocketmq subscriptions failed to subscribe")
	}
	return nil
}

func (r *Reconciler) createSubscribableStatus(subscribable *eventingduckv1.SubscribableSpec, failedSubscriptions map[types.UID]error) eventingduckv1.SubscribableStatus {
	if subscribable == nil {
		return eventingduckv1.SubscribableStatus{}
	}
	subscriberStatus := make([]eventingduckv1.SubscriberStatus, 0)
	for _, sub := range subscribable.Subscribers {
		status := eventingduckv1.SubscriberStatus{
			UID:                sub.UID,
			ObservedGeneration: sub.Generation,
			Ready:              corev1.ConditionTrue,
		}
		if err, ok := failedSubscriptions[sub.UID]; ok {
			status.Ready = corev1.ConditionFalse
			status.Message = err.Error()
		}
		subscriberStatus = append(subscriberStatus, status)
	}
	return eventingduckv1.SubscribableStatus{
		Subscribers: subscriberStatus,
	}
}

// newConfigFromRocketmqChannels creates a new Config from the list of rocketmq channels.
func (r *Reconciler) newChannelConfigFromRocketmqChannel(c *v1alpha1.RocketmqChannel) *dispatcher.ChannelConfig {
	channelConfig := dispatcher.ChannelConfig{
		Namespace: c.Namespace,
		Name:      c.Name,
		HostName:  c.Status.Address.URL.Host,
	}
	if c.Spec.SubscribableSpec.Subscribers != nil {
		newSubs := make([]dispatcher.Subscription, 0, len(c.Spec.SubscribableSpec.Subscribers))
		for _, source := range c.Spec.SubscribableSpec.Subscribers {
			innerSub, _ := fanout.SubscriberSpecToFanoutConfig(source)

			newSubs = append(newSubs, dispatcher.Subscription{
				Subscription: *innerSub,
				UID:          source.UID,
			})
		}
		channelConfig.Subscriptions = newSubs
	}

	return &channelConfig
}

// newConfigFromRocketmqChannels creates a new Config from the list of rocketmq channels.
func (r *Reconciler) newConfigFromRocketmqChannels(channels []*v1alpha1.RocketmqChannel) *dispatcher.Config {
	cc := make([]dispatcher.ChannelConfig, 0)
	for _, c := range channels {
		channelConfig := r.newChannelConfigFromRocketmqChannel(c)
		cc = append(cc, *channelConfig)
	}
	return &dispatcher.Config{
		ChannelConfigs: cc,
	}
}
func (r *Reconciler) updateStatus(ctx context.Context, desired *v1alpha1.RocketmqChannel) (*v1alpha1.RocketmqChannel, error) {
	rc, err := r.rocketmqchannelLister.RocketmqChannels(desired.Namespace).Get(desired.Name)
	if err != nil {
		return nil, err
	}

	if reflect.DeepEqual(rc.Status, desired.Status) {
		return rc, nil
	}

	// Don't modify the informers copy.
	existing := rc.DeepCopy()
	existing.Status = desired.Status

	new, err := r.rocketmqClientSet.MessagingV1alpha1().RocketmqChannels(desired.Namespace).UpdateStatus(existing)
	return new, err
}
