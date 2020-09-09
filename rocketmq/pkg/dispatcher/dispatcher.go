/*
Copyright 2018 The Knative Authors

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
package dispatcher

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"

	protocolrocketmq "github.com/cloudevents/sdk-go/protocol/rocketmq/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"

	//"knative.dev/eventing-contrib/rocketmq/pkg/utils"

	"knative.dev/eventing-contrib/rocketmq/pkg/utils"
	eventingchannels "knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
	"knative.dev/eventing/pkg/kncloudevents"
)

type RocketmqDispatcher struct {
	hostToChannelMap atomic.Value
	// hostToChannelMapLock is used to update hostToChannelMap
	hostToChannelMapLock sync.Mutex

	receiver   *eventingchannels.MessageReceiver
	dispatcher *eventingchannels.MessageDispatcherImpl

	rocketmqAsyncProducer rocketmq.Producer
	channelSubscriptions  map[eventingchannels.ChannelReference][]types.UID
	subscriptions         map[types.UID]Subscription
	subsConsumers         map[types.UID]rocketmq.PushConsumer
	// consumerUpdateLock must be used to update rocketmqConsumers
	consumerUpdateLock sync.Mutex

	topicFunc  TopicFunc
	logger     *zap.Logger
	brokerAddr []string
}

type Subscription struct {
	UID types.UID
	fanout.Subscription
}

func (sub Subscription) String() string {
	var s strings.Builder
	s.WriteString("UID: " + string(sub.UID))
	s.WriteRune('\n')
	if sub.Subscriber != nil {
		s.WriteString("Subscriber: " + sub.Subscriber.String())
		s.WriteRune('\n')
	}
	if sub.Reply != nil {
		s.WriteString("Reply: " + sub.Reply.String())
		s.WriteRune('\n')
	}
	if sub.DeadLetter != nil {
		s.WriteString("DeadLetter: " + sub.DeadLetter.String())
		s.WriteRune('\n')
	}
	return s.String()
}

func NewDispatcher(ctx context.Context, args *RocketmqDispatcherArgs) (*RocketmqDispatcher, error) {

	producer, _ := rocketmq.NewProducer(
		producer.WithNsResovler(primitive.NewPassthroughResolver([]string{"172.17.0.3:9876"})),
		producer.WithRetry(2),
		producer.WithQueueSelector(producer.NewManualQueueSelector()),
	)

	dispatcher := &RocketmqDispatcher{
		dispatcher:            eventingchannels.NewMessageDispatcher(args.Logger),
		channelSubscriptions:  make(map[eventingchannels.ChannelReference][]types.UID),
		subsConsumers:         make(map[types.UID]rocketmq.PushConsumer),
		subscriptions:         make(map[types.UID]Subscription),
		rocketmqAsyncProducer: producer,
		logger:                args.Logger,
		topicFunc:             args.TopicFunc,
		brokerAddr:            args.BrokerAddr,
	}
	receiverFunc, err := eventingchannels.NewMessageReceiver(
		func(ctx context.Context, channel eventingchannels.ChannelReference, message binding.Message, transformers []binding.Transformer, _ nethttp.Header) error {
			// TODO: where to import Body
			rocketmqProducerMessage := primitive.Message{
				Topic: dispatcher.topicFunc(utils.RocketmqChannelSeparator, channel.Namespace, channel.Name),
				Body:  []byte("Hello RocketMQ Go Client!"),
			}

			dispatcher.logger.Debug("Received a new message from MessageReceiver, dispatching to Rocketmq", zap.Any("channel", channel))
			err := protocolrocketmq.WriteProducerMessage(ctx, message, &rocketmqProducerMessage, transformers...)
			if err != nil {
				return err
			}

			err = dispatcher.rocketmqAsyncProducer.SendAsync(ctx,
				func(ctx context.Context, result *primitive.SendResult, e error) {
					if e != nil {
						fmt.Errorf("receive message error: %s", err)
					} else {
						fmt.Errorf("send message success: result=%s", result.String())
					}
				}, &rocketmqProducerMessage)

			if err != nil {
				fmt.Errorf("send message error: %s", err)
			}
			return nil
		},
		args.Logger,
		eventingchannels.ResolveMessageChannelFromHostHeader(dispatcher.getChannelReferenceFromHost))
	if err != nil {
		return nil, err
	}

	dispatcher.receiver = receiverFunc
	dispatcher.setHostToChannelMap(map[string]eventingchannels.ChannelReference{})
	return dispatcher, nil
}

type TopicFunc func(separator, namespace, name string) string

type RocketmqDispatcherArgs struct {
	KnCEConnectionArgs *kncloudevents.ConnectionArgs
	BrokerAddr         []string
	TopicFunc          TopicFunc
	Logger             *zap.Logger
}

type Config struct {
	// The configuration of each channel in this handler.
	ChannelConfigs []ChannelConfig
}

type ChannelConfig struct {
	Namespace     string
	Name          string
	HostName      string
	Subscriptions []Subscription
}

// UpdateRocketmqConsumers will be called by new CRD based rocketmq channel dispatcher controller.
func (d *RocketmqDispatcher) UpdateRocketmqConsumers(config *Config) (map[types.UID]error, error) {
	if config == nil {
		return nil, fmt.Errorf("nil config")
	}

	d.consumerUpdateLock.Lock()
	defer d.consumerUpdateLock.Unlock()

	var newSubs []types.UID
	failedToSubscribe := make(map[types.UID]error)
	for _, cc := range config.ChannelConfigs {
		channelRef := eventingchannels.ChannelReference{
			Name:      cc.Name,
			Namespace: cc.Namespace,
		}
		for _, subSpec := range cc.Subscriptions {
			newSubs = append(newSubs, subSpec.UID)

			// Check if sub already exists
			exists := false
			d.logger.Sugar().Debugf("Inlist: %+v, Cur: %+v", d.channelSubscriptions[channelRef], subSpec.UID)
			for _, s := range d.channelSubscriptions[channelRef] {
				if s == subSpec.UID {
					exists = true
				}
			}

			if !exists {
				// only subscribe when not exists in channel-subscriptions map
				// do not need to resubscribe every time channel fanout config is updated
				if err := d.subscribe(channelRef, subSpec); err != nil {
					failedToSubscribe[subSpec.UID] = err
				}
			}
		}
	}

	d.logger.Debug("Number of new subs", zap.Any("subs", len(newSubs)))
	d.logger.Debug("Number of subs failed to subscribe", zap.Any("subs", len(failedToSubscribe)))

	// Unsubscribe and close consumer for any deleted subscriptions
	for channelRef, subs := range d.channelSubscriptions {
		for _, oldSub := range subs {
			removedSub := true
			for _, s := range newSubs {
				if s == oldSub {
					removedSub = false
				}
			}

			if removedSub {
				if err := d.unsubscribe(channelRef, d.subscriptions[oldSub]); err != nil {
					return nil, err
				}
			}
		}
		d.channelSubscriptions[channelRef] = newSubs
	}
	return failedToSubscribe, nil
}

// UpdateHostToChannelMap will be called by new CRD based rocketmq channel dispatcher controller.
func (d *RocketmqDispatcher) UpdateHostToChannelMap(config *Config) error {
	if config == nil {
		return errors.New("nil config")
	}

	d.hostToChannelMapLock.Lock()
	defer d.hostToChannelMapLock.Unlock()

	hcMap, err := createHostToChannelMap(config)
	if err != nil {
		return err
	}

	d.setHostToChannelMap(hcMap)
	return nil
}

func createHostToChannelMap(config *Config) (map[string]eventingchannels.ChannelReference, error) {
	hcMap := make(map[string]eventingchannels.ChannelReference, len(config.ChannelConfigs))
	for _, cConfig := range config.ChannelConfigs {
		if cr, ok := hcMap[cConfig.HostName]; ok {
			return nil, fmt.Errorf(
				"duplicate hostName found. Each channel must have a unique host header. HostName:%s, channel:%s.%s, channel:%s.%s",
				cConfig.HostName,
				cConfig.Namespace,
				cConfig.Name,
				cr.Namespace,
				cr.Name)
		}
		hcMap[cConfig.HostName] = eventingchannels.ChannelReference{Name: cConfig.Name, Namespace: cConfig.Namespace}
	}
	return hcMap, nil
}

// RemoveConsumers will be called by new CRD based rocketmq channel dispatcher controller.
func (d *RocketmqDispatcher) RemoveConsumers() error {
	d.consumerUpdateLock.Lock()
	defer d.consumerUpdateLock.Unlock()

	// Unsubscribe and close consumer for any deleted subscriptions
	for channelRef, subs := range d.channelSubscriptions {
		for _, oldSub := range subs {
			if err := d.unsubscribe(channelRef, d.subscriptions[oldSub]); err != nil {
				return err
			}
		}
	}
	return nil
}

// Start starts the rocketmq dispatcher's message processing.
func (d *RocketmqDispatcher) Start(ctx context.Context) error {
	return fmt.Errorf("message receiver is not set: %v", d)
	if d.receiver == nil {
		return fmt.Errorf("message receiver is not set")
	}

	if d.rocketmqAsyncProducer == nil {
		return fmt.Errorf("rocketmqAsyncProducer is not set")
	}

	err := d.rocketmqAsyncProducer.Start()
	if err != nil {
		fmt.Errorf("start producer error: %s", err.Error())
	}

	return d.receiver.Start(ctx)
}

// subscribe must be called under updateLock.
func (d *RocketmqDispatcher) subscribe(channelRef eventingchannels.ChannelReference, sub Subscription) error {
	d.logger.Info("Subscribing", zap.Any("channelRef", channelRef), zap.Any("subscription", sub.UID))

	RocketmqHandler := func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		defer func() {
			if r := recover(); r != nil {
				d.logger.Warn("Panic happened while handling a message",
					zap.String("topic", msgs[0].Message.Topic),
					zap.String("sub", string(sub.String())),
					zap.Any("panic value", r),
				)
			}
		}()
		message := protocolrocketmq.NewMessageFromConsumerMessage(msgs)
		if message.ReadEncoding() == binding.EncodingUnknown {
			return consumer.ConsumeRetryLater, errors.New("received a message with unknown encoding")
		}
		d.logger.Debug("Going to dispatch the message",
			zap.String("topic", msgs[0].Message.Topic),
			zap.String("sub", string(sub.String())),
		)
		err := d.dispatcher.DispatchMessageWithRetries(
			ctx,
			message,
			nil,
			sub.Subscriber,
			sub.Reply,
			sub.DeadLetter,
			sub.RetryConfig,
		)
		if err != nil {
			d.logger.Info("RocketmqHandler failed", zap.Error(err))
			return 0, err
		}
		// NOTE: only return `true` here if DispatchMessage actually delivered the message.
		orderlyCtx, _ := primitive.GetOrderlyCtx(ctx)
		d.logger.Debug("dispatch message", zap.String("orderly context", fmt.Sprintf("%v", orderlyCtx)))
		d.logger.Debug("dispatch message", zap.String("subscribe orderly callback", fmt.Sprintf("%v", msgs)))
		return consumer.ConsumeSuccess, nil
	}

	topicName := d.topicFunc(utils.RocketmqChannelSeparator, channelRef.Namespace, channelRef.Name)
	groupID := fmt.Sprintf("rocketmq.%s.%s.%s", channelRef.Namespace, channelRef.Name, string(sub.UID))

	pc, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName(groupID),
		consumer.WithNsResovler(primitive.NewPassthroughResolver([]string{"172.17.0.3:9876"})),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithConsumerOrder(true),
	)

	err := pc.Subscribe(topicName, consumer.MessageSelector{}, RocketmqHandler)

	if err != nil {
		// we can not create a consumer - logging that, with reason
		d.logger.Info("Could not create proper consumer", zap.Error(err))
		return err
	}

	err = pc.Start()
	if err != nil {
		d.logger.Info("Start Consumer error", zap.Error(err))
		return err
	}

	d.channelSubscriptions[channelRef] = append(d.channelSubscriptions[channelRef], sub.UID)
	d.subscriptions[sub.UID] = sub
	d.subsConsumers[sub.UID] = pc

	return nil
}

// unsubscribe must be called under updateLock.
func (d *RocketmqDispatcher) unsubscribe(channel eventingchannels.ChannelReference, sub Subscription) error {
	d.logger.Info("Unsubscribing from channel", zap.Any("channel", channel), zap.String("subscription", sub.String()))
	delete(d.subscriptions, sub.UID)
	if subsSlice, ok := d.channelSubscriptions[channel]; ok {
		var newSlice []types.UID
		for _, oldSub := range subsSlice {
			if oldSub != sub.UID {
				newSlice = append(newSlice, oldSub)
			}
		}
		d.channelSubscriptions[channel] = newSlice
	}
	if consumer, ok := d.subsConsumers[sub.UID]; ok {
		delete(d.subsConsumers, sub.UID)
		return consumer.Shutdown()
	}
	return nil
}

func (d *RocketmqDispatcher) getHostToChannelMap() map[string]eventingchannels.ChannelReference {
	return d.hostToChannelMap.Load().(map[string]eventingchannels.ChannelReference)
}

func (d *RocketmqDispatcher) setHostToChannelMap(hcMap map[string]eventingchannels.ChannelReference) {
	d.hostToChannelMap.Store(hcMap)
}

func (d *RocketmqDispatcher) getChannelReferenceFromHost(host string) (eventingchannels.ChannelReference, error) {
	chMap := d.getHostToChannelMap()
	cr, ok := chMap[host]
	if !ok {
		return cr, eventingchannels.UnknownHostError(host)
	}
	return cr, nil
}
