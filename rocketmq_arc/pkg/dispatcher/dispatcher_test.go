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

package dispatcher

import (
	"context"
	"testing"

	"go.uber.org/zap"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1beta1"
	eventingchannels "knative.dev/eventing/pkg/channel"
	_ "knative.dev/pkg/system/testing"

	"knative.dev/eventing-contrib/rocketmq/pkg/utils"
)

//----- Mocks
/*
type mockRocketmqConsumer struct {
	// createErr will return an error when creating a consumer
	createErr bool
}

func (c mockRocketmqConsumer) StartConsumer(groupID string, topics []string, logger *zap.Logger, handler rocketmq.RocketmqConsumerHandler) (rocketmq.PushConsumer, error) {
	if c.createErr {
		return nil, errors.New("error creating consumer")
	}

	return mockConsumer{}, nil
}

var _ rocketmq.RocketmqConsumer = (*mockRocketmqConsumer)(nil)

type mockConsumer struct{}

func (m mockConsumer) Start() error {
	return nil
}

func (m mockConsumer) Shutdown() error {
	return nil
}

func (m mockConsumer) Subscribe(topic string, selector consumer.MessageSelector,
	f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error {
	return nil
}

func (m mockConsumer) Unsubscribe(topic string) error {
	return nil
}

var _ rocketmq.PushConsumer = (*mockConsumer)(nil)


//----- Tests

// test util for various config checks
func (d *RocketmqDispatcher) checkConfigAndUpdate(config *multichannelfanout.Config) error {
	if config == nil {
		return errors.New("nil config")
	}

	if _, err := d.UpdateRocketmqConsumers(config); err != nil {
		// failed to update dispatchers consumers
		return err
	}
	if err := d.UpdateHostToChannelMap(config); err != nil {
		return err
	}

	return nil
}

func TestDispatcher_UpdateConfig(t *testing.T) {
	subscriber, _ := apis.ParseURL("http://test/subscriber")

	testCases := []struct {
		name             string
		oldConfig        *multichannelfanout.Config
		newConfig        *multichannelfanout.Config
		subscribes       []string
		unsubscribes     []string
		createErr        string
		oldHostToChanMap map[string]eventingchannels.ChannelReference
		newHostToChanMap map[string]eventingchannels.ChannelReference
	}{
		{
			name:             "nil config",
			oldConfig:        &multichannelfanout.Config{},
			newConfig:        nil,
			createErr:        "nil config",
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:             "same config",
			oldConfig:        &multichannelfanout.Config{},
			newConfig:        &multichannelfanout.Config{},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:      "config with no subscription",
			oldConfig: &multichannelfanout.Config{},
			newConfig: &multichannelfanout.Config{
				ChannelConfigs: []multichannelfanout.ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel",
						HostName:  "a.b.c.d",
					},
				},
			},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
		},
		{
			name:      "single channel w/ new subscriptions",
			oldConfig: &multichannelfanout.Config{},
			newConfig: &multichannelfanout.Config{
				ChannelConfigs: []multichannelfanout.ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel",
						HostName:  "a.b.c.d",
						FanoutConfig: fanout.Config{
							Subscriptions: []eventingduck.SubscriberSpec{
								{
									UID:           "subscription-1",
									SubscriberURI: subscriber,
								},
								{
									UID:           "subscription-2",
									SubscriberURI: subscriber,
								},
							},
						},
					},
				},
			},
			subscribes:       []string{"subscription-1", "subscription-2"},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
		},
		{
			name: "single channel w/ existing subscriptions",
			oldConfig: &multichannelfanout.Config{
				ChannelConfigs: []multichannelfanout.ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel",
						HostName:  "a.b.c.d",
						FanoutConfig: fanout.Config{
							Subscriptions: []eventingduck.SubscriberSpec{
								{
									UID:           "subscription-1",
									SubscriberURI: subscriber,
								},
								{
									UID:           "subscription-2",
									SubscriberURI: subscriber,
								}}}}},
			},
			newConfig: &multichannelfanout.Config{
				ChannelConfigs: []multichannelfanout.ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel",
						HostName:  "a.b.c.d",
						FanoutConfig: fanout.Config{
							Subscriptions: []eventingduck.SubscriberSpec{
								{
									UID:           "subscription-2",
									SubscriberURI: subscriber,
								},
								{
									UID:           "subscription-3",
									SubscriberURI: subscriber,
								},
							},
						},
					},
				},
			},
			subscribes:   []string{"subscription-2", "subscription-3"},
			unsubscribes: []string{"subscription-1"},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
		},
		{
			name: "multi channel w/old and new subscriptions",
			oldConfig: &multichannelfanout.Config{
				ChannelConfigs: []multichannelfanout.ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
						HostName:  "a.b.c.d",
						FanoutConfig: fanout.Config{
							Subscriptions: []eventingduck.SubscriberSpec{
								{
									UID:           "subscription-1",
									SubscriberURI: subscriber,
								},
								{
									UID:           "subscription-2",
									SubscriberURI: subscriber,
								},
							},
						}}}},
			newConfig: &multichannelfanout.Config{
				ChannelConfigs: []multichannelfanout.ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
						HostName:  "a.b.c.d",
						FanoutConfig: fanout.Config{
							Subscriptions: []eventingduck.SubscriberSpec{
								{
									UID:           "subscription-1",
									SubscriberURI: subscriber,
								},
							},
						},
					},
					{
						Namespace: "default",
						Name:      "test-channel-2",
						HostName:  "e.f.g.h",
						FanoutConfig: fanout.Config{
							Subscriptions: []eventingduck.SubscriberSpec{
								{
									UID:           "subscription-3",
									SubscriberURI: subscriber,
								},
								{
									UID:           "subscription-4",
									SubscriberURI: subscriber,
								},
							},
						},
					},
				},
			},
			subscribes:   []string{"subscription-1", "subscription-3", "subscription-4"},
			unsubscribes: []string{"subscription-2"},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel-1", Namespace: "default"},
			},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel-1", Namespace: "default"},
				"e.f.g.h": {Name: "test-channel-2", Namespace: "default"},
			},
		},
		{
			name:      "Duplicate hostnames",
			oldConfig: &multichannelfanout.Config{},
			newConfig: &multichannelfanout.Config{
				ChannelConfigs: []multichannelfanout.ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
						HostName:  "a.b.c.d",
					},
					{
						Namespace: "default",
						Name:      "test-channel-2",
						HostName:  "a.b.c.d",
					},
				},
			},
			createErr:        "duplicate hostName found. Each channel must have a unique host header. HostName:a.b.c.d, channel:default.test-channel-2, channel:default.test-channel-1",
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())
			d := &RocketmqDispatcher{
				rocketmqConsumer:     &mockRocketmqConsumer{},
				channelSubscriptions: make(map[eventingchannels.ChannelReference][]types.UID),
				subsConsumers:        make(map[types.UID]rocketmq.PushConsumer),
				subscriptions:        make(map[types.UID]subscription),
				topicFunc:            utils.TopicName,
				logger:               zaptest.NewLogger(t),
			}
			d.setHostToChannelMap(map[string]eventingchannels.ChannelReference{})

			// Initialize using oldConfig
			err := d.checkConfigAndUpdate(tc.oldConfig)
			if err != nil {

				t.Errorf("unexpected error: %v", err)
			}
			oldSubscribers := sets.NewString()
			for _, sub := range d.subscriptions {
				oldSubscribers.Insert(string(sub.UID))
			}
			if diff := sets.NewString(tc.unsubscribes...).Difference(oldSubscribers); diff.Len() != 0 {
				t.Errorf("subscriptions %+v were never subscribed", diff)
			}
			if diff := cmp.Diff(tc.oldHostToChanMap, d.getHostToChannelMap()); diff != "" {
				t.Errorf("unexpected hostToChannelMap (-want, +got) = %v", diff)
			}

			// Update with new config
			err = d.checkConfigAndUpdate(tc.newConfig)
			if tc.createErr != "" {
				if err == nil {
					t.Errorf("Expected UpdateConfig error: '%v'. Actual nil", tc.createErr)
				} else if err.Error() != tc.createErr {
					t.Errorf("Unexpected UpdateConfig error. Expected '%v'. Actual '%v'", tc.createErr, err)
				}
				return
			} else if err != nil {
				t.Errorf("Unexpected UpdateConfig error. Expected nil. Actual '%v'", err)
			}

			var newSubscribers []string
			for id := range d.subscriptions {
				newSubscribers = append(newSubscribers, string(id))
			}

			if diff := cmp.Diff(tc.subscribes, newSubscribers, sortStrings); diff != "" {
				t.Errorf("unexpected subscribers (-want, +got) = %v", diff)
			}
			if diff := cmp.Diff(tc.newHostToChanMap, d.getHostToChannelMap()); diff != "" {
				t.Errorf("unexpected hostToChannelMap (-want, +got) = %v", diff)
			}

		})
	}
}
*/

func TestSubscribeError(t *testing.T) {
	ctx := context.Background()
	d := &RocketmqDispatcher{
		logger:    zap.NewNop(),
		topicFunc: utils.TopicName,
	}

	channelRef := eventingchannels.ChannelReference{
		Name:      "test-channel",
		Namespace: "test-ns",
	}

	subRef := subscription{
		SubscriberSpec: eventingduck.SubscriberSpec{
			UID: "test-sub",
		},
	}
	err := d.subscribe(ctx, channelRef, subRef)
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "error creating consumer", err)
	}
}

/*
func TestUnsubscribeUnknownSub(t *testing.T) {
	cf := &mockRocketmqConsumer{createErr: true}
	d := &RocketmqDispatcher{
		rocketmqConsumer: cf,
		logger:           zap.NewNop(),
	}

	channelRef := eventingchannels.ChannelReference{
		Name:      "test-channel",
		Namespace: "test-ns",
	}

	subRef := subscription{
		SubscriberSpec: eventingduck.SubscriberSpec{
			UID: "test-sub",
		},
	}
	if err := d.unsubscribe(channelRef, subRef); err != nil {
		t.Errorf("Unsubscribe error: %v", err)
	}
}

func TestRocketmqDispatcher_Start(t *testing.T) {
	d := &RocketmqDispatcher{}

	err := d.Start(context.TODO())
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "message receiver is not set", err)
	}

	receiver, err := eventingchannels.NewMessageReceiver(
		func(ctx context.Context, channel eventingchannels.ChannelReference, message binding.Message, _ []binding.Transformer, _ http.Header) error {
			return nil
		},
		zap.NewNop(),
		eventingchannels.ResolveMessageChannelFromHostHeader(d.getChannelReferenceFromHost))
	if err != nil {
		t.Fatalf("Error creating new message receiver. Error:%s", err)
	}
	d.receiver = receiver
	err = d.Start(context.TODO())
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "rocketmqAsyncProducer is not set", err)
	}
}

func TestNewDispatcher(t *testing.T) {
	args := &RocketmqDispatcherArgs{
		ClientID:  "rocketmq-ch-dispatcher",
		Brokers:   []string{"localhost:10000"},
		TopicFunc: utils.TopicName,
		Logger:    nil,
	}
	_, err := NewDispatcher(context.TODO(), args)
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "message receiver is not set", err)
	}
}

var sortStrings = cmpopts.SortSlices(func(x, y string) bool {
	return x < y
})
*/
