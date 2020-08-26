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
	"errors"
	"net/http"
	"net/url"

	"testing"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"knative.dev/eventing-contrib/rocketmq/pkg/utils"
	eventingchannels "knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
	_ "knative.dev/pkg/system/testing"
)

// ----- Tests

// test util for various config checks
func (d *RocketmqDispatcher) checkConfigAndUpdate(config *Config) error {
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
	subscriber, _ := url.Parse("http://test/subscriber")

	testCases := []struct {
		name             string
		oldConfig        *Config
		newConfig        *Config
		subscribes       []string
		unsubscribes     []string
		createErr        string
		oldHostToChanMap map[string]eventingchannels.ChannelReference
		newHostToChanMap map[string]eventingchannels.ChannelReference
	}{
		{
			name:             "nil config",
			oldConfig:        &Config{},
			newConfig:        nil,
			createErr:        "nil config",
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:             "same config",
			oldConfig:        &Config{},
			newConfig:        &Config{},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:      "config with no subscription",
			oldConfig: &Config{},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
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
			oldConfig: &Config{},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel",
						HostName:  "a.b.c.d",
						Subscriptions: []Subscription{
							{
								UID: "subscription-1",
								Subscription: fanout.Subscription{
									Subscriber: subscriber,
								},
							},
							{
								UID: "subscription-2",
								Subscription: fanout.Subscription{
									Subscriber: subscriber,
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
		}, /*
			//not test together when using non-mock client
			{
				name: "single channel w/ existing subscriptions",
				oldConfig: &Config{
					ChannelConfigs: []ChannelConfig{
						{
							Namespace: "default",
							Name:      "test-channel",
							HostName:  "a.b.c.d",
							Subscriptions: []Subscription{
								{
									UID: "subscription-3",
									Subscription: fanout.Subscription{
										Subscriber: subscriber,
									},
								},
								{
									UID: "subscription-4",
									Subscription: fanout.Subscription{
										Subscriber: subscriber,
									},
								},
							},
						},
					},
				},
				newConfig: &Config{
					ChannelConfigs: []ChannelConfig{
						{
							Namespace: "default",
							Name:      "test-channel",
							HostName:  "a.b.c.d",
							Subscriptions: []Subscription{
								{
									UID: "subscription-4",
									Subscription: fanout.Subscription{
										Subscriber: subscriber,
									},
								},
								{
									UID: "subscription-5",
									Subscription: fanout.Subscription{
										Subscriber: subscriber,
									},
								},
							},
						},
					},
				},
				subscribes:   []string{"subscription-4", "subscription-5"},
				unsubscribes: []string{"subscription-3"},
				oldHostToChanMap: map[string]eventingchannels.ChannelReference{
					"a.b.c.d": {Name: "test-channel", Namespace: "default"},
				},
				newHostToChanMap: map[string]eventingchannels.ChannelReference{
					"a.b.c.d": {Name: "test-channel", Namespace: "default"},
				},
			},*/
		{
			name: "multi channel w/old and new subscriptions",
			oldConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
						HostName:  "a.b.c.d",
						Subscriptions: []Subscription{
							{
								UID: "subscription-6",
								Subscription: fanout.Subscription{
									Subscriber: subscriber,
								},
							},
							{
								UID: "subscription-7",
								Subscription: fanout.Subscription{
									Subscriber: subscriber,
								},
							},
						},
					},
				},
			},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
						HostName:  "a.b.c.d",
						Subscriptions: []Subscription{
							{
								UID: "subscription-6",
								Subscription: fanout.Subscription{
									Subscriber: subscriber,
								},
							},
						},
					},
					{
						Namespace: "default",
						Name:      "test-channel-2",
						HostName:  "e.f.g.h",
						Subscriptions: []Subscription{
							{
								UID: "subscription-8",
								Subscription: fanout.Subscription{
									Subscriber: subscriber,
								},
							},
							{
								UID: "subscription-9",
								Subscription: fanout.Subscription{
									Subscriber: subscriber,
								},
							},
						},
					},
				},
			},
			subscribes:   []string{"subscription-6", "subscription-8", "subscription-9"},
			unsubscribes: []string{"subscription-7"},
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
			oldConfig: &Config{},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
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
				channelSubscriptions: make(map[eventingchannels.ChannelReference][]types.UID),
				subsConsumers:        make(map[types.UID]rocketmq.PushConsumer),
				subscriptions:        make(map[types.UID]Subscription),
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

			//err = d.RemoveConsumers()
			//if err != nil {
			//	t.Errorf("Unexpected RemoveConsumers error: %v", err)
			//}

		})
	}
}

func TestUnsubscribeUnknownSub(t *testing.T) {

	d := &RocketmqDispatcher{
		logger: zap.NewNop(),
	}

	channelRef := eventingchannels.ChannelReference{
		Name:      "test-channel",
		Namespace: "test-ns",
	}

	subRef := Subscription{
		UID:          "test-sub",
		Subscription: fanout.Subscription{},
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

/*
// TestNewDispatcher need to test when everything started
func TestNewDispatcher(t *testing.T) {
	args := &RocketmqDispatcherArgs{
		BrokerAddr: []string{"127.0.0.1:9876"},
		//TopicFunc:            utils.TopicName,
		TopicFunc: "TopicTest",
		Logger:    nil,
	}
	_, err := NewDispatcher(context.TODO(), args)
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "message receiver is not set", err)
	}
}
*/
var sortStrings = cmpopts.SortSlices(func(x, y string) bool {
	return x < y
})
