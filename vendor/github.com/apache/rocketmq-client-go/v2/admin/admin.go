/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type Admin interface {
	CreateTopic(ctx context.Context, opts ...OptionCreate) error
	//TopicList(ctx context.Context, mq *primitive.MessageQueue) (*remote.RemotingCommand, error)
	//GetBrokerClusterInfo(ctx context.Context) (*remote.RemotingCommand, error)

	DeleteTopic(ctx context.Context, opts ...OptionDelete) error

	Close() error
}

// TODO: 超时的内容, 全部转移到 ctx
type adminOptions struct {
	internal.ClientOptions
}

type AdminOption func(options *adminOptions)

func defaultAdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_ADMIN"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func WithResolver(resolver primitive.NsResolver) AdminOption {
	return func(options *adminOptions) {
		options.Resolver = resolver
	}
}

type admin struct {
	cli     internal.RMQClient
	namesrv internal.Namesrvs

	opts *adminOptions

	closeOnce sync.Once
}

// NewAdmin initialize admin
func NewAdmin(opts ...AdminOption) (Admin, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver)
	if err != nil {
		return nil, err
	}
	//log.Printf("Client: %#v", namesrv.srvs)
	return &admin{
		cli:     cli,
		namesrv: namesrv,
		opts:    defaultOpts,
	}, nil
}

func (a *admin) getAddr(mq *primitive.MessageQueue) (string, error) {
	broker := a.namesrv.FindBrokerAddrByName(mq.BrokerName)
	if len(broker) == 0 {
		a.namesrv.UpdateTopicRouteInfo(mq.Topic)
		broker = a.namesrv.FindBrokerAddrByName(mq.BrokerName)

		if len(broker) == 0 {
			return "", fmt.Errorf("broker: %s address not found", mq.BrokerName)
		}
	}
	return broker, nil
}

// CreateTopic create topic.
func (a *admin) CreateTopic(ctx context.Context, opts ...OptionCreate) error {
	cfg := defaultTopicConfigCreate()
	for _, apply := range opts {
		apply(&cfg)
	}

	request := &internal.CreateTopicRequestHeader{
		Topic:           cfg.Topic,
		DefaultTopic:    cfg.DefaultTopic,
		ReadQueueNums:   cfg.ReadQueueNums,
		WriteQueueNums:  cfg.WriteQueueNums,
		Perm:            cfg.Perm,
		TopicFilterType: cfg.TopicFilterType,
		TopicSysFlag:    cfg.TopicSysFlag,
		Order:           cfg.Order,
	}

	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, request, nil)
	_, err := a.cli.InvokeSync(ctx, cfg.BrokerAddr, cmd, 5*time.Second)
	return err
}

/*
// DeleteTopicInBroker delete topic in broker.
func (a *admin) TopicList(ctx context.Context, mq *primitive.MessageQueue) (*remote.RemotingCommand, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllTopicListFromNameServer, nil, nil)
	response, err := a.cli.InvokeSync(ctx, "", cmd, 5*time.Second)
	return response, err
}

// DeleteTopicInBroker delete topic in broker.
func (a *admin) GetBrokerClusterInfo(ctx context.Context) (*remote.RemotingCommand, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
	response, err := a.cli.InvokeSync(ctx, "", cmd, 5*time.Second)

	return response, err
}
*/
// DeleteTopicInBroker delete topic in broker.
func (a *admin) deleteTopicInBroker(ctx context.Context, topic string, brokerAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInBroker, request, nil)
	return a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
}

// DeleteTopicInNameServer delete topic in nameserver.
func (a *admin) deleteTopicInNameServer(ctx context.Context, topic string, nameSrvAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInNameSrv, request, nil)
	return a.cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

// DeleteTopic delete topic in both broker and nameserver.
func (a *admin) DeleteTopic(ctx context.Context, opts ...OptionDelete) error {
	cfg := defaultTopicConfigDelete()
	for _, apply := range opts {
		apply(&cfg)
	}
	//delete topic in broker
	if cfg.BrokerAddr == "" {
		a.namesrv.UpdateTopicRouteInfo(cfg.Topic)
		cfg.BrokerAddr = a.namesrv.FindBrokerAddrByTopic(cfg.Topic)
	}

	if _, err := a.deleteTopicInBroker(ctx, cfg.Topic, cfg.BrokerAddr); err != nil {
		return err
	}

	//delete topic in nameserver
	if len(cfg.NameSrvAddr) == 0 {
		a.namesrv.UpdateTopicRouteInfo(cfg.Topic)
		cfg.NameSrvAddr = a.namesrv.AddrList()
	}

	for _, nameSrvAddr := range cfg.NameSrvAddr {
		if _, err := a.deleteTopicInNameServer(ctx, cfg.Topic, nameSrvAddr); err != nil {
			return err
		}
	}
	return nil
}

func (a *admin) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}
