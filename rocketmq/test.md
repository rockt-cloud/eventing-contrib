# Test Process

1. Setup [Rocketmq](https://rocketmq.apache.org/docs/quick-start/)
2. Create the topic needed by test process manually like

```shell
bin/mqadmin updateTopic -t knative-messaging-rocketmq_test-namespace_test-rc -b 127.0.0.1:10911
bin/mqadmin updateTopic -t knative-messaging-rocketmq_default_test-channel -b 127.0.0.1:10911
bin/mqadmin updateTopic -t knative-messaging-rocketmq_default_test-channel-1 -b 127.0.0.1:10911
bin/mqadmin updateTopic -t knative-messaging-rocketmq_default_test-channel-2 -b 127.0.0.1:10911
```

3. `go test` in `eventing-contrib/rocketmq` folder