# Apache Rocketmq Channels

Rocketmq channels are those backed by [Apache Rocketmq](https://rocketmq.apache.org/) topics.

## Deployment steps

### 1. Start RocketMQ
See Apache RocketMQ [quick start doc](https://rocketmq.apache.org/docs/quick-start/) for detail.

Return to `/HOME/go/src/knative.dev/eventing-contrib/rocketmq/`.

### 2. Install Knative Eventing CRD and core components
```shell
kubectl apply --filename https://github.com/knative/eventing/releases/download/v0.17.0/eventing-crds.yaml
kubectl apply --filename https://github.com/knative/eventing/releases/download/v0.17.0/eventing-core.yaml
```

### 3. Install RocketMQ Channel with ko
Install ko if needed.
```shell
GO111MODULE=on go get github.com/google/ko/cmd/ko
```

And then install RocketMQ Channel
```shell
# Only works for minikube
# See [ko doc](https://github.com/google/ko/blob/master/README.md#with-minikube) for details
export KO_DOCKER_REPO=ko.local
eval $(minikube docker-env)
kubectl config use-context minikube
ko apply -f config
```

### 4. Install Eventing Brokers (use InMemoryChannel-based broker)
```shell
kubectl apply --filename https://github.com/knative/eventing/releases/download/v0.17.0/in-memory-channel.yaml
kubectl apply --filename https://github.com/knative/eventing/releases/download/v0.17.0/mt-channel-broker.yaml
```
Monitor until all of the components show a STATUS of Running
```shell
kubectl get pods --namespace knative-eventing
```

### 5. Configure RocketmqChannel
#### Creating a `RocketmqChannel` CRD
```shell
kubectl apply -f demo/000-rocketmqchannel_crd.yaml
```
#### Specifying the default channel configuration
```shell
kubectl apply -f demo/001-default-ch-webhook.yaml
```
#### Creating an RocketMQ channel using the default channel configuration
```shell
kubectl apply -f demo/002-testchannel.yaml
```
#### Create test namespace and configuring the Knative broker for RocketMQ channels
```shell
kubectl create namespace event-example
kubectl -n event-example apply -f demo/100-createBroker.yaml
# check if broker works
kubectl -n event-example get broker default
```
#### Creating simple event consumer
```shell
kubectl -n event-example apply -f demo/101_createConsumer.yaml
```
#### Creating trigger, which defines the events that each event consumer receives
```shell
kubectl -n event-example apply -f demo/102_createTrigger.yaml
```
#### Creating simple event producer which could execute the `curl` command
```shell
kubectl -n event-example apply -f demo/103_createProducer.yaml
```
#### Sending events to the broker
SSH into pod
```shell
kubectl -n event-example attach curl -it
```
and make a HTTP Request to the broker by sending command in demo/104_sendEvent.sh.
If the event has been received, you will receive a `202 Accepted` response like:
```shell
< HTTP/1.1 202 Accepted
< Content-Length: 0
< Date: Mon, 12 Aug 2019 19:48:18 GMT
```
and type `exit` to exit.
#### Verifying that events were received
```shell
kubectl -n event-example logs -l app=hello-display --tail=100
```
and the response would look like
```shell
☁️  cloudevents.Event
Validation: valid
Context Attributes,
  specversion: 1.0
  type: greeting
  source: not-sendoff
  id: say-hello
  time: 2019-05-20T17:59:43.81718488Z
  contenttype: application/json
Extensions,
  knativehistory: default-broker-srk54-channel-24gls.event-example.svc.cluster.local
Data,
  {
    "msg": "Hello Knative!"
  }
```



## Test Process

1. Setup [Rocketmq](https://rocketmq.apache.org/docs/quick-start/)
2. Create the topic needed by test process manually like

```shell
bin/mqadmin updateTopic -t knative-messaging-rocketmq_test-namespace_test-rc -b 127.0.0.1:10911
bin/mqadmin updateTopic -t knative-messaging-rocketmq_default_test-channel -b 127.0.0.1:10911
bin/mqadmin updateTopic -t knative-messaging-rocketmq_default_test-channel-1 -b 127.0.0.1:10911
bin/mqadmin updateTopic -t knative-messaging-rocketmq_default_test-channel-2 -b 127.0.0.1:10911
```

3. `go test` in `eventing-contrib/rocketmq` folder
