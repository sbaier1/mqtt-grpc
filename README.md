# gRPC over MQTT

Just playing around with the idea of using MQTT5 as a transport protocol for gRPC to avoid a lot of network complexity.

MQTT5 introduces nifty concepts such as correlation data and response topics to accommodate such use cases.

Check the [integration test](src/intTest/java/MqttGrpcIntegrationTest.java) for a simple usage example.

## Topic structure and MQTT semantics

The topic structure of this implementation is defined as follows:

```
* topicPrefix (common MQTT topic prefix on client and server)
|
\-- /responses/<clientId>/<full method name>: MqttChannels subscribe here to receive responses from the servers
\-- /endpoints/<full method name>: MqttServers subscribe here to receive queries from clients
\-- /ping: MqttChannels publish here to verify that an MqttServer is present at this topic structure. (optional) 
```

The only really fixed part of this topic structure is the `topicPrefix/endpoints/<full method name>`. The Server
actually does not care about the response topic structure since it just uses the response topic for responding, so
custom implementations are easy.

MqttServers can use shared subscriptions as their topic prefix to facilitate horizontal scaling automatically.

## TODOs

* Backpressure handling + scale testing: Everything is currently loosely thrown together, but there are many
  opportunities here for backpressure handling making it scale well
* Ensure polyglot compatibility: Right now it's just a java implementation. It's TBD if all the assumptions made during
  this implementation (esp. if fullMethodName reflects exactly the same in other languages) will carry over nicely when
  talking to an implementation in another language (e.g. a javascript implementation of this same concept)

## Example

This is an example of how the flow of MQTT messages might look like using this transport.

The example refers to the [ping_service.proto](src/intTest/proto/ping_service.proto) which is used in integration tests.

```mermaid
sequenceDiagram
    participant Client as gRPC Client
    participant MqttClient as MQTT Client (clientID: test-client)
    participant MqttBroker as MQTT Broker
    participant MqttServer as MQTT Server
    participant PingService as Ping Service


    Client->>MqttClient: Send PingRequest
    note over MqttClient, MqttBroker: Publish to "grpc/server/ping.PingService/Ping"
    note over MqttClient, MqttBroker: Correlation ID: 123e4567-e89b-12d3-a456-426614174000
    note over MqttClient, MqttBroker: Response Topic: "grpc/server/ping.PingService/Ping/responses/test-client"
    MqttClient ->> MqttBroker: PUBLISH (PingRequest, correlation ID, response topic)
    MqttBroker->>MqttServer: PUBLISH (PingRequest, correlation ID, response topic)
    MqttServer->>PingService: Invoke Ping method
    PingService ->> MqttServer: Return PingResponse
    note over MqttServer, MqttBroker: Publish to "grpc/server/ping.PingService/Ping/responses/test-client"
    MqttServer ->> MqttBroker: PUBLISH (PingResponse, correlation ID)
    MqttBroker->>MqttClient: PUBLISH (PingResponse, correlation ID)
    MqttClient->>Client: Return PingResponse

```
