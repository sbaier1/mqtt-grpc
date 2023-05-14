# gRPC over MQTT

Just playing around with the idea of using MQTT5 as a transport protocol for gRPC to avoid a lot of network complexity.

MQTT5 introduces nifty concepts such as correlation data and response topics to accommodate such use cases.

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
    note over MqttClient, MqttBroker: Response Topic: "grpc/server/ping.PingService/Ping/response/test-client"
    MqttClient->>MqttBroker: PUBLISH (PingRequest, correlation ID, response topic)
    MqttBroker->>MqttServer: PUBLISH (PingRequest, correlation ID, response topic)
    MqttServer->>PingService: Invoke Ping method
    PingService->>MqttServer: Return PingResponse
    note over MqttServer, MqttBroker: Publish to "grpc/server/ping.PingService/Ping/response/test-client"
    MqttServer->>MqttBroker: PUBLISH (PingResponse, correlation ID)
    MqttBroker->>MqttClient: PUBLISH (PingResponse, correlation ID)
    MqttClient->>Client: Return PingResponse

```
