import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.testcontainer.core.HiveMQTestContainerCore;
import io.grpc.stub.StreamObserver;
import io.mqttgrpc.client.MqttChannel;
import io.mqttgrpc.server.MqttServer;
import org.example.grpc.PingRequest;
import org.example.grpc.PingResponse;
import org.example.grpc.PingServiceGrpc;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baier
 */

@Testcontainers
public class MqttGrpcIntegrationTest {
    private static final int MQTT_PORT = 1883;
    @Container
    private static final HiveMQTestContainerCore<?> mqttBroker =
            new HiveMQTestContainerCore<>(DockerImageName.parse("hivemq/hivemq-ce"))
                    .withExposedPorts(MQTT_PORT)
                    .withNetwork(Network.SHARED);
    private static MqttServer mqttServer;
    private static PingServiceGrpc.PingServiceBlockingStub client;
    private static MqttChannel mqttChannel;

    @BeforeAll
    static void setup() throws IOException {
        // Start the MQTT server
        String brokerUrl = "localhost";
        final Integer mqttPort = mqttBroker.getMappedPort(MQTT_PORT);
        final String topicPrefix = "grpc/server";

        setupObserverClient(mqttPort);
        System.out.println("MQTT port " + mqttPort);
        mqttServer = MqttServer.builder()
                .clientId("server")
                .topicPrefix(topicPrefix)
                .brokerAddress(brokerUrl)
                .port(mqttPort)
                .addService(getMockImpl())
                .build();
        mqttServer.start();

        // Create the gRPC channel using the MqttChannel implementation
        mqttChannel = MqttChannel.builder()
                .clientId("client")
                .topicPrefix(topicPrefix)
                .brokerAddress(brokerUrl)
                .port(mqttPort)
                .build();
        client = PingServiceGrpc.newBlockingStub(mqttChannel);
    }

    private static void setupObserverClient(Integer mqttPort) {
        final Mqtt5AsyncClient observerClient = MqttClient.builder()
                .serverPort(mqttPort)
                .serverHost("localhost")
                .useMqttVersion5()
                .buildAsync();
        observerClient.connectWith()
                .send().join();
        observerClient.subscribeWith()
                .topicFilter("#")
                .qos(MqttQos.AT_LEAST_ONCE)
                .callback(mqtt5Publish -> {
                    System.out.println("TEST-OBSERVER: Received publish on topic " + mqtt5Publish.getTopic() + ", responseTopic: " + mqtt5Publish.getResponseTopic() + ", correlationID: " + mqtt5Publish.getCorrelationData() + ", payload: " + new String(mqtt5Publish.getPayloadAsBytes()));
                })
                .send()
                .join();
    }

    @NotNull
    private static PingServiceGrpc.PingServiceImplBase getMockImpl() {
        return new PingServiceGrpc.PingServiceImplBase() {
            @Override
            public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
                String payload = "Pong: " + request.getPayload();
                PingResponse response = PingResponse.newBuilder().setPayload(payload).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    @AfterAll
    static void teardown() {
        mqttServer.stop();
    }

    @Test
    void pingRequest_ExpectPongResponse() {
        PingRequest request = PingRequest.newBuilder()
                .setPayload("Hello")
                .build();
        PingResponse response = client.ping(request);
        assertThat(response.getPayload()).isEqualTo("Pong: Hello");
    }
}
