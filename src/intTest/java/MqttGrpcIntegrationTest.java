import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.testcontainer.core.HiveMQTestContainerCore;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.mqttgrpc.client.MqttChannel;
import io.mqttgrpc.server.MqttServer;
import org.example.grpc.PingRequest;
import org.example.grpc.PingResponse;
import org.example.grpc.PingServiceGrpc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
                .addInterceptor(getNoopInterceptor())
                .build();
        mqttServer.start();

        // Create the gRPC channel using the MqttChannel implementation
        mqttChannel = MqttChannel.builder()
                .clientId("client")
                .topicPrefix(topicPrefix)
                .brokerAddress(brokerUrl)
                .port(mqttPort)
                .intercept(getSampleInterceptor())
                .build();
        client = PingServiceGrpc.newBlockingStub(mqttChannel);
    }


    @AfterAll
    static void teardown() {
        mqttServer.stop();
    }

    @NotNull
    private static ServerInterceptor getNoopInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                final ServerCall.Listener<ReqT> listener = next.startCall(call, headers);
                return new ServerCall.Listener<ReqT>() {
                    @Override
                    public void onMessage(ReqT message) {
                        System.out.println("Intercepted request example: " + message);
                        // Process the request message

                        // Forward the message to the next listener in the chain
                        listener.onMessage(message);
                    }

                    @Override
                    public void onHalfClose() {
                        // Handle the client indicating it has finished sending messages
                        listener.onHalfClose();
                    }

                    @Override
                    public void onCancel() {
                        // Handle cancellation of the RPC call
                        listener.onCancel();
                    }


                    @Override
                    public void onComplete() {
                        // Handle the completion of the RPC call
                        listener.onComplete();
                    }
                };
            }
        };
    }

    @NotNull
    private static ClientInterceptor getSampleInterceptor() {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);
                return new ClientCall<ReqT, RespT>() {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        clientCall.start(responseListener, headers);
                    }

                    @Override
                    public void request(int numMessages) {
                        clientCall.request(2);
                    }

                    @Override
                    public void cancel(@Nullable String message, @Nullable Throwable cause) {
                        clientCall.cancel(message, cause);
                    }

                    @Override
                    public void halfClose() {
                        clientCall.halfClose();
                    }

                    @Override
                    public void sendMessage(ReqT message) {
                        System.out.println("Intercepted client call example " + message);
                        clientCall.sendMessage(message);
                    }
                };
            }
        };
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

    @Test
    void pingRequest_ExpectPongResponse() {
        PingRequest request = PingRequest.newBuilder()
                .setPayload("Hello")
                .build();
        PingResponse response = client.ping(request);
        assertThat(response.getPayload()).isEqualTo("Pong: Hello");
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

}
