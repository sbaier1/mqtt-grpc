package io.mqttgrpc;
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
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Simon Baier
 */

@Testcontainers
public class MqttGrpcIntegrationTest {
    private static final int MQTT_PORT = 1883;
    @Container
    private static final HiveMQTestContainerCore<?> mqttBroker =
            new HiveMQTestContainerCore<>(DockerImageName.parse("hivemq/hivemq-edge"))
                    .withExposedPorts(MQTT_PORT)
                    .withNetwork(Network.SHARED);
    private static MqttServer mqttServer;
    private static PingServiceGrpc.PingServiceBlockingStub client;
    private static MqttChannel mqttChannel;
    private static Integer mqttPort;
    private static String brokerUrl;
    private static String topicPrefix;

    @BeforeAll
    static void setup() throws IOException {
        // Start the MQTT server
        brokerUrl = "localhost";
        mqttPort = mqttBroker.getMappedPort(MQTT_PORT);
        topicPrefix = "grpc/server";

        setupObserverClient(mqttPort);
        System.out.println("MQTT port " + mqttPort);
    }

    @AfterAll
    static void teardown() {
        if (mqttServer != null) {
            mqttServer.stop();
        }
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
                        clientCall.request(1);
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
                    final Optional<ByteBuffer> correlationData = mqtt5Publish.getCorrelationData();
                    byte[] bytes;
                    if (correlationData.isPresent()) {
                        final ByteBuffer byteBuffer = correlationData.get();
                        bytes = new byte[byteBuffer.remaining()];
                        byteBuffer.get(bytes);
                    } else {
                        bytes = new byte[]{};
                    }
                    System.out.println("TEST-OBSERVER: Received publish on topic " + mqtt5Publish.getTopic() + ", responseTopic: " + mqtt5Publish.getResponseTopic() + ", correlationID: " + new String(bytes) + ", payload: " + new String(mqtt5Publish.getPayloadAsBytes()));
                })
                .send()
                .join();
    }

    @Test
    void pingPong_shared_subscription() {
        mqttServer = MqttServer.builder()
                .clientId("server")
                .topicPrefix("$share/group1/" + topicPrefix)
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

        PingRequest request = PingRequest.newBuilder()
                .setPayload("Hello")
                .build();
        PingResponse response = client.ping(request);
        assertThat(response.getPayload()).isEqualTo("Pong: Hello");

        mqttServer.stop();
        mqttChannel.close();
    }

    @Test
    void pingPong_pingService_noServer() {
        // Create the gRPC channel using the MqttChannel implementation
        assertThatThrownBy(() -> MqttChannel.builder()
                .clientId("client")
                .topicPrefix(topicPrefix)
                .brokerAddress(brokerUrl)
                .port(mqttPort)
                .intercept(getSampleInterceptor())
                .pingDeadlineSeconds(5)
                .build(), "starting the channel must time out due to missing backend");
    }

    // TODO test case: server is present but does not implement method

    @Test
    void pingPong_noPingService_noServer() {
        // Create the gRPC channel using the MqttChannel implementation
        mqttChannel = MqttChannel.builder()
                .clientId("client")
                .topicPrefix(topicPrefix)
                .brokerAddress(brokerUrl)
                .port(mqttPort)
                .intercept(getSampleInterceptor())
                .pingService(false)
                .timeout(2)
                .build();
        client = PingServiceGrpc.newBlockingStub(mqttChannel);

        PingRequest request = PingRequest.newBuilder()
                .setPayload("Hello")
                .build();
        assertThatThrownBy(() -> client.withDeadlineAfter(5, TimeUnit.SECONDS).ping(request), "client query must time out");
        mqttChannel.close();
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

    @Test
    void pingRequest_ExpectPongResponse() {
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

        PingRequest request = PingRequest.newBuilder()
                .setPayload("Hello")
                .build();
        PingResponse response = client.ping(request);
        assertThat(response.getPayload()).isEqualTo("Pong: Hello");

        mqttServer.stop();
        mqttChannel.close();
    }

    @Test
    void pingPong_twice() throws InterruptedException {
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

        PingRequest request = PingRequest.newBuilder()
                .setPayload("Hello")
                .build();
        client.ping(request);
        PingResponse response = client.ping(request);
        assertThat(response.getPayload()).isEqualTo("Pong: Hello");

        mqttServer.stop();
        mqttChannel.close();
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
