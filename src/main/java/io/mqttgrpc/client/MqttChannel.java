package io.mqttgrpc.client;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientBuilder;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConnectionConfig;
import com.hivemq.client.mqtt.mqtt5.message.Mqtt5ReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static io.mqttgrpc.Constants.*;

/**
 * A gRPC Channel implementation that uses MQTT5 as its transport protocol
 */
public class MqttChannel extends Channel {
    private static final Logger log = LoggerFactory.getLogger(MqttChannel.class);

    private final Mqtt5AsyncClient mqttClient;
    private final String topicPrefix;
    private final String clientId;

    private final List<String> registeredResponseTopics;

    private final Map<String, ClientCall.Listener<?>> listeners;
    private final List<ClientInterceptor> interceptors;
    private final Channel actualChannel;

    private MqttChannel(Mqtt5AsyncClient mqttClient,
                        String topicPrefix,
                        String clientId,
                        List<ClientInterceptor> interceptors) {
        this.mqttClient = mqttClient;
        this.topicPrefix = topicPrefix;
        this.clientId = clientId;
        registeredResponseTopics = new CopyOnWriteArrayList<>();
        listeners = new ConcurrentHashMap<>();
        this.interceptors = interceptors;
        if (interceptors.size() > 0) {
            actualChannel = ClientInterceptors.intercept(new ActualMqttChannel(), interceptors);
        } else {
            actualChannel = new ActualMqttChannel();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> ClientCall.Listener<T> castListener(ClientCall.Listener<?> listener) {
        return (ClientCall.Listener<T>) listener;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return actualChannel.newCall(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
        final Optional<Mqtt5ClientConnectionConfig> config = mqttClient.getConfig().getConnectionConfig();
        return config.map(mqtt5ClientConnectionConfig -> mqtt5ClientConnectionConfig.getTransportConfig().getServerAddress().getHostName())
                .orElse(null);
    }

    public static class Builder {
        private final MqttClientBuilder clientBuilder;

        private boolean pingService;
        private int pingDeadlineSeconds;
        private int port;

        private String brokerUrl;
        private String topicPrefix;

        private String clientId;
        private final List<ClientInterceptor> interceptors;

        private Builder() {
            clientBuilder = MqttClient.builder();
            pingService = true;
            pingDeadlineSeconds = 30;
            interceptors = new ArrayList<>();
        }

        public Builder brokerAddress(String brokerUrl) {
            this.brokerUrl = brokerUrl;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder topicPrefix(String topicPrefix) {
            this.topicPrefix = topicPrefix;
            return this;
        }

        // Whether the topic prefix should be checked whether it has a MqttServer which will respond
        public Builder pingService(boolean pingService) {
            this.pingService = pingService;
            return this;
        }

        public Builder pingDeadlineSeconds(int pingDeadlineSeconds) {
            this.pingDeadlineSeconds = pingDeadlineSeconds;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder intercept(ClientInterceptor... interceptor) {
            this.interceptors.addAll(List.of(interceptor));
            return this;
        }

        /**
         * Get the client builder for adjusting additional parameters of the client as necessary.
         * NOTE: some parameters, such as clientId and serverHost will be set by the builder.
         */
        public MqttClientBuilder getClientBuilder() {
            return clientBuilder;
        }

        public MqttChannel build() {
            Mqtt5AsyncClient mqttClient = clientBuilder
                    .identifier(clientId)
                    .serverHost(brokerUrl)
                    .serverPort(port)
                    .useMqttVersion5()
                    .buildAsync();

            mqttClient.connect().join();

            // Optionally, verify there's _some_ backend service that will respond
            // (does not check any implemented methods, just an initial check to ensure we don't publish into nothingness)
            pingService(mqttClient);

            if (topicPrefix == null || topicPrefix.isBlank()) {
                throw new IllegalStateException("topic must not be empty");
            }
            return new MqttChannel(mqttClient, topicPrefix, clientId, interceptors);
        }

        private void pingService(Mqtt5AsyncClient mqttClient) {
            if (pingService) {
                final String pingResponseTopic = topicPrefix + RESPONSES_INFIX + clientId + "/ping";
                final CountDownLatch latch = new CountDownLatch(1);
                final Mqtt5SubAck suback = mqttClient.subscribeWith()
                        .topicFilter(pingResponseTopic)
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .callback(mqtt5Publish -> {
                            latch.countDown();
                        })
                        .send().join();
                if (suback.getReasonCodes().stream().anyMatch(Mqtt5ReasonCode::isError)) {
                    log.error("Failed to subscribe to ping response topic, suback: {}", suback);
                    throw new IllegalStateException("Failed to subscribe to ping response topic");
                }
                final Mqtt5PublishResult publishResult = mqttClient.toBlocking()
                        .publishWith()
                        .topic(topicPrefix + PING_SUFFIX)
                        .payload("ping".getBytes())
                        .responseTopic(pingResponseTopic)
                        .send();
                if (publishResult.getError().isPresent()) {
                    log.error("Failed to publish ping to backend service", publishResult.getError().get());
                    throw new IllegalStateException("Could not ping backend service");
                }
                try {
                    final boolean reached = latch.await(pingDeadlineSeconds, TimeUnit.SECONDS);
                    if (!reached) {
                        throw new IllegalStateException("Failed to ping service in " + pingDeadlineSeconds + "s");
                    }
                    log.debug("Backend service is present, proceeding");
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Interrupted while pinging service");
                }
            }
        }
    }

    public final class ActualMqttChannel extends Channel {

        @Override
        public String authority() {
            // Return the appropriate authority, or null if it doesn't apply in your use case
            return null;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            String correlationId = UUID.randomUUID().toString();
            final String methodQueryTopic = topicPrefix + ENDPOINTS_INFIX + method.getFullMethodName();
            final String methodResponseTopic = topicPrefix + RESPONSES_INFIX + clientId + method.getFullMethodName();

            return new ClientCall<>() {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    listeners.put(correlationId, responseListener);
                    if (!registeredResponseTopics.contains(methodResponseTopic)) {
                        mqttClient.subscribeWith()
                                .topicFilter(methodResponseTopic)
                                .qos(MqttQos.AT_LEAST_ONCE)
                                .callback(publish -> {
                                    try {
                                        final Optional<ByteBuffer> correlationData = publish.getCorrelationData();
                                        if (correlationData.isEmpty()) {
                                            log.error("No correlation data found in PUBLISH on topic {}, ignoring", publish.getTopic());
                                        } else {
                                            byte[] correlationBytes = new byte[correlationData.get().remaining()];
                                            correlationData.get().get(correlationBytes);
                                            String correlationId1 = new String(correlationBytes, StandardCharsets.UTF_8);
                                            if (listeners.containsKey(correlationId1)) {
                                                InputStream payloadStream = new ByteArrayInputStream(publish.getPayloadAsBytes());
                                                Object response = method.getResponseMarshaller().parse(payloadStream);
                                                if (response != null) {
                                                    final Listener<?> listener = listeners.get(correlationId1);
                                                    // Ensure we notify listener in the caller's executor
                                                    CompletableFuture.runAsync(() -> {
                                                        listener.onHeaders(headers);
                                                        castListener(listener).onMessage(response);
                                                        listener.onReady();
                                                        listener.onClose(Status.OK, new Metadata());
                                                        // Handled, remove again
                                                        listeners.remove(correlationId1);
                                                    }, callOptions.getExecutor());
                                                } else {
                                                    if (Arrays.equals(NOT_IMPLEMENTED_PAYLOAD, publish.getPayloadAsBytes())) {
                                                        log.error("Received method not implemented on topic {}", publish.getTopic());
                                                    } else {
                                                        log.error("Failed to unmarshal protobuf message on topic {}", publish.getTopic());
                                                    }
                                                }
                                            } else {
                                                log.warn("Could not find a listener for message with correlation ID {}, ignoring", correlationId1);
                                            }
                                        }
                                    } catch (Exception ex) {
                                        log.error("Unexpected error in response callback", ex);
                                    }
                                })
                                .send()
                                .whenComplete((subAck, throwable) -> {
                                    if (throwable != null) {
                                        // Handle subscription failure
                                        log.error("Failed to subscribe to topic {}", topicPrefix);
                                    }
                                });
                        registeredResponseTopics.add(methodResponseTopic);
                    }
                }

                @Override
                public void request(int numMessages) {
                    // No-op
                }

                @Override
                public void cancel(String message, Throwable cause) {
                    log.error("Canceling call. message {} cause", message, cause);
                    mqttClient.disconnect();
                }

                @Override
                public void halfClose() {
                    // No-op
                }

                @Override
                public void sendMessage(ReqT message) {
                    // Serialize the message using the method's marshaller
                    InputStream serializedMessageStream = method.getRequestMarshaller().stream(message);
                    final ByteBuffer payloadBuffer;
                    try {
                        payloadBuffer = ByteBuffer.wrap(serializedMessageStream.readAllBytes());
                    } catch (IOException e) {
                        log.error("Failed to serialize request", e);
                        return;
                    }
                    mqttClient.toAsync().publishWith()
                            .topic(methodQueryTopic)
                            .responseTopic(methodResponseTopic)
                            .correlationData(correlationId.getBytes(StandardCharsets.UTF_8))
                            .payload(payloadBuffer)
                            .qos(MqttQos.AT_LEAST_ONCE)
                            .send()
                            .whenComplete((publish, throwable) -> {
                                if (throwable != null) {
                                    log.error("Failed to publish on topic {}", methodQueryTopic);
                                    // Handle publish failure
                                }
                            });
                }
            };
        }
    }
}
