package client;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientBuilder;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A gRPC Channel implementation that uses MQTT5 as its transport protocol
 */
public class MqttChannel extends Channel {
    private static final Logger log = LoggerFactory.getLogger(MqttChannel.class);

    private final Mqtt5AsyncClient mqttClient;
    private final String topic;
    private final String clientId;

    private final List<String> registeredResponseTopics;

    private final Map<String, ClientCall.Listener<?>> listeners;

    private MqttChannel(Mqtt5AsyncClient mqttClient, String topic, String clientId) {
        this.mqttClient = mqttClient;
        this.topic = topic;
        this.clientId = clientId;
        registeredResponseTopics = new CopyOnWriteArrayList<>();
        listeners = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private static <T> ClientCall.Listener<T> castListener(ClientCall.Listener<?> listener) {
        return (ClientCall.Listener<T>) listener;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String authority() {
        // Return the appropriate authority, or null if it doesn't apply in your use case
        return null;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
        String correlationId = UUID.randomUUID().toString();
        final String methodTopic = topic + method.getFullMethodName();
        final String methodResponseTopic = methodTopic + "/responses/" + clientId;
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
                                        String correlationId = new String(correlationBytes, StandardCharsets.UTF_8);
                                        if (listeners.containsKey(correlationId)) {
                                            InputStream payloadStream = new ByteArrayInputStream(publish.getPayloadAsBytes());
                                            Object response = method.getResponseMarshaller().parse(payloadStream);
                                            if (response != null) {
                                                final Listener<?> listener = listeners.get(correlationId);
                                                // Ensure we notify listener in the caller's executor
                                                CompletableFuture.runAsync(() -> {
                                                    listener.onHeaders(headers);
                                                    castListener(listener).onMessage(response);
                                                    listener.onReady();
                                                    listener.onClose(Status.OK, new Metadata());
                                                    // Handled, remove again
                                                    listeners.remove(correlationId);
                                                }, callOptions.getExecutor());
                                            } else {
                                                log.error("Failed to unmarshal protobuf message on topic {}", publish.getTopic());
                                            }
                                        } else {
                                            log.warn("Could not find a listener for message with correlation ID {}, ignoring", correlationId);
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
                                    log.error("Failed to subscribe to topic {}", topic);
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
                        .topic(methodTopic)
                        .responseTopic(methodResponseTopic)
                        .correlationData(correlationId.getBytes(StandardCharsets.UTF_8))
                        .payload(payloadBuffer)
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .send()
                        .whenComplete((publish, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to publish on topic {}", methodTopic);
                                // Handle publish failure
                            }
                        });
            }
        };
    }

    public static class Builder {
        private final MqttClientBuilder clientBuilder;
        private int port;

        private String brokerUrl;
        private Builder() {
            clientBuilder = MqttClient.builder();
        }
        private String clientId;
        private String topic;


        public Builder brokerUrl(String brokerUrl) {
            this.brokerUrl = brokerUrl;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
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

            if (topic == null || topic.isBlank()) {
                throw new IllegalStateException("topic must not be empty");
            }
            return new MqttChannel(mqttClient, topic, clientId);
        }
    }
}
