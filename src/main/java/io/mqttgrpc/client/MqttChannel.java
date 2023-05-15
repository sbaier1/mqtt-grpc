package io.mqttgrpc.client;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConnectionConfig;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
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
import java.util.function.Consumer;

import static io.mqttgrpc.Constants.*;

/**
 * A gRPC Channel implementation that uses MQTT5 as its transport protocol
 */
public class MqttChannel extends Channel {
    private static final Logger log = LoggerFactory.getLogger(MqttChannel.class);

    private final Mqtt5AsyncClient mqttClient;
    private final String topicPrefix;
    private final String clientId;
    private final int timeoutSeconds;

    private final List<String> registeredResponseTopics;


    // gRPC method -> correlation-id -> listener
    private final Map<String, Map<String, ClientCall.Listener<?>>> listeners;

    // Dumb initial timeout handling: store some scheduled futures, cancel them when the response arrives.
    private final Map<String, ScheduledFuture<?>> timeoutTasks;
    private final Channel actualChannel;
    private final MqttQos qos;
    private final ScheduledExecutorService timer;

    MqttChannel(Mqtt5AsyncClient mqttClient,
                String topicPrefix,
                String clientId,
                List<ClientInterceptor> interceptors,
                int timeoutSeconds,
                MqttQos qos) {
        this.mqttClient = mqttClient;
        this.topicPrefix = topicPrefix;
        this.clientId = clientId;
        this.timeoutSeconds = timeoutSeconds;
        this.qos = qos;
        this.timer = Executors.newSingleThreadScheduledExecutor();
        registeredResponseTopics = new CopyOnWriteArrayList<>();
        listeners = new ConcurrentHashMap<>();
        if (interceptors.size() > 0) {
            actualChannel = ClientInterceptors.intercept(new ActualMqttChannel(), interceptors);
        } else {
            actualChannel = new ActualMqttChannel();
        }
        timeoutTasks = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private static <T> ClientCall.Listener<T> castListener(ClientCall.Listener<?> listener) {
        return (ClientCall.Listener<T>) listener;
    }

    public static MqttChannelBuilder builder() {
        return new MqttChannelBuilder();
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


    public final class ActualMqttChannel extends Channel {

        @Override
        public String authority() {
            // Return the appropriate authority, or null if it doesn't apply in your use case
            return null;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            String correlationId = UUID.randomUUID().toString();
            final String methodName = method.getFullMethodName();
            final String methodQueryTopic = topicPrefix + ENDPOINTS_INFIX + methodName;
            final String methodResponseTopic = topicPrefix + RESPONSES_INFIX + clientId + methodName;

            return new ClientCall<>() {
                private Listener<RespT> callListener;

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    this.callListener = responseListener;
                    listeners.computeIfAbsent(methodName, s -> {
                        final ConcurrentHashMap<String, Listener<?>> correlationIdToListenersMap = new ConcurrentHashMap<>();
                        correlationIdToListenersMap.put(correlationId, callListener);
                        return correlationIdToListenersMap;
                    });
                    if (!registeredResponseTopics.contains(methodResponseTopic)) {
                        mqttClient.subscribeWith()
                                .topicFilter(methodResponseTopic)
                                .qos(qos)
                                .callback(methodCallback(headers))
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

                private Consumer<Mqtt5Publish> methodCallback(Metadata headers) {
                    return publish -> {
                        try {
                            final Optional<ByteBuffer> correlationData = publish.getCorrelationData();
                            if (correlationData.isEmpty()) {
                                log.warn("No correlation data found in PUBLISH on topic {}, ignoring", publish.getTopic());
                            } else {
                                byte[] correlationBytes = new byte[correlationData.get().remaining()];
                                correlationData.get().get(correlationBytes);
                                String correlationId = new String(correlationBytes, StandardCharsets.UTF_8);
                                final Map<String, Listener<?>> listenersForMethod = listeners.get(methodName);
                                if (listenersForMethod.containsKey(correlationId)) {
                                    InputStream payloadStream = new ByteArrayInputStream(publish.getPayloadAsBytes());
                                    Object response = method.getResponseMarshaller().parse(payloadStream);
                                    if (response != null) {
                                        final Listener<?> listener = listenersForMethod.get(correlationId);
                                        // Ensure we notify listener in the caller's executor
                                        CompletableFuture.runAsync(() -> {
                                            // Stop timeout task
                                            if (timeoutTasks.containsKey(correlationId)) {
                                                timeoutTasks.get(correlationId).cancel(true);
                                                timeoutTasks.remove(correlationId);
                                            }
                                            listener.onHeaders(headers);
                                            castListener(listener).onMessage(response);
                                            listener.onReady();
                                            listener.onClose(Status.OK, new Metadata());
                                            // Handled, remove again
                                            listenersForMethod.remove(correlationId);
                                        }, callOptions.getExecutor());
                                    } else {
                                        if (Arrays.equals(NOT_IMPLEMENTED_PAYLOAD, publish.getPayloadAsBytes())) {
                                            log.error("Received method not implemented on topic {}", publish.getTopic());
                                            if (listenersForMethod.containsKey(correlationId)) {
                                                listenersForMethod.get(correlationId).onClose(Status.UNIMPLEMENTED, new Metadata());
                                                listenersForMethod.remove(correlationId);
                                            }
                                        } else {
                                            log.error("Failed to unmarshal protobuf message on topic {}", publish.getTopic());
                                            if (listenersForMethod.containsKey(correlationId)) {
                                                listenersForMethod.get(correlationId).onClose(Status.INTERNAL, new Metadata());
                                                listenersForMethod.remove(correlationId);
                                            }
                                        }
                                    }
                                } else {
                                    log.warn("Could not find a listener for message with correlation ID {} on method {}, ignoring. Received on topic {}", correlationId, methodName, publish.getTopic());
                                }
                            }
                        } catch (Exception ex) {
                            log.error("Unexpected error in response callback", ex);
                        }
                    };
                }

                @Override
                public void request(int numMessages) {
                    // No-op
                }

                @Override
                public void cancel(String message, Throwable cause) {
                    log.error("Canceling call. message {} cause", message, cause);
                    callListener.onClose(Status.CANCELLED, new Metadata());
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
                    final CompletableFuture<Mqtt5PublishResult> sendFuture = mqttClient.toAsync().publishWith()
                            .topic(methodQueryTopic)
                            .responseTopic(methodResponseTopic)
                            .correlationData(correlationId.getBytes(StandardCharsets.UTF_8))
                            .payload(payloadBuffer)
                            .qos(qos)
                            .send();
                    // Timeout handling
                    sendFuture.thenRunAsync(() -> {
                        timeoutTasks.put(correlationId, timer.schedule(() -> {
                            log.warn("Request to method {} for correlation id {} timed out", method.getFullMethodName(), correlationId);
                            final Map<String, Listener<?>> correlationIdToListeners = listeners.get(method.getFullMethodName());
                            correlationIdToListeners.get(correlationId).onClose(Status.CANCELLED, new Metadata());
                            correlationIdToListeners.remove(correlationId);
                        }, timeoutSeconds, TimeUnit.SECONDS));
                    });
                    sendFuture
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
