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
    private final String clientId;
    private final int timeoutSeconds;

    private final Set<String> registeredResponseTopics;


    // gRPC method -> correlation-id -> call context
    private final Map<String, Map<String, CallContext>> contexts;

    // Dumb initial timeout handling: store some scheduled futures, cancel them when the response arrives.
    private final Map<String, ScheduledFuture<?>> timeoutTasks;
    private final Channel actualChannel;
    private final MqttQos qos;
    private final ScheduledExecutorService timeoutExecutor;

    private final Set<String> topicPrefixes;

    MqttChannel(Mqtt5AsyncClient mqttClient,
                String topicPrefix,
                String clientId,
                List<ClientInterceptor> interceptors,
                int timeoutSeconds,
                MqttQos qos) {
        this.mqttClient = mqttClient;
        this.clientId = clientId;
        this.timeoutSeconds = timeoutSeconds;
        this.qos = qos;
        this.timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        this.topicPrefixes = new HashSet<>();
        registeredResponseTopics = Collections.synchronizedSet(new HashSet<>());
        contexts = new ConcurrentHashMap<>();
        if (interceptors.size() > 0) {
            actualChannel = ClientInterceptors.intercept(new ActualMqttChannel(topicPrefix), interceptors);
        } else {
            actualChannel = new ActualMqttChannel(topicPrefix);
        }
        topicPrefixes.add(topicPrefix);
        timeoutTasks = new ConcurrentHashMap<>();
    }

    /**
     * Return a new "separate" Channel using this {@link MqttChannel} as a container.
     *
     * @param topicPrefix prefix for the new channel.
     * @return {@link Channel} to be used for other stubs.
     */
    public ActualMqttChannel getChannel(String topicPrefix) {
        if (topicPrefix == null || topicPrefix.isBlank()) {
            throw new IllegalStateException("topic prefix must not be blank");
        }
        if (topicPrefixes.contains(topicPrefix)) {
            throw new IllegalStateException("topic prefix " + topicPrefix + " is already in use, topic prefixes must be unique");
        }
        topicPrefixes.add(topicPrefix);
        return new ActualMqttChannel(topicPrefix);
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

    public void close() {
        this.mqttClient.disconnect();
    }

    // Tracks all necessary context for a single gRPC unary call
    private static final class CallContext {
        private ClientCall.Listener<?> listener;
        private CallOptions callOptions;
        private Metadata headers;

        public ClientCall.Listener<?> listener() {
            return listener;
        }

        public CallOptions callOptions() {
            return callOptions;
        }

        public CallContext setCallOptions(CallOptions callOptions) {
            this.callOptions = callOptions;
            return this;
        }

        public void setListener(ClientCall.Listener<?> listener) {
            this.listener = listener;
        }

        public CallContext setHeaders(Metadata headers) {
            this.headers = headers;
            return this;
        }

        public Metadata headers() {
            return headers;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (CallContext) obj;
            return Objects.equals(this.listener, that.listener) &&
                    Objects.equals(this.callOptions, that.callOptions) &&
                    Objects.equals(this.headers, that.headers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(listener, callOptions, headers);
        }

        @Override
        public String toString() {
            return "CallContext[" +
                    "listener=" + listener + ", " +
                    "callOptions=" + callOptions + ", " +
                    "headers=" + headers + ']';
        }
    }

    public final class ActualMqttChannel extends Channel {

        private final String topicPrefix;

        public ActualMqttChannel(final String topicPrefix) {
            this.topicPrefix = topicPrefix;
        }

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
            final String methodResponseTopic = topicPrefix + RESPONSES_INFIX + clientId + "/" + methodName;
            log.debug("Starting call on method {} with correlation id {}", methodName, correlationId);

            final Map<String, CallContext> methodContexts = contexts.computeIfAbsent(methodName, s -> new ConcurrentHashMap<>());
            methodContexts.put(correlationId, new CallContext().setCallOptions(callOptions));
            return new InternalClientCall<>(methodName, correlationId, methodResponseTopic, method, methodQueryTopic);
        }

        private <ReqT, RespT> Consumer<Mqtt5Publish> methodCallback(String methodName, MethodDescriptor<ReqT, RespT> method) {
            return publish -> {
                try {
                    final Optional<ByteBuffer> correlationData = publish.getCorrelationData();
                    if (correlationData.isEmpty()) {
                        log.warn("No correlation data found in PUBLISH on topic {}, ignoring", publish.getTopic());
                    } else {
                        byte[] correlationBytes = new byte[correlationData.get().remaining()];
                        correlationData.get().get(correlationBytes);
                        String correlationId = new String(correlationBytes, StandardCharsets.UTF_8);
                        final Map<String, CallContext> contextsForMethod = contexts.get(methodName);
                        if (contextsForMethod.containsKey(correlationId)) {
                            InputStream payloadStream = new ByteArrayInputStream(publish.getPayloadAsBytes());
                            Object response = method.getResponseMarshaller().parse(payloadStream);
                            if (response != null) {
                                final CallContext context = contextsForMethod.get(correlationId);
                                final ClientCall.Listener<?> listener = context.listener;
                                final CallOptions callOptions = context.callOptions;
                                final Metadata headers = context.headers();
                                // Ensure we notify listener in the caller's executor
                                CompletableFuture.runAsync(() -> {
                                    try {
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
                                        contextsForMethod.remove(correlationId);
                                        log.debug("Successfully finished gRPC call on method {} for correlation ID {}", methodName, correlationId);
                                    } catch (Exception ex) {
                                        log.error("Failed to notify caller, method {}, correlation ID {}", methodName, correlationId, ex);
                                    }
                                }, callOptions.getExecutor());
                            } else {
                                if (Arrays.equals(NOT_IMPLEMENTED_PAYLOAD, publish.getPayloadAsBytes())) {
                                    log.error("Received method not implemented on method {} for correlation ID {} on topic {}", methodName, correlationId, publish.getTopic());
                                    if (contextsForMethod.containsKey(correlationId)) {
                                        contextsForMethod.get(correlationId).listener().onClose(Status.UNIMPLEMENTED, new Metadata());
                                        contextsForMethod.remove(correlationId);
                                    }
                                } else {
                                    log.error("Failed to unmarshal protobuf message on method {} for correlation ID {} on topic {}", methodName, correlationId, publish.getTopic());
                                    if (contextsForMethod.containsKey(correlationId)) {
                                        contextsForMethod.get(correlationId).listener().onClose(Status.INTERNAL, new Metadata());
                                        contextsForMethod.remove(correlationId);
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

        private class InternalClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
            private final String methodName;
            private final String correlationId;
            private final String methodResponseTopic;
            private final MethodDescriptor<ReqT, RespT> method;
            private final String methodQueryTopic;
            private Listener<RespT> callListener;

            public InternalClientCall(String methodName, String correlationId, String methodResponseTopic, MethodDescriptor<ReqT, RespT> method, String methodQueryTopic) {
                this.methodName = methodName;
                this.correlationId = correlationId;
                this.methodResponseTopic = methodResponseTopic;
                this.method = method;
                this.methodQueryTopic = methodQueryTopic;
            }

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                this.callListener = responseListener;
                final Map<String, CallContext> correlationContexts = contexts.get(methodName);
                correlationContexts.get(correlationId)
                        .setHeaders(headers)
                        .setListener(responseListener);
                synchronized (registeredResponseTopics) {
                    if (!registeredResponseTopics.contains(methodResponseTopic)) {
                        log.debug("Subscribing to topic {}", methodResponseTopic);
                        mqttClient.subscribeWith()
                                .topicFilter(methodResponseTopic)
                                .qos(qos)
                                .callback(methodCallback(methodName, method))
                                .send()
                                .whenComplete((subAck, throwable) -> {
                                    if (throwable != null) {
                                        // Handle subscription failure
                                        log.error("Failed to subscribe to topic {}", topicPrefix);
                                    }
                                })
                                .join();
                        registeredResponseTopics.add(methodResponseTopic);
                    }
                }
            }

            @Override
            public void request(int numMessages) {
                // No-op
            }

            @Override
            public void cancel(String message, Throwable cause) {
                log.error("Canceling call. message {} cause", message, cause);
                callListener.onClose(Status.CANCELLED, new Metadata());
            }

            @Override
            public void halfClose() {
                // No-op
            }

            @Override
            public void sendMessage(ReqT message) {
                // Serialize the message using the method's marshaller
                final ByteBuffer payloadBuffer;
                try (InputStream serializedMessageStream = method.getRequestMarshaller().stream(message)) {
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
                    final Map<String, CallContext> correlationContexts = contexts.get(methodName);
                    final CallContext callContext = correlationContexts.get(correlationId);
                    if (callContext != null) {
                        if (callContext.callOptions().getDeadline() != null) {
                            // Deadline set by client, use that instead of the global timeout
                            callContext.callOptions().getDeadline().runOnExpiration(getTimeoutTask(), timeoutExecutor);
                        } else {
                            timeoutTasks.put(correlationId, timeoutExecutor.schedule(getTimeoutTask(), timeoutSeconds, TimeUnit.SECONDS));
                        }
                    }
                });
                sendFuture
                        .whenComplete((publish, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to publish on topic {}", methodQueryTopic);
                                // Handle publish failure
                            }
                        })
                        .join();
            }

            private Runnable getTimeoutTask() {
                return () -> {
                    final Map<String, CallContext> correlationContexts = contexts.get(method.getFullMethodName());
                    final CallContext callContext = correlationContexts.get(correlationId);
                    log.warn("Request to method {} for correlation id {} timed out", method.getFullMethodName(), correlationId);
                    CompletableFuture.runAsync(() -> {
                                final Listener<?> listener = callContext.listener;
                                log.debug("timed out, notifying listener {} for correlation id {}", listener, correlationId);
                                listener.onClose(Status.CANCELLED, new Metadata());
                            }, callContext.callOptions().getExecutor())
                            .join();
                    correlationContexts.remove(correlationId);
                };
            }
        }
    }
}
