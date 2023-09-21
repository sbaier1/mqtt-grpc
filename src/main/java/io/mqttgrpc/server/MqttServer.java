package io.mqttgrpc.server;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.rx.FlowableWithSingle;
import io.grpc.*;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.mqttgrpc.Constants.*;

/**
 * A gRPC Server implementation that uses MQTT5 as its transport protocol
 *
 * @author Simon Baier
 */
public class MqttServer {
    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private final Mqtt5Client mqttClient;
    private final List<BindableService> services;
    private final List<ServerInterceptor> interceptors;
    private final String topicPrefix;
    private final MqttQos qos;
    private final boolean respondNotImplemented;
    private final String topicPrefixWithoutSharedGroup;
    private Disposable messageHandler;
    private Disposable pingResponderHandler;


    public MqttServer(final Mqtt5Client mqttClient,
                      final List<BindableService> services,
                      final List<ServerInterceptor> interceptors,
                      final String topicPrefix,
                      final MqttQos qos, boolean respondNotImplemented) {
        this.mqttClient = mqttClient;
        this.services = services;
        this.interceptors = interceptors;
        this.topicPrefix = topicPrefix;
        this.qos = qos;
        this.respondNotImplemented = respondNotImplemented;
        // Remove shared group prefix if any (it's not present in received publishes)
        topicPrefixWithoutSharedGroup = topicPrefix
                .replaceAll("^\\$share/[^/]+/", "");
    }

    public static MqttServerBuilder builder() {
        return new MqttServerBuilder();
    }

    private static String getCorrelationId(Mqtt5Publish publish) {
        final Optional<ByteBuffer> correlationData = publish.getCorrelationData();
        byte[] correlationBytes = new byte[correlationData.get().remaining()];
        correlationData.get().get(correlationBytes);
        return new String(correlationBytes, StandardCharsets.UTF_8);
    }

    public void start() {
        addEndpointsResponder();
        addServicePingResponder();
    }

    private void addEndpointsResponder() {
        final String endpointTopicFilter = topicPrefix + ENDPOINTS_INFIX + "#";
        mqttClient.toAsync().subscribeWith()
                .topicFilter(endpointTopicFilter)
                .qos(qos)
                .send()
                .join();

        messageHandler = mqttClient.toRx().subscribePublishesWith()
                .topicFilter(endpointTopicFilter)
                .qos(qos)
                .applySubscribe()
                .doOnSingle(mqtt5SubAck -> log.info("Subscribed endpoints topic {}. Suback {}", endpointTopicFilter, mqtt5SubAck))
                .doOnError(throwable -> log.error("Failed to subscribe to endpoints topic '{}'", endpointTopicFilter, throwable))
                .subscribe(this::onMqttMessageReceived, throwable -> {
                    if (throwable instanceof MqttGrpcException) {
                        log.error("Uncaught error in gRPC call. topic {}, response topic {}", ((MqttGrpcException) throwable).getPublish().getTopic(), ((MqttGrpcException) throwable).getPublish().getResponseTopic(), throwable);
                    } else {
                        log.error("Uncaught error in gRPC call.", throwable);
                    }
                });
    }

    private void addServicePingResponder() {
        final String pingTopic = topicPrefix + PING_SUFFIX;
        final FlowableWithSingle<Mqtt5Publish, Mqtt5SubAck> publishFlowable = mqttClient.toRx()
                .subscribePublishesWith()
                .topicFilter(pingTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .applySubscribe();

        pingResponderHandler = publishFlowable
                .doOnSingle(mqtt5SubAck -> log.info("Subscribed ping topic {}. Suback {}", pingTopic, mqtt5SubAck))
                .doOnError(throwable -> log.error("Failed to subscribe to ping topic '{}'", pingTopic, throwable))
                .subscribe(mqtt5Publish -> {
                    final Optional<MqttTopic> topicOptional = mqtt5Publish.getResponseTopic();
                    topicOptional.ifPresent(mqttTopic -> mqttClient.toRx()
                            .publish(Flowable.just(Mqtt5Publish.builder()
                                    .topic(mqttTopic)
                                    .qos(MqttQos.AT_LEAST_ONCE)
                                    .payload("OK".getBytes())
                                    .build()))
                            .subscribe(mqtt5PublishResult -> mqtt5PublishResult.getError()
                                    .ifPresentOrElse(throwable -> log.error("Failed to respond to service ping", throwable),
                                            () -> log.debug("Responded to ping on topic {}", mqttTopic)))
                            .dispose());
                });
    }

    private void respondNotImplemented(Mqtt5Publish publish) {
        final Optional<MqttTopic> topic = publish.getResponseTopic();
        if (respondNotImplemented && topic.isPresent()) {
            final Mqtt5PublishBuilder.Send.Complete<CompletableFuture<Mqtt5PublishResult>> publishBuilder = mqttClient.toAsync()
                    .publishWith()
                    .topic(topic.get())
                    .qos(qos)
                    .payload(NOT_IMPLEMENTED_PAYLOAD);
            //noinspection ResultOfMethodCallIgnored
            publish.getCorrelationData().ifPresent(byteBuffer -> publishBuilder.correlationData(publish.getCorrelationData().get()));
            publishBuilder.send();
            log.debug("Sent a NOT_IMPLEMENTED response on topic {}", topic);
        } else {
            log.debug("No response topic present in unhandled PUBLISH on topic {}", publish.getTopic());
        }
    }


    private ServerServiceDefinition lookupServiceDefinitionForMethod(String methodName) {
        for (BindableService service : services) {
            ServerServiceDefinition serviceDefinition = service.bindService();
            if (serviceDefinition.getMethod(methodName) != null) {
                return serviceDefinition;
            }
        }
        return null;
    }

    private void onMqttMessageReceived(Mqtt5Publish publish) throws MqttGrpcException {
        try {
            if (!publish.getTopic().toString().startsWith(topicPrefixWithoutSharedGroup + ENDPOINTS_INFIX)) {
                log.trace("Ignoring publish on topic {} as it doesn't match the endpoint prefix.", publish.getTopic());
                return;
            }
            String methodName = getMethodNameFromPublish(publish);
            if (methodName == null) {
                log.warn("Could not find method name for topic {}, ignoring request.", publish.getTopic());
                return;
            }
            ServerServiceDefinition serviceDefinition = lookupServiceDefinitionForMethod(methodName);
            if (serviceDefinition == null) {
                log.error("Could not find service definition for method {}. Cannot handle request. Original topic {}", methodName, publish.getTopic());
                respondNotImplemented(publish);
                return;
            }
            ServerMethodDefinition<?, ?> methodDefinition = serviceDefinition.getMethod(methodName);
            if (methodDefinition == null) {
                log.error("Could not find method definition for method {}. Cannot handle request. Original topic {}", methodName, publish.getTopic());
                respondNotImplemented(publish);
                return;
            }
            Metadata headers = new Metadata(); // Additional logic can be added to extract headers from MQTT message if needed
            dispatchMessage(publish, methodDefinition, headers);
        } catch (Exception ex) {
            throw new MqttGrpcException(publish, ex);
        }
    }

    private String getMethodNameFromPublish(Mqtt5Publish publish) {
        String topic = publish.getTopic().toString();
        final int prefixEndpointLength = (topicPrefixWithoutSharedGroup + ENDPOINTS_INFIX).length();
        if (prefixEndpointLength < topic.length()) {
            return topic
                    .substring(prefixEndpointLength);
        } else {
            return null;
        }
    }

    public void stop() {
        if (messageHandler != null) {
            messageHandler.dispose();
        }
        if (pingResponderHandler != null) {
            pingResponderHandler.dispose();
        }
    }

    private <ReqT, RespT> void dispatchMessage(Mqtt5Publish publish, ServerMethodDefinition<ReqT, RespT> methodDefinition, Metadata headers) {
        MethodDescriptor<ReqT, RespT> methodDescriptor = methodDefinition.getMethodDescriptor();
        ReqT request;
        final String correlationId = getCorrelationId(publish);
        log.debug("Invoking method {} for publish on topic {}, correlation id {}", methodDefinition.getMethodDescriptor().getFullMethodName(), publish.getTopic(), correlationId);

        request = methodDescriptor.getRequestMarshaller().parse(new ByteArrayInputStream(publish.getPayloadAsBytes()));

        ServerCallHandler<ReqT, RespT> callHandler = methodDefinition.getServerCallHandler();
        for (ServerInterceptor interceptor : interceptors) {
            callHandler = InternalServerInterceptors.interceptCallHandlerCreate(interceptor, callHandler);
        }

        // Implement a custom ServerCall for MQTT
        MqttServerCall<ReqT, RespT> serverCall = new MqttServerCall<>(mqttClient, publish, methodDescriptor, qos);
        ServerCall.Listener<ReqT> listener = callHandler.startCall(serverCall, headers);

        // If the call is unary or server-streaming, you can handle it directly here
        if (methodDescriptor.getType() == MethodDescriptor.MethodType.UNARY || methodDescriptor.getType() == MethodDescriptor.MethodType.SERVER_STREAMING) {
            try {
                listener.onMessage(request);
                listener.onHalfClose();
            } catch (Exception ex) {
                log.error("Failed to invoke method {}, correlationId {}, response topic {}, error:", correlationId, publish.getResponseTopic(), methodDefinition.getMethodDescriptor().getFullMethodName(), ex);
                serverCall.close(Status.INTERNAL.withDescription("Internal server error"), new Metadata());
                listener.onCancel();
            }
        }
    }

    private static class MqttGrpcException extends Exception {
        private final Mqtt5Publish publish;

        public MqttGrpcException(Mqtt5Publish publish, Exception ex) {
            super(ex);
            this.publish = publish;
        }

        public Mqtt5Publish getPublish() {
            return publish;
        }
    }
}
