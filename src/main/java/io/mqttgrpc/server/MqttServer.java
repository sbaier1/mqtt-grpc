package io.mqttgrpc.server;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.mqttgrpc.Constants.ENDPOINTS_INFIX;

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


    public MqttServer(final Mqtt5Client mqttClient,
                      final List<BindableService> services,
                      final List<ServerInterceptor> interceptors,
                      final String topicPrefix,
                      final MqttQos qos) {
        this.mqttClient = mqttClient;
        this.services = services;
        this.interceptors = interceptors;
        this.topicPrefix = topicPrefix;
        this.qos = qos;
    }

    public static MqttServerBuilder builder() {
        return new MqttServerBuilder();
    }

    public CompletableFuture<Void> start() {
        return CompletableFuture.runAsync(() -> {
            for (BindableService service : services) {
                ServerServiceDefinition serviceDefinition = service.bindService();
                for (ServerMethodDefinition<?, ?> methodDefinition : serviceDefinition.getMethods()) {
                    String methodName = methodDefinition.getMethodDescriptor().getFullMethodName();
                    String topicFilter = topicPrefix + ENDPOINTS_INFIX + methodName;
                    final MqttMessageHandler handler = new MqttMessageHandler(mqttClient, serviceDefinition, this.interceptors, topicPrefix, qos);
                    // TODO: Callback probably doesn't perform well at scale
                    mqttClient.toAsync().subscribeWith()
                            .topicFilter(topicFilter)
                            .qos(qos)
                            .callback(handler::onMqttMessageReceived)
                            .send()
                            .whenComplete((subAck, throwable) -> {
                                if (throwable != null) {
                                    log.error("Failed to subscribe to topic: {}", topicFilter);
                                } else {
                                    log.debug("Successfully subscribed to topic: {}", topicFilter);
                                }
                            });
                }
            }
        });
    }

    public void stop() {
        mqttClient.toAsync().disconnect();
    }

    public static class MqttMessageHandler {
        private final Mqtt5Client mqttClient1;
        private final ServerServiceDefinition registry;
        private final List<ServerInterceptor> interceptors;
        private final String topicPrefix1;
        private final MqttQos qos1;

        public MqttMessageHandler(final Mqtt5Client mqttClient,
                                  final ServerServiceDefinition registry,
                                  final List<ServerInterceptor> interceptors,
                                  final String topicPrefix,
                                  final MqttQos qos) {
            mqttClient1 = mqttClient;
            this.registry = registry;
            this.interceptors = interceptors;
            topicPrefix1 = topicPrefix;
            qos1 = qos;
        }

        public void onMqttMessageReceived(Mqtt5Publish publish) {
            String methodName = getMethodNameFromPublish(topicPrefix1, publish);
            Metadata headers = new Metadata(); // You might need to extract headers from the MQTT message

            ServerMethodDefinition<?, ?> methodDefinition = registry.getMethod(methodName);
            if (methodDefinition == null) {
                // Handle method not found
                // You might want to send an MQTT message with an appropriate error status
                log.error("Could not find method for inferred method name {}. Cannot handle request. Original topic name: {}", methodName, publish.getTopic());
                // Try ensuring that the callback fails, so we don't ack the PUBLISH. TBD if this works at all. something like this could be useful though. not sure.
                throw new IllegalStateException("Failed to handle request on method " + methodName);
            }

            dispatchMessage(publish, methodDefinition, headers);
        }

        private String getMethodNameFromPublish(String topicPrefix, Mqtt5Publish publish) {
            String topic = publish.getTopic().toString();
            // Remove shared group prefix if any (it's not present in received publishes)
            final String topicPrefixWithoutSharedGroup = topicPrefix
                    .replaceAll("^\\$share/[^/]+/", "");
            return topic
                    .substring((topicPrefixWithoutSharedGroup + ENDPOINTS_INFIX).length());
        }

        private <ReqT, RespT> void dispatchMessage(Mqtt5Publish publish, ServerMethodDefinition<ReqT, RespT> methodDefinition, Metadata headers) {
            MethodDescriptor<ReqT, RespT> methodDescriptor = methodDefinition.getMethodDescriptor();
            ReqT request;

            request = methodDescriptor.getRequestMarshaller().parse(new ByteArrayInputStream(publish.getPayloadAsBytes()));

            ServerCallHandler<ReqT, RespT> callHandler = methodDefinition.getServerCallHandler();
            for (ServerInterceptor interceptor : interceptors) {
                callHandler = InternalServerInterceptors.interceptCallHandlerCreate(interceptor, callHandler);
            }
            // TODO: is something missing here to invoke the actual impl method? not sure

            // Implement a custom ServerCall for MQTT
            MqttServerCall<ReqT, RespT> serverCall = new MqttServerCall<>(mqttClient1, publish, methodDescriptor, qos1);
            ServerCall.Listener<ReqT> listener = callHandler.startCall(serverCall, headers);

            // If the call is unary or server-streaming, you can handle it directly here
            if (methodDescriptor.getType() == MethodDescriptor.MethodType.UNARY || methodDescriptor.getType() == MethodDescriptor.MethodType.SERVER_STREAMING) {
                listener.onMessage(request);
                listener.onHalfClose();
            }
        }
    }


}
