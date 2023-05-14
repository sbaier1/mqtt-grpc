package server;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A gRPC Server implementation that uses MQTT5 as its transport protocol
 *
 * @author Simon Baier
 */
public class MqttServer {
    public static final String TOPIC_PREFIX = "grpc/server/";
    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private final Mqtt5Client mqttClient;
    private final List<BindableService> services;
    private final List<ServerInterceptor> interceptors;

    public MqttServer(Mqtt5Client mqttClient, List<BindableService> services, List<ServerInterceptor> interceptors) {
        this.mqttClient = mqttClient;
        this.services = services;
        this.interceptors = interceptors;
    }

    public static MqttServerBuilder newBuilder() {
        return new MqttServerBuilder();
    }

    public CompletableFuture<Void> start() {
        return CompletableFuture.runAsync(() -> {
            for (BindableService service : services) {
                ServerServiceDefinition serviceDefinition = service.bindService();
                for (ServerMethodDefinition<?, ?> methodDefinition : serviceDefinition.getMethods()) {
                    String methodName = methodDefinition.getMethodDescriptor().getFullMethodName();
                    String topicFilter = TOPIC_PREFIX + methodName;
                    final MqttMessageHandler handler = new MqttMessageHandler(mqttClient, serviceDefinition, this.interceptors);
                    // TODO: Callback probably doesn't perform well at scale
                    mqttClient.toAsync().subscribeWith()
                            .topicFilter(topicFilter)
                            .qos(MqttQos.AT_LEAST_ONCE)
                            .callback(handler::onMqttMessageReceived)
                            .send()
                            .whenComplete((subAck, throwable) -> {
                                if (throwable != null) {
                                    System.err.println("Failed to subscribe to topic: " + topicFilter);
                                } else {
                                    System.out.println("Successfully subscribed to topic: " + topicFilter);
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

        public MqttMessageHandler(Mqtt5Client mqttClient, ServerServiceDefinition registry, List<ServerInterceptor> interceptors) {
            mqttClient1 = mqttClient;
            this.registry = registry;
            this.interceptors = interceptors;
        }

        public void onMqttMessageReceived(Mqtt5Publish publish) {
            String methodName = getMethodNameFromPublish(publish);
            Metadata headers = new Metadata(); // You might need to extract headers from the MQTT message

            ServerMethodDefinition<?, ?> methodDefinition = registry.getMethod(methodName);
            if (methodDefinition == null) {
                // Handle method not found
                // You might want to send an MQTT message with an appropriate error status
                log.error("Could not find method for inferred method name {}. Cannot handle request.", methodName);
                // Try ensuring that the callback fails, so we don't ack the PUBLISH. TBD if this works at all. something like this could be useful though. not sure.
                throw new IllegalStateException("Failed to handle request on method" + methodName);
            }

            dispatchMessage(publish, methodDefinition, headers);
        }

        private String getMethodNameFromPublish(Mqtt5Publish publish) {
            String topic = publish.getTopic().toString();
            return topic.substring("grpc/server/".length());
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
            MqttServerCall<ReqT, RespT> serverCall = new MqttServerCall<>(mqttClient1, publish, methodDescriptor);
            ServerCall.Listener<ReqT> listener = callHandler.startCall(serverCall, headers);

            // If the call is unary or server-streaming, you can handle it directly here
            if (methodDescriptor.getType() == MethodDescriptor.MethodType.UNARY || methodDescriptor.getType() == MethodDescriptor.MethodType.SERVER_STREAMING) {
                listener.onMessage(request);
                listener.onHalfClose();
            }
        }
    }


}
