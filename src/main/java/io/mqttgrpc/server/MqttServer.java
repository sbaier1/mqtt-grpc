package io.mqttgrpc.server;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.mqttgrpc.Constants.ENDPOINTS_INFIX;
import static io.mqttgrpc.Constants.NOT_IMPLEMENTED_PAYLOAD;

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


    public void start() {
        mqttClient.toAsync().subscribeWith()
                .topicFilter(topicPrefix + ENDPOINTS_INFIX + "#")
                .qos(qos)
                .send()
                .join();

        final Mqtt5RxClient rx = mqttClient.toRx();
        rx.publishes(MqttGlobalPublishFilter.ALL)
                .subscribe(this::onMqttMessageReceived);
    }

    private void onMqttMessageReceived(Mqtt5Publish publish) {
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

    public void stop() {
        mqttClient.toAsync().disconnect();
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

    private <ReqT, RespT> void dispatchMessage(Mqtt5Publish publish, ServerMethodDefinition<ReqT, RespT> methodDefinition, Metadata headers) {
        MethodDescriptor<ReqT, RespT> methodDescriptor = methodDefinition.getMethodDescriptor();
        ReqT request;


        final Optional<ByteBuffer> correlationData = publish.getCorrelationData();
        byte[] correlationBytes = new byte[correlationData.get().remaining()];
        correlationData.get().get(correlationBytes);
        String correlationId = new String(correlationBytes, StandardCharsets.UTF_8);
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
            listener.onMessage(request);
            listener.onHalfClose();
        }
    }


}
