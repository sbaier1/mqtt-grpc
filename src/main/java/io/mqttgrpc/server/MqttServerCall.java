package io.mqttgrpc.server;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;


/**
 * @author Simon Baier
 */

public class MqttServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
    private static final Logger log = LoggerFactory.getLogger(MqttServerCall.class);

    private final Mqtt5Client mqttClient;
    private final Mqtt5Publish publish;
    private final MethodDescriptor<ReqT, RespT> methodDescriptor;
    private final MqttQos qos;
    private boolean sendCalled = false;

    public MqttServerCall(Mqtt5Client mqttClient, Mqtt5Publish publish, MethodDescriptor<ReqT, RespT> methodDescriptor, MqttQos qos) {
        this.mqttClient = mqttClient;
        this.publish = publish;
        this.methodDescriptor = methodDescriptor;
        this.qos = qos;
    }

    @Override
    public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
        return methodDescriptor;
    }

    @Override
    public void request(int numMessages) {
        // In this example, we ignore flow control and assume the server can handle messages as they arrive.
    }

    @Override
    public void sendHeaders(Metadata headers) {
        // We ignore the headers in this example. You may want to extract headers from the MQTT message and
        // convert them to gRPC metadata, or implement a mechanism to send headers back to the client.
    }

    @Override
    public void sendMessage(RespT message) {
        if (sendCalled) {
            throw new IllegalStateException("A message was already sent. Only one message is allowed in unary calls.");
        }

        InputStream responseStream = methodDescriptor.getResponseMarshaller().stream(message);
        byte[] responsePayload;

        try {
            responsePayload = responseStream.readAllBytes();
        } catch (IOException e) {
            // Handle the exception, e.g., by sending an error status
            close(Status.INTERNAL.withDescription("Failed to serialize response message."), new Metadata());
            return;
        }

        // Send the responsePayload as an MQTT message back to the client
        final Optional<MqttTopic> responseTopicOptional = publish.getResponseTopic();
        if (responseTopicOptional.isPresent()) {
            //MqttMessage responseMessage = new MqttMessage(responseTopic, MqttQos.AT_LEAST_ONCE, responsePayload);
            final MqttTopic responseTopic = responseTopicOptional.get();
            final Optional<ByteBuffer> correlationData = publish.getCorrelationData();
            byte[] correlationBytes = new byte[correlationData.get().remaining()];
            correlationData.get().get(correlationBytes);
            String correlationId = new String(correlationBytes, StandardCharsets.UTF_8);
            log.debug("Responding to gRPC call on method {}, topic {}, correlation ID {}", methodDescriptor.getFullMethodName(), responseTopic, correlationId);
            mqttClient.toAsync().publishWith()
                    .topic(responseTopic)
                    .qos(qos)
                    .payload(responsePayload)
                    .correlationData(ByteBuffer.wrap(correlationBytes))
                    .send()
                    .whenComplete((mqtt5PublishResult, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to publish response for request received on topic {} to response topic {}", publish.getTopic(), responseTopic);
                        }
                    });
        } else {
            throw new IllegalStateException("Response topic must be present in incoming publish in order to respond to gRPC request. Ignoring publish received on topic " + publish.getTopic());
        }
        sendCalled = true;
    }

    @Override
    public void close(Status status, Metadata trailers) {
        if (status.isOk()) {
            // The call completed successfully. If sendMessage() wasn't called yet, you may want to handle this case.
        } else {
            // The call failed. Send an error status back to the client, e.g., as an MQTT message.
            // This can be done using a different topic, or by encoding the error status in the MQTT message payload.
        }
    }

    @Override
    public boolean isCancelled() {
        // Implement cancellation handling based on your MQTT library, or return false if you don't support cancellation.
        return false;
    }
}
