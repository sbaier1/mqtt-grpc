import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientSslConfig;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import io.grpc.*;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class MqttChannel extends Channel {

    private final Mqtt5AsyncClient mqttClient;
    private final String topic;
    private final String responseTopic;

    private MqttChannel(Mqtt5AsyncClient mqttClient, String topic, String responseTopic) {
        this.mqttClient = mqttClient;
        this.topic = topic;
        this.responseTopic = responseTopic;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
        return new ClientCall<ReqT, RespT>() {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                // No-op
            }

            @Override
            public void request(int numMessages) {
                // No-op
            }

            @Override
            public void cancel(String message, Throwable cause) {
                mqttClient.disconnect();
            }

            @Override
            public void halfClose() {
                // No-op
            }

            @Override
            public void sendMessage(ReqT message) {
                // Serialize the message using your custom marshaller
                byte[] payload = "".getBytes()/* TODO your serialization logic here */;

                String correlationId = UUID.randomUUID().toString();

                mqttClient.toAsync().publishWith()
                        .topic(topic)
                        .responseTopic(responseTopic)
                        .correlationData(correlationId.getBytes(StandardCharsets.UTF_8))
                        .payload(payload)
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .send()
                        .whenComplete((publish, throwable) -> {
                            if (throwable != null) {
                                // Handle publish failure
                            }
                        });
            }
        };
    }

    @Override
    public String authority() {
        // Return the appropriate authority, or null if it doesn't apply in your use case
        return null;
    }

    public static class Builder {
        private String brokerUrl;
        private String clientId;
        private String topic;
        private MqttClientSslConfig sslConfig;

        public Builder brokerUrl(String brokerUrl) {
            this.brokerUrl = brokerUrl;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public MqttChannel build() {
            Mqtt5AsyncClient mqttClient = MqttClient.builder()
                    .identifier(clientId)
                    .serverHost(brokerUrl)
                    .sslConfig(sslConfig)
                    .useMqttVersion5()
                    .buildAsync();

            mqttClient.connect().join();

            String responseTopic = topic + "/response/" + UUID.randomUUID().toString();
            mqttClient.subscribeWith()
                    .topicFilter(responseTopic)
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .callback(publish -> {
                        // Handle received messages, e.g., deserialize them using your custom marshaller
                        // and pass them to the responseListener
                    })
                    .send()
                    .whenComplete((subAck, throwable) -> {
                        if (throwable != null) {
                            // Handle subscription failure
                        }
                    });

            return new MqttChannel(mqttClient, topic, responseTopic);
        }

    }
}
