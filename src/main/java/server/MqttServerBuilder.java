package server;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientBuilder;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import java.util.ArrayList;
import java.util.List;

import static com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode.SUCCESS;

/**
 * Sort of similar to a {@link io.grpc.ServerBuilder} but as agnostic as possible to any HTTP specific stuff, but adjusted to fit MQTT as transport instead.
 *
 * @author Simon Baier
 */
public class MqttServerBuilder {
    private final List<BindableService> services;
    private final List<ServerInterceptor> interceptors;
    private final MqttClientBuilder clientBuilder;
    private String brokerUrl;
    private int brokerPort;
    private String clientId;
    // Translates to MQTT keepalive
    private int keepAliveInterval;
    private byte[] password;
    private String username;

    public MqttServerBuilder() {
        services = new ArrayList<>();
        interceptors = new ArrayList<>();
        clientBuilder = MqttClient.builder();
    }

    public MqttServerBuilder brokerAddress(String brokerUrl) {
        this.brokerUrl = brokerUrl;
        return this;
    }

    public MqttServerBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public MqttServerBuilder addService(BindableService service) {
        services.add(service);
        return this;
    }

    public MqttServerBuilder addInterceptor(ServerInterceptor interceptor) {
        interceptors.add(interceptor);
        return this;
    }

    public MqttServerBuilder keepAliveInterval(int interval) {
        this.keepAliveInterval = interval;
        return this;
    }

    public MqttServerBuilder setPassword(byte[] password) {
        this.password = password;
        return this;
    }

    public MqttServerBuilder setUsername(String username) {
        this.username = username;
        return this;
    }

    public MqttServerBuilder setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort;
        return this;
    }

    public MqttServer build() {
        Mqtt5Client mqttClient = clientBuilder
                .identifier(clientId)
                .serverHost(brokerUrl)
                .serverPort(brokerPort)
                .automaticReconnectWithDefaultConfig()
                .useMqttVersion5()
                .build();


        final Mqtt5ConnectBuilder.Send<Mqtt5ConnAck> connectBuilder = mqttClient.toBlocking()
                .connectWith();
        if (username != null && password != null) {
            connectBuilder.simpleAuth()
                    .username(username)
                    .password(password)
                    .applySimpleAuth();
        }

        final Mqtt5ConnAck connack = connectBuilder
                .cleanStart(false)
                .keepAlive(keepAliveInterval)
                .send();

        if (!SUCCESS.equals(connack.getReasonCode())) {
            throw new IllegalStateException("Failed to connect to broker " + brokerUrl + ", " +
                    "reason code: " + connack.getReasonCode() + ", " +
                    "reason string: " + connack.getReasonString());
        }

        return new MqttServer(mqttClient, services, interceptors);
    }
}
