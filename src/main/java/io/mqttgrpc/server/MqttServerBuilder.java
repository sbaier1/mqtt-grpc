package io.mqttgrpc.server;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientBuilder;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode.SUCCESS;

/**
 * Sort of similar to a {@link io.grpc.ServerBuilder} but as agnostic as possible to any HTTP specific stuff, but adjusted to fit MQTT as transport instead.
 *
 * @author Simon Baier
 */
public class MqttServerBuilder {
    private static final Logger log = LoggerFactory.getLogger(MqttServerBuilder.class);
    private final List<BindableService> services;
    private final List<ServerInterceptor> interceptors;
    private final MqttClientBuilder clientBuilder;
    private Mqtt5Client client;
    private boolean useExistingClient;
    private String brokerUrl;
    private int brokerPort;
    private String clientId;
    // Translates to MQTT keepalive
    private int keepAliveInterval;

    private String topicPrefix;
    private byte[] password;
    private String username;

    private boolean respondNotImplemented;
    private MqttQos qos;

    public MqttServerBuilder() {
        services = new ArrayList<>();
        interceptors = new ArrayList<>();
        clientBuilder = MqttClient.builder();
        useExistingClient = false;
        respondNotImplemented = true;
        qos = MqttQos.AT_LEAST_ONCE;
    }

    public MqttServerBuilder brokerAddress(String brokerUrl) {
        assertNoExistingClient();
        this.brokerUrl = brokerUrl;
        return this;
    }

    public MqttServerBuilder clientId(String clientId) {
        assertNoExistingClient();
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
        assertNoExistingClient();
        this.keepAliveInterval = interval;
        return this;
    }

    public MqttServerBuilder setPassword(byte[] password) {
        assertNoExistingClient();
        this.password = password;
        return this;
    }

    public MqttServerBuilder setUsername(String username) {
        assertNoExistingClient();
        this.username = username;
        return this;
    }

    public MqttServerBuilder port(int brokerPort) {
        assertNoExistingClient();
        this.brokerPort = brokerPort;
        return this;
    }

    public MqttServerBuilder topicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
        return this;
    }

    public MqttServerBuilder qos(MqttQos qos) {
        this.qos = qos;
        return this;
    }

    private void assertNoExistingClient() {
        if (useExistingClient) {
            throw new IllegalStateException("Must not use setters when providing an existing client");
        }
    }

    /**
     * Get the client builder for adjusting some parameters of the client as needed
     */
    public MqttClientBuilder getClientBuilder() {
        return clientBuilder;
    }

    /**
     * Use an existing, fully configured (and assumed in connected state) client instance.
     * It should also use automatic reconnect + cleanStart=false
     */
    public MqttServerBuilder setClient(Mqtt5Client client) {
        this.useExistingClient = true;
        this.client = client;
        return this;
    }

    /**
     * Whether the MqttServer should respond to unknown method calls with a NOT IMPLEMENTED string payload to indicate to the client that a method is missing from the server
     */
    public MqttServerBuilder respondNotImplemented(boolean respondNotImplemented) {
        this.respondNotImplemented = respondNotImplemented;
        return this;
    }

    public MqttServer build() {
        assertNotEmptyOrNull(topicPrefix, "topicPrefix");
        if (topicPrefix.endsWith("/")) {
            throw new IllegalStateException("Topic prefix must not end with a topic separator");
        }

        if (!useExistingClient) {
            client = clientBuilder
                    .identifier(clientId)
                    .serverHost(brokerUrl)
                    .serverPort(brokerPort)
                    .automaticReconnectWithDefaultConfig()
                    .useMqttVersion5()
                    .build();


            final Mqtt5ConnectBuilder.Send<Mqtt5ConnAck> connectBuilder = client.toBlocking()
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
        } else {
            if (!client.getState().isConnected()) {
                throw new IllegalStateException("Existing client must be connected prior to building the MqttServer");
            }
        }

        return new MqttServer(client, services, interceptors, topicPrefix, qos, respondNotImplemented);
    }


    private void assertNotEmptyOrNull(final String field, final String name) {
        if (field == null || field.isBlank()) {
            throw new IllegalStateException("Field " + name + " must not be empty or null");
        }
    }
}
