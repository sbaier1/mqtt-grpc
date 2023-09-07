package io.mqttgrpc.client;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientBuilder;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.Mqtt5ReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import io.grpc.ClientInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.mqttgrpc.Constants.PING_SUFFIX;
import static io.mqttgrpc.Constants.RESPONSES_INFIX;

/**
 * @author Simon Baier
 */

public class MqttChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(MqttChannelBuilder.class);

    private final MqttClientBuilder clientBuilder;
    private final List<ClientInterceptor> interceptors;
    private boolean pingService;
    private int pingDeadlineSeconds;
    private int timeout;
    private int port;
    private String brokerUrl;
    private String topicPrefix;
    private String clientId;
    private MqttQos qos;

    private Mqtt5Client client;

    private boolean useExistingClient;

    MqttChannelBuilder() {
        clientBuilder = MqttClient.builder();
        pingService = true;
        pingDeadlineSeconds = 30;
        timeout = 30;
        qos = MqttQos.AT_LEAST_ONCE;
        interceptors = new ArrayList<>();
        useExistingClient = false;
        client = null;
    }

    public MqttChannelBuilder brokerAddress(String brokerUrl) {
        this.brokerUrl = brokerUrl;
        return this;
    }

    public MqttChannelBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public MqttChannelBuilder topicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
        return this;
    }

    // Whether the topic prefix should be checked whether it has a MqttServer which will respond
    public MqttChannelBuilder pingService(boolean pingService) {
        this.pingService = pingService;
        return this;
    }

    public MqttChannelBuilder pingDeadlineSeconds(int pingDeadlineSeconds) {
        this.pingDeadlineSeconds = pingDeadlineSeconds;
        return this;
    }

    public MqttChannelBuilder port(int port) {
        this.port = port;
        return this;
    }

    public MqttChannelBuilder setClient(Mqtt5Client client) {
        this.client = client;
        this.useExistingClient = true;
        return this;
    }

    public MqttChannelBuilder intercept(ClientInterceptor... interceptor) {
        this.interceptors.addAll(List.of(interceptor));
        return this;
    }

    /**
     * How long to wait for calls to return
     */
    public MqttChannelBuilder timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public MqttChannelBuilder qos(MqttQos qos) {
        this.qos = qos;
        return this;
    }

    /**
     * Get the client builder for adjusting additional parameters of the client as necessary.
     * NOTE: some parameters, such as clientId and serverHost will be set by the builder.
     */
    public MqttClientBuilder getClientBuilder() {
        return clientBuilder;
    }

    public MqttChannel build() {

        if (!useExistingClient) {
            this.client = clientBuilder
                    .identifier(clientId)
                    .serverHost(brokerUrl)
                    .serverPort(port)
                    .useMqttVersion5()
                    .buildAsync();
            client.toAsync().connect().join();
        }

        // Optionally, verify there's _some_ backend service that will respond
        // (does not check any implemented methods, just an initial check to ensure we don't publish into nothingness)
        pingService(client.toAsync());

        if (clientId == null || clientId.isBlank()) {
            throw new IllegalStateException("client id must not be empty");
        }

        if (topicPrefix == null || topicPrefix.isBlank()) {
            throw new IllegalStateException("topic must not be empty");
        }
        return new MqttChannel(client.toAsync(), topicPrefix, clientId, interceptors, timeout, qos);
    }

    private void pingService(Mqtt5AsyncClient mqttClient) {
        if (pingService) {
            final String pingResponseTopic = topicPrefix + RESPONSES_INFIX + clientId + "/ping";
            final CountDownLatch latch = new CountDownLatch(1);
            final Mqtt5SubAck suback = mqttClient.subscribeWith()
                    .topicFilter(pingResponseTopic)
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .callback(mqtt5Publish -> {
                        latch.countDown();
                    })
                    .send().join();
            if (suback.getReasonCodes().stream().anyMatch(Mqtt5ReasonCode::isError)) {
                log.error("Failed to subscribe to ping response topic, suback: {}", suback);
                throw new IllegalStateException("Failed to subscribe to ping response topic");
            }
            final Mqtt5PublishResult publishResult = mqttClient.toBlocking()
                    .publishWith()
                    .topic(topicPrefix + PING_SUFFIX)
                    .payload("ping".getBytes())
                    .responseTopic(pingResponseTopic)
                    .send();
            if (publishResult.getError().isPresent()) {
                log.error("Failed to publish ping to backend service", publishResult.getError().get());
                throw new IllegalStateException("Could not ping backend service");
            }
            try {
                final boolean reached = latch.await(pingDeadlineSeconds, TimeUnit.SECONDS);
                if (!reached) {
                    throw new IllegalStateException("Failed to ping service in " + pingDeadlineSeconds + "s");
                }
                log.debug("Backend service is present, proceeding");
            } catch (InterruptedException e) {
                throw new IllegalStateException("Interrupted while pinging service");
            }
        }
    }
}
