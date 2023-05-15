package io.mqttgrpc;

import java.nio.charset.StandardCharsets;

/**
 * @author Simon Baier
 */
public class Constants {
    public static final String ENDPOINTS_INFIX = "/endpoints/";
    public static final String RESPONSES_INFIX = "/responses/";
    public static final String PING_SUFFIX = "/ping";

    // Payload sent by MqttServer when a message is not implemented and the server is configured to respond as such.
    public static final byte[] NOT_IMPLEMENTED_PAYLOAD = "NOT IMPLEMENTED".getBytes(StandardCharsets.UTF_8);
}
