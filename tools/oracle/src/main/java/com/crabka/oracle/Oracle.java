package com.crabka.oracle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * Line-oriented JSON-RPC oracle.
 *
 * Request:  {"op":"encode","apiKey":18,"version":3,"isRequest":true,"value":{...}}
 *           {"op":"decode","apiKey":18,"version":3,"isRequest":true,"hex":"..."}
 * Response: {"ok":true,"hex":"..."}   or   {"ok":true,"value":{...}}
 *           {"ok":false,"error":"..."}
 *
 * Uses Kafka's generated *JsonConverter classes for JSON<->message conversion,
 * discovered by reflection from the concrete message class name.
 */
public final class Oracle {
    private static final ObjectMapper M = new ObjectMapper();
    // Package where Kafka generates its *Data and *DataJsonConverter classes.
    private static final String MSG_PKG = "org.apache.kafka.common.message.";

    public static void main(String[] args) throws Exception {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(System.out, true, StandardCharsets.UTF_8)) {
            String line;
            while ((line = in.readLine()) != null) {
                try {
                    out.println(M.writeValueAsString(handle(M.readTree(line))));
                } catch (Throwable t) {
                    ObjectNode err = M.createObjectNode();
                    err.put("ok", false);
                    err.put("error", t.getClass().getSimpleName() + ": " + t.getMessage());
                    out.println(M.writeValueAsString(err));
                }
            }
        }
    }

    private static ObjectNode handle(JsonNode req) throws Exception {
        String op = req.get("op").asText();
        int apiKey = req.get("apiKey").asInt();
        short version = (short) req.get("version").asInt();
        boolean isRequest = req.get("isRequest").asBoolean();

        ApiMessage msg = isRequest
                ? ApiKeys.forId(apiKey).messageType.newRequest()
                : ApiKeys.forId(apiKey).messageType.newResponse();

        // Resolve the generated JsonConverter class, e.g.
        // org.apache.kafka.common.message.ApiVersionsRequestDataJsonConverter
        String msgClassName = msg.getClass().getSimpleName();         // e.g. ApiVersionsRequestData
        String converterName = MSG_PKG + msgClassName + "JsonConverter";
        Class<?> converterClass = Class.forName(converterName);

        ObjectNode resp = M.createObjectNode();

        if (op.equals("encode")) {
            // Use the generated converter to populate the message from JSON
            Method readMethod = converterClass.getMethod("read", JsonNode.class, short.class);
            ApiMessage populated = (ApiMessage) readMethod.invoke(null, req.get("value"), version);

            ObjectSerializationCache cache = new ObjectSerializationCache();
            int size = populated.size(cache, version);
            ByteBuffer bb = ByteBuffer.allocate(size);
            populated.write(new ByteBufferAccessor(bb), cache, version);
            bb.flip();
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            resp.put("ok", true);
            resp.put("hex", HexFormat.of().formatHex(bytes));

        } else if (op.equals("decode")) {
            byte[] bytes = HexFormat.of().parseHex(req.get("hex").asText());
            msg.read(new ByteBufferAccessor(ByteBuffer.wrap(bytes)), version);

            // Use the generated converter to turn the message back into a JsonNode
            // Signature: write(MessageData, short) or write(MessageData, short, boolean)
            // Prefer the 2-arg form; fall back to 3-arg if needed.
            JsonNode value;
            try {
                Method writeMethod = converterClass.getMethod("write", msg.getClass(), short.class);
                value = (JsonNode) writeMethod.invoke(null, msg, version);
            } catch (NoSuchMethodException e) {
                // Some converters have write(Data, short, boolean) — pass true for include defaults
                Method writeMethod = converterClass.getMethod("write", msg.getClass(), short.class, boolean.class);
                value = (JsonNode) writeMethod.invoke(null, msg, version, true);
            }
            resp.put("ok", true);
            resp.set("value", value);

        } else {
            throw new IllegalArgumentException("unknown op: " + op);
        }
        return resp;
    }
}
