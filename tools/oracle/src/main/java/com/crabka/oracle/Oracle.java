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
 *           {"op":"compress","codec":"gzip","data":"<hex>"}
 *           {"op":"decompress","codec":"gzip","data":"<hex>"}
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

        ObjectNode resp = M.createObjectNode();

        if (op.equals("record_batch_encode")) {
            return encodeRecordBatch(req.get("value"));
        } else if (op.equals("record_batch_decode")) {
            byte[] bytes = HexFormat.of().parseHex(req.get("hex").asText());
            return decodeRecordBatch(bytes);
        } else if (op.equals("compress") || op.equals("decompress")) {
            String codec = req.get("codec").asText();
            byte[] input = HexFormat.of().parseHex(req.get("data").asText());
            byte[] result;
            if (op.equals("compress")) {
                result = compressBytes(codec, input);
            } else {
                result = decompressBytes(codec, input);
            }
            resp.put("ok", true);
            resp.put("hex", HexFormat.of().formatHex(result));

        } else {
            // encode / decode ops need apiKey, version, isRequest
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
        }
        return resp;
    }

    private static ObjectNode encodeRecordBatch(JsonNode value) throws Exception {
        long baseOffset = value.get("base_offset").asLong();
        short producerEpoch = (short) value.get("producer_epoch").asInt();
        int baseSequence = value.get("base_sequence").asInt();
        long producerId = value.get("producer_id").asLong();
        int partitionLeaderEpoch = value.get("partition_leader_epoch").asInt();
        long baseTimestamp = value.get("base_timestamp").asLong();
        boolean isTransactional = value.get("is_transactional").asBoolean();
        boolean isControl = value.get("is_control_batch").asBoolean();
        String tsType = value.get("timestamp_type").asText(); // "CreateTime" or "LogAppendTime"
        String codecName = value.get("compression").asText(); // "NONE" / "GZIP" / "SNAPPY" / "LZ4" / "ZSTD"

        org.apache.kafka.common.record.CompressionType compressionType =
            org.apache.kafka.common.record.CompressionType.valueOf(codecName);
        org.apache.kafka.common.compress.Compression compression =
            org.apache.kafka.common.compress.Compression.of(compressionType).build();

        // Map "CreateTime" -> CREATE_TIME, "LogAppendTime" -> LOG_APPEND_TIME
        String tsName = tsType.equals("LogAppendTime") ? "LOG_APPEND_TIME" : "CREATE_TIME";
        org.apache.kafka.common.record.TimestampType timestampType =
            org.apache.kafka.common.record.TimestampType.valueOf(tsName);

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(1024 * 1024);
        org.apache.kafka.common.record.MemoryRecordsBuilder mrb =
            org.apache.kafka.common.record.MemoryRecords.builder(
                buffer,
                org.apache.kafka.common.record.RecordBatch.CURRENT_MAGIC_VALUE,
                compression,
                timestampType,
                baseOffset,
                baseTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                isControl,
                partitionLeaderEpoch);

        com.fasterxml.jackson.databind.node.ArrayNode records =
            (com.fasterxml.jackson.databind.node.ArrayNode) value.get("records");
        for (JsonNode r : records) {
            long ts = baseTimestamp + r.get("timestamp_delta").asLong();
            long offset = baseOffset + r.get("offset_delta").asLong();
            byte[] key = r.has("key") && !r.get("key").isNull()
                ? HexFormat.of().parseHex(r.get("key").asText()) : null;
            byte[] val = r.has("value") && !r.get("value").isNull()
                ? HexFormat.of().parseHex(r.get("value").asText()) : null;

            java.util.List<org.apache.kafka.common.header.Header> headers = new java.util.ArrayList<>();
            if (r.has("headers")) {
                for (JsonNode h : r.get("headers")) {
                    String hk = h.get("key").asText();
                    byte[] hv = h.has("value") && !h.get("value").isNull()
                        ? HexFormat.of().parseHex(h.get("value").asText()) : null;
                    headers.add(new org.apache.kafka.common.header.internals.RecordHeader(hk, hv));
                }
            }
            mrb.appendWithOffset(offset, ts, key, val,
                headers.toArray(new org.apache.kafka.common.header.Header[0]));
        }

        org.apache.kafka.common.record.MemoryRecords mr = mrb.build();
        java.nio.ByteBuffer out = mr.buffer();
        byte[] bytes = new byte[out.remaining()];
        out.duplicate().get(bytes);

        ObjectNode resp = M.createObjectNode();
        resp.put("ok", true);
        resp.put("hex", HexFormat.of().formatHex(bytes));
        return resp;
    }

    private static ObjectNode decodeRecordBatch(byte[] bytes) throws Exception {
        org.apache.kafka.common.record.MemoryRecords mr =
            org.apache.kafka.common.record.MemoryRecords.readableRecords(
                java.nio.ByteBuffer.wrap(bytes));
        java.util.Iterator<org.apache.kafka.common.record.MutableRecordBatch> it =
            mr.batches().iterator();
        if (!it.hasNext()) {
            ObjectNode err = M.createObjectNode();
            err.put("ok", false);
            err.put("error", "no batch in input");
            return err;
        }
        org.apache.kafka.common.record.MutableRecordBatch b = it.next();
        // DefaultRecordBatch exposes baseTimestamp(); cast to access it.
        org.apache.kafka.common.record.DefaultRecordBatch db =
            (org.apache.kafka.common.record.DefaultRecordBatch) b;

        ObjectNode value = M.createObjectNode();
        value.put("base_offset", b.baseOffset());
        value.put("partition_leader_epoch", b.partitionLeaderEpoch());
        value.put("compression", b.compressionType().name());
        value.put("timestamp_type",
            b.timestampType() == org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME
                ? "LogAppendTime" : "CreateTime");
        value.put("is_transactional", b.isTransactional());
        value.put("is_control_batch", b.isControlBatch());
        value.put("base_timestamp", db.baseTimestamp());
        value.put("max_timestamp", b.maxTimestamp());
        value.put("producer_id", b.producerId());
        value.put("producer_epoch", (int) b.producerEpoch());
        value.put("base_sequence", b.baseSequence());

        com.fasterxml.jackson.databind.node.ArrayNode recordsArr = value.putArray("records");
        for (org.apache.kafka.common.record.Record r : b) {
            ObjectNode rj = recordsArr.addObject();
            rj.put("offset_delta", (int)(r.offset() - b.baseOffset()));
            rj.put("timestamp_delta", r.timestamp() - db.baseTimestamp());
            if (r.hasKey()) {
                byte[] k = new byte[r.keySize()];
                r.key().duplicate().get(k);
                rj.put("key", HexFormat.of().formatHex(k));
            } else {
                rj.putNull("key");
            }
            if (r.hasValue()) {
                byte[] v = new byte[r.valueSize()];
                r.value().duplicate().get(v);
                rj.put("value", HexFormat.of().formatHex(v));
            } else {
                rj.putNull("value");
            }
            com.fasterxml.jackson.databind.node.ArrayNode hs = rj.putArray("headers");
            for (org.apache.kafka.common.header.Header h : r.headers()) {
                ObjectNode hj = hs.addObject();
                hj.put("key", h.key());
                if (h.value() != null) {
                    hj.put("value", HexFormat.of().formatHex(h.value()));
                } else {
                    hj.putNull("value");
                }
            }
        }

        ObjectNode resp = M.createObjectNode();
        resp.put("ok", true);
        resp.set("value", value);
        return resp;
    }

    private static byte[] compressBytes(String codec, byte[] input) throws Exception {
        switch (codec) {
            case "gzip": {
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                try (java.io.OutputStream s = new java.util.zip.GZIPOutputStream(out)) {
                    s.write(input);
                }
                return out.toByteArray();
            }
            case "snappy": {
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                try (java.io.OutputStream s = new org.xerial.snappy.SnappyOutputStream(out)) {
                    s.write(input);
                }
                return out.toByteArray();
            }
            case "lz4": {
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                // BLOCKSIZE_64KB = 4; useBrokenFlagDescriptorChecksum = false
                try (java.io.OutputStream s =
                        new org.apache.kafka.common.compress.Lz4BlockOutputStream(out,
                            org.apache.kafka.common.compress.Lz4BlockOutputStream.BLOCKSIZE_64KB,
                            false)) {
                    s.write(input);
                }
                return out.toByteArray();
            }
            case "zstd": {
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                try (java.io.OutputStream s = new com.github.luben.zstd.ZstdOutputStream(out, 3)) {
                    s.write(input);
                }
                return out.toByteArray();
            }
            default:
                throw new IllegalArgumentException("unknown codec: " + codec);
        }
    }

    private static byte[] decompressBytes(String codec, byte[] input) throws Exception {
        switch (codec) {
            case "gzip": {
                java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(input);
                try (java.io.InputStream s = new java.util.zip.GZIPInputStream(in)) {
                    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = s.read(buf)) >= 0) out.write(buf, 0, n);
                    return out.toByteArray();
                }
            }
            case "snappy": {
                java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(input);
                try (java.io.InputStream s = new org.xerial.snappy.SnappyInputStream(in)) {
                    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = s.read(buf)) >= 0) out.write(buf, 0, n);
                    return out.toByteArray();
                }
            }
            case "lz4": {
                java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(input);
                try (java.io.InputStream s = new org.apache.kafka.common.compress.Lz4BlockInputStream(
                        bb, org.apache.kafka.common.utils.BufferSupplier.NO_CACHING, false)) {
                    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = s.read(buf)) >= 0) out.write(buf, 0, n);
                    return out.toByteArray();
                }
            }
            case "zstd": {
                java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(input);
                try (java.io.InputStream s = new com.github.luben.zstd.ZstdInputStream(in)) {
                    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = s.read(buf)) >= 0) out.write(buf, 0, n);
                    return out.toByteArray();
                }
            }
            default:
                throw new IllegalArgumentException("unknown codec: " + codec);
        }
    }
}
