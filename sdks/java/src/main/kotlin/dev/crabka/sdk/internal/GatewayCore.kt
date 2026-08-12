package dev.crabka.sdk.internal

import com.connectrpc.Headers
import com.connectrpc.ResponseMessage
import com.google.protobuf.ByteString
import crabka.gateway.v1.GatewayClientInterface
import crabka.gateway.v1.GatewayOuterClass
import dev.crabka.sdk.Header
import dev.crabka.sdk.Record

public class GatewayCore private constructor(
    private val generatedGatewayClient: GatewayClientInterface?,
) {
    public fun toSendRequest(record: Record): GatewayOuterClass.SendRequest =
        GatewayOuterClass.SendRequest.newBuilder()
            .addRecords(record.toGatewayRecord())
            .setAcks(GatewayOuterClass.Acks.ACKS_ALL)
            .build()

    public fun toSubscribeFrame(
        topics: List<String>,
        group: String,
        filterExpression: String,
    ): GatewayOuterClass.SubscribeFrame {
        val start = GatewayOuterClass.SubscribeStart.newBuilder()
            .setGroupId(group)
            .addAllTopics(topics)
            .setAutoCommit(true)

        if (filterExpression.isNotBlank()) {
            start.setFilter(filterExpression)
        }

        return GatewayOuterClass.SubscribeFrame.newBuilder()
            .setStart(start)
            .build()
    }

    public suspend fun sendWithGeneratedClient(
        request: GatewayOuterClass.SendRequest,
        headers: Headers = emptyMap(),
    ): ResponseMessage<GatewayOuterClass.SendResponse> {
        val client = generatedGatewayClient
            ?: error("connect-kotlin GatewayClientInterface is not configured")
        return client.send(request, headers)
    }

    public fun hasGeneratedGatewayClient(): Boolean = generatedGatewayClient != null

    private fun Record.toGatewayRecord(): GatewayOuterClass.Record {
        val builder = GatewayOuterClass.Record.newBuilder()
            .setTopic(topic())
            .setRaw(ByteString.copyFrom(value()))
        headers().map { header -> header.toGatewayHeader() }.forEach(builder::addHeaders)
        return builder.build()
    }

    private fun Header.toGatewayHeader(): GatewayOuterClass.Header {
        val builder = GatewayOuterClass.Header.newBuilder().setKey(name())
        value()?.let { builder.setValue(ByteString.copyFrom(it)) }
        return builder.build()
    }

    public companion object {
        @JvmStatic
        public fun withoutGeneratedClient(): GatewayCore = GatewayCore(null)

        @JvmStatic
        public fun withGeneratedClient(generatedGatewayClient: GatewayClientInterface): GatewayCore =
            GatewayCore(generatedGatewayClient)
    }
}
