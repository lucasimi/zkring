package io.github.lucasimi.zkring.rpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ReqSerializer<S> implements Serializer<Req<S>> {

    private final Serializer<S> serializer;

    private ReqSerializer(Serializer<S> serializer) {
        this.serializer = serializer;
    }

    public static <S> ReqSerializer<S> from(Serializer<S> serializer) {
        return new ReqSerializer<>(serializer);
    }

    @Override
    public byte[] serialize(Req<S> req) {
        byte[] serializedService = req.service().getBytes(StandardCharsets.UTF_8);
        byte[] serializedData = serializer.serialize(req.data());
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + serializedService.length + serializedData.length);
        buffer.putInt(serializedService.length);
        buffer.put(serializedService);
        buffer.put(serializedData);
        return buffer.array();
    }
    
}
