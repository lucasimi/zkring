package io.github.lucasimi.zkring.rpc;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.lucasimi.zkring.Node;
import io.github.lucasimi.zkring.discovery.Discovery;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final Vertx vertx = Vertx.vertx();

    private final WebClient client = WebClient.create(vertx);

    private Discovery discovery;

    public <S, T> Optional<T> rpc(String path, S request, Serializer<S> serializer, Deserializer<T> deserializer) {
        byte[] reqSer = serializer.serialize(request);
        return discovery.getRing(path)
            .map(ring -> ring.get(request))
            .flatMap(node -> sendBlocking(node, path, reqSer))
            .map(deserializer::deserialize);
    }

    private Optional<byte[]> sendBlocking(Node node, String path, byte[] request) {
        CompletableFuture<byte[]> futureRes = new CompletableFuture<>(); 
        client
            .post(node.port(), node.address(), path)
            .sendBuffer(
                Buffer.buffer(request), 
                asyncResult -> {
                    if (asyncResult.succeeded()) {
                        HttpResponse<Buffer> httpResponse = asyncResult.result();
                        byte[] response = httpResponse.body().getBytes();
                        futureRes.complete(response);
                    } else {
                        Throwable cause = asyncResult.cause();
                        LOGGER.error("Found error while sending request", cause);
                        futureRes.complete(null);
                    }
            });
        try {
            byte[] response = futureRes.get();
            return Optional.ofNullable(response);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Found error while waiting for response", e);
            return Optional.empty();
        }
    }
    
}
