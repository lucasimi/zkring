package io.github.lucasimi.zkring.rpc;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final Vertx vertx = Vertx.vertx();

    private final WebClient client = WebClient.create(vertx);

    public <S, T> Optional<T> rpc(String path, S request, Serializer<S> serializer, Deserializer<T> deserializer) {
        byte[] reqSer = serializer.serialize(request);
        CompletableFuture<T> futureRes = new CompletableFuture<>(); 
        client
            .postAbs(path)
            .sendBuffer(
                Buffer.buffer(reqSer), 
                ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<Buffer> response = ar.result();
                        byte[] resSer = response.body().getBytes();
                        T res = deserializer.deserialize(resSer);
                        futureRes.complete(res);
                    } else {
                        Throwable cause = ar.cause();
                        LOGGER.error("Found error:", cause);
                    }
            });
        try {
            return Optional.ofNullable(futureRes.get());
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Found error:", e);
            return Optional.empty();
        }
    }
    
}
