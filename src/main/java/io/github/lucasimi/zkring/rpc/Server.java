package io.github.lucasimi.zkring.rpc;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final Vertx vertx = Vertx.vertx();

    private final Router router = Router.router(vertx);

    public <S, T> void register(String path, Deserializer<S> deserializer, Function<S, T> function, Serializer<T> serializer) {
        router.post(path).handler(handler(deserializer, function, serializer));
    }

    public <S, T> void register(String path, Service<S, T> service) {
        router.post(path).handler(handler(service, service, service));
    }

    public void start(int port) {
        vertx
            .createHttpServer()
            .requestHandler(router)
            .listen(port, http -> {
                if (http.succeeded()) {
                    LOGGER.info("HTTP server started on port {}", port);
                } else {
                    LOGGER.error("Failed to start HTTP server on port {}", port);
                }
            });
    }

    private <S, T> Handler<RoutingContext> handler(Deserializer<S> deserializer, Function<S, T> function, Serializer<T> serializer) {
        return ctx -> {
            byte[] reqSer = ctx.body().buffer().getBytes();
            S req = deserializer.deserialize(reqSer);
            T res = function.apply(req);
            byte[] resSer = serializer.serialize(res);
            ctx.response().end(Buffer.buffer(resSer));
        };
    }

}
