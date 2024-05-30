package io.github.lucasimi.zkring;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class WebService {
   
    private static final Vertx vertx = Vertx.vertx();

    private static final HttpClient client = vertx.createHttpClient();
    
    private static final Router router = Router.router(vertx);

    public Handler<RoutingContext> redirection() {
        return new Handler<RoutingContext>() {

            @Override
            public void handle(RoutingContext ctx) {
                HttpServerRequest req = ctx.request();
                String targetUri = "http://targetserver.com" + req.uri();
                client.request(req.method(), targetUri, ar -> {
                    if (ar.succeeded()) {
                        ar.result().send(req).onComplete(clientResponse -> {
                            if (clientResponse.succeeded()) {
                                HttpServerResponse response = ctx.response();
                                response.setChunked(true);
                                response.setStatusCode(clientResponse.result().statusCode());
                                response.headers().setAll(clientResponse.result().headers());
                                clientResponse.result().bodyHandler(buffer -> response.write(buffer));
                                clientResponse.result().endHandler(v -> response.end());
                            } else {
                                ctx.fail(clientResponse.cause());
                            }
                        });
                    } else {
                        ctx.fail(ar.cause());
                    }
                });
            }
            
        };
    }

    public void start() {
        router.route().handler(redirection());
        vertx.createHttpServer().requestHandler(router).listen(8888);
    }
    
}
