package io.github.lucasimi.zkring.rpc;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import io.github.lucasimi.zkring.Node;
import io.github.lucasimi.zkring.discovery.Discovery;
import io.github.lucasimi.zkring.ring.Ring;

public class ZMQRPC implements RPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZMQRPC.class);

    private Discovery ringDiscovery;

    private ZMQ.Context context;

    public ZMQRPC() {
        context = ZMQ.context(1);
    }

    @Override
    public <S, T> Optional<T> get(String ringId, S request, SerDes<S, T> serdes) {
        Optional<Ring> ring = ringDiscovery.getRing(ringId);
        Optional<Node> node = ring
            .map(r -> r.get(request));
        if (node.isEmpty()) {
            return Optional.empty();
        } else {
            return sendRequest(node.get(), ringId, request, serdes);
        }
    }

    private <S, T> Optional<T> sendRequest(Node node, Req<S> request, SerDes<S, T> serdes) {
        ZMQ.Socket socket = context.socket(SocketType.REQ);
        try {
            String address = "tcp://" + node.address() + ":" + node.port();
            socket.connect(address);
            byte[] serializedReq = serdes.serialize(request);
            socket.send(serializedReq, 0);
            byte[] responseBytes = socket.recv(0);
            if (responseBytes != null) {
                T response = serdes.deserialize(responseBytes);
                return Optional.of(response);
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        } finally {
            socket.close();
        }
    }

    private void runServer() {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(SocketType.REP);
        socket.bind("tcp://*:5555");
        LOGGER.info("Server is listening on port 5555...");
        while (!Thread.currentThread().isInterrupted()) {
            String request = socket.recvStr(0);
            LOGGER.trace("Received request: {}", request);
            String reply = "World";
            LOGGER.trace("Sending reply: {}", reply);
            socket.send(reply.getBytes(), 0);
        }
        socket.close();
        context.term();
    }
    
}
