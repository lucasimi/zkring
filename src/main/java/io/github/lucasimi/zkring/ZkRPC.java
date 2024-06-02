package io.github.lucasimi.zkring;

import java.util.Optional;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class ZkRPC {

    private RingDiscovery ringDiscovery;

    private ZMQ.Context context;

    public ZkRPC() {
        context = ZMQ.context(1);
    }

    public <S, T> Optional<T> get(String ringId, S request, SerDes<S, T> serdes) {
        Optional<ConsistentCollection> ring = ringDiscovery.getRing(ringId);
        Optional<Node> node = ring
            .map(r -> r.get(request));
        if (node.isEmpty()) {
            return Optional.empty();
        } else {
            return sendRequest(node.get(), ringId, request, serdes);
        }
    }

    private <S, T> Optional<T> sendRequest(Node node, String ringId, S request, SerDes<S, T> serdes) {
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
    
}
