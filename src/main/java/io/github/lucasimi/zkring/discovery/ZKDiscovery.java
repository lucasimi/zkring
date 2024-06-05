package io.github.lucasimi.zkring.discovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.lucasimi.zkring.Node;
import io.github.lucasimi.zkring.Utils;
import io.github.lucasimi.zkring.ring.Ring;

public class ZKDiscovery implements Discovery, AutoCloseable {
 
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKDiscovery.class);

    private final Node identity;

    private final Map<String, Ring> rings = new HashMap<>();

    private final ZooKeeper zk;

    private AsyncCallback.ChildrenCallback childrenCallback(String ringId, Supplier<Ring> ringSupplier) {
        return new AsyncCallback.ChildrenCallback() {
       
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                String ringPath = getPath(ringId);
                if ((children == null) || !ringPath.equals(path)) {
                    return;
                }
                List<Node> peers = new ArrayList<>(children.size());
                for (String child : children) {
                    String childPath = path + "/" + child;
                    try {
                        byte[] childData = zk.getData(childPath, false, null);
                        Node childIdentity = Utils.deserialize(childData, Node.class);
                        if (childIdentity != null) {
                            peers.add(childIdentity);
                        }
                    } catch (KeeperException | InterruptedException e) {
                        LOGGER.error("Unable to retrieve data for path {}: {}", childPath, e.getLocalizedMessage());
                        throw new RuntimeException(e);
                    }
                }
                LOGGER.info("Updating ring {} with {} peers", ringId, children.size());
                Ring ring = ringSupplier.get();
                ring.clear();
                ring.addAll(peers);
                rings.put(ringId, ring);
            }
            
        };
    }

    private Watcher childrenWatcher(String ringId, Supplier<Ring> ringSupplier) {
        return new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    String path = watchedEvent.getPath();
                    LOGGER.info("Peers changed for service {}", path.substring(1));
                    zk.getChildren(path, this, childrenCallback(ringId, ringSupplier), null);
                }
            }

        };
    }

    public static class Builder {

        private String connectString = "localhost:2181";

        private int sessionTimeout = 10_000;

        private Node identity;

        public Builder withConnectString(String connectString) {
            this.connectString = connectString;
            return this;
        }

        public Builder withSessionTimeout(int sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        public Builder withIdentity(Node identity) {
            this.identity = identity;
            return this;
        }

        public ZKDiscovery build() throws IOException {
            return new ZKDiscovery(this);
        }

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private ZKDiscovery(Builder builder) throws IOException {
        this.identity = builder.identity;
        this.zk = new ZooKeeper(builder.connectString, builder.sessionTimeout, null);
    }

    @Override
    public void subscribe(String ringId, Supplier<Ring> ringSupplier) {
        try {
            String servicePath = getPath(ringId);
            if (zk.exists(servicePath, null) == null) {
                zk.create(servicePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Registered new ring: {}", ringId);
            }
            Watcher watcher = childrenWatcher(ringId, ringSupplier);
            ChildrenCallback callback = childrenCallback(ringId, ringSupplier);
            zk.getChildren(servicePath, watcher, callback, null);
            String peerPath = getPath(ringId, identity);
            byte[] serialized = Utils.serialize(identity);
            zk.create(peerPath, serialized, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Subscribed to {}", ringId);
        } catch (InterruptedException | KeeperException e) {
            LOGGER.error("Unable to subscribe to {}", ringId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unsubscribe(String ringId) {
        String peerPath = getPath(ringId, identity);
        UUID uuid = identity.uuid();
        try {
            zk.delete(peerPath, -1);
            LOGGER.info("Unsubscribed peer {} from ring {}", uuid, ringId);
        } catch (InterruptedException | KeeperException e) {
            LOGGER.error("Unable to unsubscibe peer {} from {}", uuid, ringId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Ring> getRing(String ringId) {
        return Optional.ofNullable(this.rings.get(ringId));
    }

    @Override
    public void close() {
        try {
            this.rings.keySet().forEach(this::unsubscribe);
            this.rings.clear();
            this.zk.close();
            LOGGER.info("Closed connection to ZooKeeper");
        } catch (InterruptedException e) {
            LOGGER.error("Unable to close connection to ZooKeeper: {}", e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    public int size(String ringId) {
        return Optional.ofNullable(this.rings.get(ringId))
            .map(Ring::size)
            .orElse(0);
    }

    private static String getPath(String ringId) {
        return "/" + ringId;
    }

    private static String getPath(String ringId, Node identity) {
        return getPath(ringId) + "/" + identity.uuid();
    }

}