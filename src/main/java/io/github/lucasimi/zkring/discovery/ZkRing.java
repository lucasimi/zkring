package io.github.lucasimi.zkring.discovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.zookeeper.AsyncCallback;
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
import io.github.lucasimi.zkring.consistency.ConsistentCollection;
import io.github.lucasimi.zkring.consistency.Hash;
import io.github.lucasimi.zkring.consistency.HashRing;

public class ZkRing implements RingDiscovery, AutoCloseable {
 
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkRing.class);

    private final String connectString;

    private final int sessionTimeout;

    private final Node identity;

    private final int partitions;

    private final int replication;

    private final Hash hash;

    private final Map<String, ConsistentCollection> rings = new HashMap<>();

    private ZooKeeper zk;

    private AsyncCallback.ChildrenCallback childrenCallback(String ringId) {
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
                HashRing hashRing = new HashRing(partitions, replication, hash);
                hashRing.addAll(peers);
                rings.put(ringId, hashRing);
            }
            
        };
    }

    private Watcher childrenWatcher(String ringId) {
        return new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    String path = watchedEvent.getPath();
                    LOGGER.info("Peers changed for service {}", path.substring(1));
                    zk.getChildren(path, this, childrenCallback(ringId), null);
                }
            }

        };
    }

    public static class Builder {

        private String connectString = "localhost:2181";

        private int sessionTimeout = 10_000;

        private Node identity;

        private Hash hash = new Hash.Default();

        private int partitions = 1;

        private int replication = 1;

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

        public Builder withHash(Hash hash) {
            this.hash = hash;
            return this;
        }

        public Builder withPartitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder withReplication(int replication) {
            this.replication = replication;
            return this;
        }

        public ZkRing build() {
            return new ZkRing(this);
        }

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private ZkRing(Builder builder) {
        this.connectString = builder.connectString;
        this.sessionTimeout = builder.sessionTimeout;
        this.identity = builder.identity;
        this.partitions = builder.partitions;
        this.replication = builder.replication;
        this.hash = builder.hash;
    }

    @Override
    public void subscribe(String ringId) {
        try {
            this.zk = new ZooKeeper(connectString, sessionTimeout, null);
            String servicePath = getPath(ringId);
            if (zk.exists(servicePath, null) == null) {
                zk.create(servicePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Registered new ring: {}", ringId);
            }
            zk.getChildren(servicePath, childrenWatcher(ringId), childrenCallback(ringId), null);
            String peerPath = getPath(ringId, identity);
            byte[] serialized = Utils.serialize(identity);
            zk.create(peerPath, serialized, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Subscribed to {}", ringId);
        } catch (IOException | InterruptedException | KeeperException e) {
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
    public Optional<ConsistentCollection> getRing(String ringId) {
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
            .map(ConsistentCollection::size)
            .orElse(0);
    }

    private static String getPath(String ringId) {
        return "/" + ringId;
    }

    private static String getPath(String ringId, Node identity) {
        return getPath(ringId) + "/" + identity.uuid();
    }

}