package io.github.lucasimi.zkring;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.lucasimi.zkring.discovery.ZkRing;

public class ZkRingTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkRingTest.class);

    private static final boolean zkEmbedded = true;

    static ZooKeeperServer zkServer;

    static ServerCnxnFactory factory;

    @BeforeAll
    public static void startEmbeddedZK() throws Exception {
        if (zkEmbedded) {
            File logFile = Paths.get("./log").toFile();
            File tmpFile = Paths.get("./tmp").toFile();
            delete(logFile);
            delete(tmpFile);
            zkServer = new ZooKeeperServer(tmpFile, logFile, 1000);
            factory = new NIOServerCnxnFactory();
            factory.configure(new InetSocketAddress(2181), 100);
            factory.startup(zkServer);
        }
    }

    @AfterAll
    public static void stopEmbeddedZK() {
        if (zkEmbedded) {
            factory.shutdown();
            zkServer.shutdown();
            File logFile = Paths.get("./log").toFile();
            File tmpFile = Paths.get("./tmp").toFile();
            delete(logFile);
            delete(tmpFile);
        }
    }

    private static boolean delete(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                delete(file);
            }
        }
        return directory.delete();
    }

    @Test
    public void testZK() throws InterruptedException {
        String ringId1 = "ring1";
        String ringId2 = "ring2";
        String connectString = "localhost:2181";
        int sessionTimeout = 10_000;

        Node id1 = new Node(UUID.randomUUID(), "node1", 1);
        ZkRing zkDisc1 = ZkRing.newBuilder()
                .withConnectString(connectString)
                .withSessionTimeout(sessionTimeout)
                .withIdentity(id1)
                .withPartitions(10)
                .build();
        zkDisc1.subscribe(ringId1);
        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .until(() -> zkDisc1.size(ringId1) == 1);

        LOGGER.info("SUB 1");
        Thread.sleep(2_000);

        Node id2 = new Node(UUID.randomUUID(), "node2", 2);
        ZkRing zkDisc2 = ZkRing.newBuilder()
                .withConnectString(connectString)
                .withSessionTimeout(sessionTimeout)
                .withIdentity(id2)
                .withPartitions(10)
                .build();
        zkDisc2.subscribe(ringId1);

        LOGGER.info("SUB 2");
        Thread.sleep(2_000);

        Node id3 = new Node(UUID.randomUUID(), "node3", 3);
        ZkRing zkDisc3 = ZkRing.newBuilder()
                .withConnectString(connectString)
                .withSessionTimeout(sessionTimeout)
                .withIdentity(id3)
                .withPartitions(10)
                .build();
        zkDisc3.subscribe(ringId2);

        LOGGER.info("SUB 3");
        Thread.sleep(2_000);


        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .until(() -> zkDisc1.size(ringId1) == 2);
        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .until(() -> zkDisc2.size(ringId1) == 2);
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> zkDisc3.size(ringId2) == 1);

        //List<Node> peers1 = testSharding1.peers;
        //List<Node> peers2 = testSharding2.peers;
        //assertEquals(new HashSet<>(peers1), new HashSet<>(peers2));

        zkDisc2.close();
        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .until(() -> zkDisc1.size(ringId1) == 1);

        zkDisc1.close();
        zkDisc3.close();
    }

}
 