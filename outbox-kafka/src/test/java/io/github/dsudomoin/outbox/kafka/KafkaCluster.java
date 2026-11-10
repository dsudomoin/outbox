package io.github.dsudomoin.outbox.kafka;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

/**
 * Three KRaft nodes (broker and controller each) of apache/kafka:3.9.1 on one Docker network, for fault tests:
 * auto-create off, default replication factor 3, {@code min.insync.replicas=2}, unclean election off. Brokers talk over
 * a PLAINTEXT listener on the network; clients reach broker {@code n} through an in-JVM TCP proxy on a fixed
 * {@code localhost} port, which is also the advertised address, so clients survive a broker restart and the proxy can
 * cut or slow the client side of one broker without tc/netem. The client listener is SASL/PLAIN with
 * {@code StandardAuthorizer}: {@code admin} is a super user, {@code app} may do anything no ACL denies.
 */
final class KafkaCluster implements AutoCloseable {

    static final String APP = "app";
    private static final String ADMIN = "admin";
    private static final int CLIENT_PORT = 9092;
    private static final Duration UP = Duration.ofSeconds(120);

    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> brokers = new ArrayList<>();
    private final List<Proxy> proxies = new ArrayList<>();

    KafkaCluster() throws IOException {
        var voters = IntStream.rangeClosed(1, 3).mapToObj(n -> n + "@kafka-" + n + ":9093")
                .collect(Collectors.joining(","));
        for (int n = 1; n <= 3; n++) {
            int hostPort = freePort();
            var proxy = new Proxy(hostPort);
            proxies.add(proxy);
            brokers.add(new GenericContainer<>("apache/kafka:3.9.1")
                    .withNetwork(network)
                    .withNetworkAliases("kafka-" + n)
                    .withEnv("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw")
                    .withEnv("KAFKA_NODE_ID", String.valueOf(n))
                    .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                    .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", voters)
                    .withEnv("KAFKA_LISTENERS",
                            "INTERNAL://0.0.0.0:19092,EXTERNAL://0.0.0.0:" + CLIENT_PORT + ",CONTROLLER://0.0.0.0:9093")
                    .withEnv("KAFKA_ADVERTISED_LISTENERS",
                            "INTERNAL://kafka-" + n + ":19092,EXTERNAL://localhost:" + proxy.port())
                    .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                            "INTERNAL:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
                    .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
                    .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                    .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                    .withEnv("KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required"
                                    + " user_admin=\"admin-secret\" user_app=\"app-secret\";")
                    .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
                    .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS;User:admin") // ANONYMOUS: the PLAINTEXT peers
                    .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                    .withEnv("KAFKA_DEFAULT_REPLICATION_FACTOR", "3")
                    .withEnv("KAFKA_MIN_INSYNC_REPLICAS", "2")
                    .withEnv("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "false")
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2")
                    .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                    // a fixed host port: a broker started again after a kill keeps it, so the proxy keeps its target
                    .withCreateContainerCmdModifier(cmd -> {
                        cmd.withExposedPorts(ExposedPort.tcp(CLIENT_PORT));
                        cmd.getHostConfig().withPortBindings(
                                new PortBinding(Ports.Binding.bindPort(hostPort), ExposedPort.tcp(CLIENT_PORT)));
                    })
                    // the nodes wait for each other's quorum, so they start together
                    .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(UP)));
        }
        Startables.deepStart(brokers).join();
        awaitBrokers(Set.of(1, 2, 3), UP);
    }

    String bootstrapServers() {
        return proxies.stream().map(p -> "localhost:" + p.port()).collect(Collectors.joining(","));
    }

    /** Client properties of {@link #APP} (a producer's, the outbox's identity) or of the super user. */
    Map<String, Object> clientProperties(boolean app) {
        var user = app ? APP : ADMIN;
        return Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\"" + user + "\" password=\"" + user + "-secret\";");
    }

    private Admin admin() {
        var properties = new HashMap<>(clientProperties(false));
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        return Admin.create(properties);
    }

    /** Creates {@code topic} with replication factor 3 and waits until every partition has a leader and 3 in sync. */
    void createTopic(String topic, int partitions, Map<String, String> configs) {
        try (var admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 3).configs(configs)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("cannot create topic " + topic, e);
        }
        await(Duration.ofSeconds(60), () -> {
            try {
                return describe(topic).partitions().stream()
                        .allMatch(p -> p.leader() != null && p.isr().size() == 3);
            } catch (IllegalStateException e) {
                return false; // the broker asked has not learnt of the topic yet
            }
        });
    }

    /** Deletes {@code topic} and waits until no broker lists it any more. */
    void deleteTopic(String topic) {
        try (var admin = admin()) {
            admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            await(Duration.ofSeconds(60), () -> {
                try {
                    return !admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic);
                } catch (Exception e) {
                    return false;
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException("cannot delete topic " + topic, e);
        }
    }

    /** Sets one config of {@code topic}. */
    void setTopicConfig(String topic, String name, String value) {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (var admin = admin()) {
            admin.incrementalAlterConfigs(Map.of(resource,
                    List.of(new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET))))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("cannot set " + name + " of " + topic, e);
        }
    }

    TopicDescription describe(String topic) {
        try (var admin = admin()) {
            return admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
        } catch (Exception e) {
            throw new IllegalStateException("cannot describe topic " + topic, e);
        }
    }

    /** The node leading the most partitions of {@code topic}. */
    int leaderOfMostPartitions(String topic) {
        return describe(topic).partitions().stream()
                .collect(Collectors.groupingBy(p -> p.leader().id(), Collectors.counting()))
                .entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
    }

    /** Leader node by partition, -1 for none: for the logs. */
    Map<Integer, Integer> leaders(String topic) {
        return describe(topic).partitions().stream().collect(Collectors.toMap(p -> p.partition(),
                p -> p.leader() == null ? -1 : p.leader().id()));
    }

    /** {@code docker kill}: SIGKILL, the container stays, with its log directory. */
    void kill(int node) {
        docker().killContainerCmd(broker(node).getContainerId()).exec();
    }

    /** Starts a killed node again, without waiting. */
    void start(int node) {
        docker().startContainerCmd(broker(node).getContainerId()).exec();
    }

    void pause(int node) {
        docker().pauseContainerCmd(broker(node).getContainerId()).exec();
    }

    void unpause(int node) {
        docker().unpauseContainerCmd(broker(node).getContainerId()).exec();
    }

    /**
     * Cuts the clients off node {@code node} or lets them through again: its proxy accepts connections but forwards
     * nothing, as when packets are dropped; what it holds flows on once let through, as TCP would retransmit it.
     */
    void blackhole(int node, boolean on) {
        proxies.get(node - 1).blackholed = on;
    }

    /** Delays every chunk the proxy of {@code node} forwards, either way, by {@code delay}; zero ends it. */
    void slow(int node, Duration delay) {
        proxies.get(node - 1).delayMs = delay.toMillis();
    }

    /** Denies {@link #APP} writes to {@code topic}, or lifts that. */
    void denyWrites(String topic, boolean deny) {
        var binding = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:" + APP, "*", AclOperation.WRITE, AclPermissionType.DENY));
        try (var admin = admin()) {
            if (deny) {
                admin.createAcls(List.of(binding)).all().get(30, TimeUnit.SECONDS);
            } else {
                admin.deleteAcls(List.of(new AclBindingFilter(
                        new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.LITERAL),
                        AccessControlEntryFilter.ANY))).all().get(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot change the ACLs of " + topic, e);
        }
    }

    /** Undoes every fault: the proxies forward at full speed, paused nodes run, killed ones start, all registered. */
    void heal() {
        proxies.forEach(p -> {
            p.blackholed = false;
            p.delayMs = 0;
        });
        for (int node = 1; node <= 3; node++) {
            var state = docker().inspectContainerCmd(broker(node).getContainerId()).exec().getState();
            if (Boolean.TRUE.equals(state.getPaused())) {
                unpause(node);
            } else if (!Boolean.TRUE.equals(state.getRunning())) {
                start(node);
            }
        }
        awaitBrokers(Set.of(1, 2, 3), UP);
    }

    /** Waits until the cluster lists {@code nodes} among its live brokers. */
    void awaitBrokers(Set<Integer> nodes, Duration timeout) {
        await(timeout, () -> {
            try (var admin = admin()) {
                return admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).stream()
                        .map(n -> n.id()).collect(Collectors.toSet()).containsAll(nodes);
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Override
    public void close() {
        for (int node = 1; node <= 3; node++) { // a paused container does not stop
            try {
                if (Boolean.TRUE.equals(docker().inspectContainerCmd(broker(node).getContainerId()).exec()
                        .getState().getPaused())) {
                    unpause(node);
                }
            } catch (RuntimeException ignored) {
                // best effort: Testcontainers removes the containers at exit anyway
            }
        }
        brokers.forEach(GenericContainer::stop);
        proxies.forEach(Proxy::close);
        network.close();
    }

    private GenericContainer<?> broker(int node) {
        return brokers.get(node - 1);
    }

    private static com.github.dockerjava.api.DockerClient docker() {
        return DockerClientFactory.instance().client();
    }

    private static int freePort() throws IOException {
        try (var s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static void await(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            sleep(Duration.ofMillis(100));
        }
    }

    static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** A TCP proxy from a loopback port of its own to {@code target} on localhost, one thread per direction. */
    private static final class Proxy implements AutoCloseable {

        private final ServerSocket server;
        private final int target;
        private final Set<Socket> open = ConcurrentHashMap.newKeySet();
        volatile boolean blackholed;
        volatile long delayMs;

        Proxy(int target) throws IOException {
            this.target = target;
            this.server = new ServerSocket(0, 100, InetAddress.getLoopbackAddress());
            daemon("proxy-" + port(), this::accept);
        }

        int port() {
            return server.getLocalPort();
        }

        private void accept() {
            while (!server.isClosed()) {
                try {
                    var client = server.accept();
                    daemon("proxy-" + port() + "-conn", () -> connect(client));
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void connect(Socket client) {
            var upstream = new Socket();
            open.add(client);
            open.add(upstream);
            try {
                upstream.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), target), 5_000);
                client.setTcpNoDelay(true);
                upstream.setTcpNoDelay(true);
                var in = client.getInputStream();
                var out = upstream.getOutputStream();
                daemon("proxy-" + port() + "-up", () -> pump(in, out, client, upstream));
                pump(upstream.getInputStream(), client.getOutputStream(), client, upstream);
            } catch (IOException e) {
                closeBoth(client, upstream);
            }
        }

        private void pump(InputStream in, OutputStream out, Socket a, Socket b) {
            var buffer = new byte[16 * 1024];
            try {
                int n;
                while ((n = in.read(buffer)) >= 0) {
                    while (blackholed && !server.isClosed()) {
                        Thread.sleep(20);
                    }
                    long delay = delayMs;
                    if (delay > 0) {
                        Thread.sleep(delay);
                    }
                    out.write(buffer, 0, n);
                    out.flush();
                }
            } catch (IOException | InterruptedException e) {
                // the other side went away
            } finally {
                closeBoth(a, b);
            }
        }

        private void closeBoth(Socket a, Socket b) {
            for (var s : List.of(a, b)) {
                open.remove(s);
                try {
                    s.close();
                } catch (IOException ignored) {
                    // closing
                }
            }
        }

        @Override
        public void close() {
            try {
                server.close();
            } catch (IOException ignored) {
                // closing
            }
            List.copyOf(open).forEach(s -> {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // closing
                }
            });
        }

        private static void daemon(String name, Runnable body) {
            var t = new Thread(body, name);
            t.setDaemon(true);
            t.start();
        }
    }
}
