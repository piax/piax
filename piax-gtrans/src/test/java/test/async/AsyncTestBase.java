package test.async;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.Indirect;
import org.piax.gtrans.async.LatencyProvider.StarLatencyProvider;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidates;
import org.piax.gtrans.ov.async.ddll.DdllStrategy;
import org.piax.gtrans.ov.async.ddll.DdllStrategy.SetRNakMode;
import org.piax.gtrans.ov.async.rq.RQAdapter;
import org.piax.gtrans.ov.async.rq.RQAdapter.CacheAdapter;
import org.piax.gtrans.ov.async.rq.RQAdapter.InsertionPointAdapter;
import org.piax.gtrans.ov.async.rq.RQAdapter.KeyAdapter;
import org.piax.gtrans.ov.async.rq.RQStrategy;
import org.piax.gtrans.ov.async.suzaku.SuzakuEvent.GetFTEntEvent;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.UniqId;

public class AsyncTestBase {
    static LocalNode[] nodes;
    static StarLatencyProvider latencyProvider;

    static boolean REALTIME = true;

    static LocalNode createNode(NodeFactory factory, int key) {
        return createNode(factory, key, NetworkParams.HALFWAY_DELAY);
    }

    static LocalNode createNode(NodeFactory factory, int key, long latency) {
        return createNode(factory, key, "P" + key, latency);
    }

    private static LocalNode createNode(NodeFactory factory, int key,
            String peerIdStr, long latency) {
        TransportId transId = new TransportId("SimTrans");
        if (REALTIME) {
            Peer peer = Peer.getInstance(new PeerId(peerIdStr));
            DdllKey k =
                    new DdllKey(key, new UniqId(peer.getPeerId()), "", null);
            PeerLocator loc = newLocator("emu", key);
            ChannelTransport<?> trans;
            try {
                trans = peer.newBaseChannelTransport(loc);
                LocalNode n = new LocalNode(transId, trans, k);
                factory.setupNode(n);
                latencyProvider.add(n, latency);
                return n;
            } catch (IOException | IdConflictException e) {
                System.out.println("***** got " + e);
                e.printStackTrace();
                throw new Error("something wrong!", e);
            }
        } else {
            UniqId p = new UniqId(peerIdStr);
            DdllKey k = new DdllKey(key, p, "", null);
            try {
                LocalNode n = new LocalNode(null, null, k);
                factory.setupNode(n);
                latencyProvider.add(n, latency);
                return n;
            } catch (IOException | IdConflictException e) {
                throw new Error("something wrong!", e);
            }
        }
    }

    static LocalNode[] createNodes(NodeFactory factory, int num) {
        return createNodes(factory, num, k -> "P" + k);
    }

    static LocalNode[] createNodes(NodeFactory factory, int num,
            Function<Integer, String> key2IdMapper) {
        nodes = new LocalNode[num];
        for (int i = 0; i < num; i++) {
            int key = i * 100;
            nodes[i] = createNode(factory, key, key2IdMapper.apply(key), 50);
        }
        return nodes;
    }

    static PeerLocator newLocator(String locatorType, int vport) {
        PeerLocator peerLocator;
        if (locatorType.equals("emu")) {
            peerLocator = new EmuLocator(vport);
        } else if (locatorType.equals("udp")) {
            peerLocator = new UdpLocator(
                    new InetSocketAddress("localhost", 10000 + vport));
        } else {
            peerLocator = new TcpLocator(
                    new InetSocketAddress("localhost", 10000 + vport));
        }
        return peerLocator;
    }

    public static void init() {
        init(10000);
    }

    /*
     * reset the simulation environment.
     */
    public static void init(int pingPeriod) {
        Log.verbose = true;
        if (REALTIME) {
            EventExecutor.realtime.set(true);
        }
        DdllStrategy.pingPeriod.set(pingPeriod);
        DdllStrategy.setrnakmode.set(SetRNakMode.SETRNAK_OPT2);

        // clear strong references to "Node" to cleanup Node.instances.
        EventExecutor.reset();
        if (nodes != null) {
            // reset the previous execution
            for (LocalNode n : nodes) {
                if (n != null) {
                    n.cleanup();
                }
            }
            nodes = null;
        }
        latencyProvider = new StarLatencyProvider();
        EventExecutor.setLatencyProvider(latencyProvider);
        System.gc(); // force gc for cleaning Node.instances
    }

    public static void dump(LocalNode[] nodes) {
        System.out.println("node dump:");
        for (int i = 0; i < nodes.length; i++) {
            System.out.println(i + ": " + nodes[i].toStringDetail());
        }
    }

    @SafeVarargs
    static final void checkCompleted(CompletableFuture<Boolean>... futures) {
        for (int i = 0; i < futures.length; i++) {
            CompletableFuture<Boolean> f = futures[i];
            assertTrue(f.isDone());
            try {
                assertTrue(f.get());
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Node " + i);
                e.getCause().printStackTrace();
                fail(e.toString());
            }
        }
    }

    static void checkConsistent(LocalNode... nodes) {
        int s = nodes.length;
        for (int i = 0; i < s; i++) {
            assertTrue(nodes[i].succ == nodes[(i + 1) % s]);
            assertTrue(nodes[i].pred == nodes[(i - 1 + s) % s]);
        }
    }
    
    static boolean isTransientRequest(RequestEvent<?, ?> req) {
        if (req instanceof GetCandidates) {
            return true;
        }
        if (req instanceof GetFTEntEvent) {
            return true;
        }
        return false;
    }

    static void checkMemoryLeakage(LocalNode... nodes) {
        int s = nodes.length;
        for (int i = 0; i < s; i++) {
            Map<Integer, RequestEvent<?, ?>> m1 =
                    (Map) getPrivateField(nodes[i], "ongoingRequests");
            Optional<RequestEvent<?, ?>> o1 = m1.values().stream()
                    .filter(req -> !isTransientRequest(req)).findAny();
            if (o1.isPresent()) {
                System.out.println(nodes[i] + ": ongoingRequests: " + m1);
                fail();
            }
            Map<Integer, RequestEvent<?, ?>> m2 =
                    (Map) getPrivateField(nodes[i], "unAckedRequests");
            Optional<RequestEvent<?, ?>> o2 = m2.values().stream()
                    .filter(req -> !isTransientRequest(req)).findAny();
            if (o2.isPresent()) {
                System.out.println(nodes[i] + ": unAckedRequests: " + m2);
                fail();
            }
        }
    }

    void createAndInsert(NodeFactory factory, int num,
            RQAdapter<?> adapter) {
        createAndInsert(factory, num, adapter, null);
    }
    
    void createAndInsert(NodeFactory factory, int num,
            RQAdapter<?> adapter, Runnable after) {
        createAndInsert(factory, num, adapter, after, 30*1000);
    }

    void createAndInsert(NodeFactory factory, int num,
            RQAdapter<?> adapter, Runnable after, long duration) {
        nodes = createNodes(factory, num);
        for (LocalNode node: nodes) {
            NodeStrategy s = node.getTopStrategy();
            if (!(adapter instanceof InsertionPointAdapter 
                    || adapter instanceof KeyAdapter)) {
                ((RQStrategy)s).registerAdapter(adapter);
            }
        }
        insertAll(duration, after);
    }

    void createAndInsert(NodeFactory factory, int num) {
        nodes = createNodes(factory, num);
        insertAll(30 * 1000);
    }

    void createAndInsert(NodeFactory factory, int num,
            Function<Integer, String> mapper) {
        nodes = createNodes(factory, num, mapper);
        insertAll(30 * 1000);
    }

    void insertAll(long duration) {
        insertAll(duration, null);
    }
    
    void insertAll(long duration, Runnable after) {
        int num = nodes.length;
        nodes[0].joinInitialNode();
        @SuppressWarnings("unchecked")
        // insert all nodes sequentially
        CompletableFuture<Boolean>[] futures = new CompletableFuture[num];
        Indirect<Consumer<Integer>> job = new Indirect<>();
        /*job.val = (index) -> {
            if (index < num) {
                futures[index] = nodes[index].joinAsync(nodes[index - 1]);
                futures[index].thenRun(() -> job.val.accept(index + 1));
            } else if (after != null) {
                after.run();
            }
        };
        job.val.accept(1);*/
        job.val = (index) -> {
            System.out.println("T" + EventExecutor.getVTime() + ": inserting " + index);
            if (index > 0) {
                futures[index] = nodes[index].joinAsync(nodes[0]);
                futures[index].thenRun(() -> job.val.accept(index - 1));
            } else if (after != null) {
                after.run();
            }
        };
        job.val.accept(nodes.length - 1);
        EventExecutor.startSimulation(duration);
        for (int i = 1; i < num; i++) {
            checkCompleted(futures[i]);
        }
        if (after == null) {
            checkConsistent(nodes);
        }
    }

    public static Object getPrivateField(Object target, String field) {
        try {
            Class<?> c = target.getClass();
            Field f = c.getDeclaredField(field);
            f.setAccessible(true);
            return f.get(target);
        } catch (IllegalAccessException | IllegalArgumentException
                | SecurityException | NoSuchFieldException e) {
            fail(e.toString());
            return null;
        }
    }

    public static Object getPrivateField(Class<?> clazz, String field) {
        try {
            Field f = clazz.getDeclaredField(field);
            f.setAccessible(true);
            return f.get(null);
        } catch (IllegalAccessException | IllegalArgumentException
                | SecurityException | NoSuchFieldException e) {
            fail(e.toString());
            return null;
        }
    }

    public static class FastValueProvider extends RQAdapter<Integer> {
        public FastValueProvider(Consumer<RemoteValue<Integer>> resultsReceiver) {
            super(resultsReceiver);
        }
        @Override
        public CompletableFuture<Integer> get(RQAdapter<Integer> received,
                LocalNode node) {
            return CompletableFuture.completedFuture(result(node.key));
        }

        int result(DdllKey key) {
            int pkey = (int) key.getRawKey();
            return pkey;
        }
    }

    public static class SlowValueProvider extends RQAdapter<Integer> {
        final int delay;

        public SlowValueProvider(Consumer<RemoteValue<Integer>> resultsReceiver, int delay) {
            super(resultsReceiver);
            this.delay = delay;
        }

        @Override
        public CompletableFuture<Integer> get(RQAdapter<Integer> received,
                LocalNode node) {
            SlowValueProvider r = (SlowValueProvider) received;
            CompletableFuture<Integer> f = new CompletableFuture<>();
            EventExecutor.sched("slowvalueprovider", r.delay, () -> {
                System.out.println("provider finished: " + node.key);
                f.complete(result(node.key));
            });
            return f;
        }

        int result(DdllKey key) {
            int pkey = (int) key.getRawKey();
            return pkey;
        }
    }

    // XXX
    public static class SlowCacheValueProvider extends CacheAdapter<Integer> {
        int count;
        final int delay;

        public SlowCacheValueProvider(Consumer<RemoteValue<Integer>> resultsReceiver, int delay) {
            super(resultsReceiver, 30 * 1000);
            this.delay = delay;
        }

        @Override
        public CompletableFuture<Integer> get(RQAdapter<Integer> received,
                LocalNode node) {
            SlowCacheValueProvider p = (SlowCacheValueProvider)received; 
            CompletableFuture<Integer> f = new CompletableFuture<>();
            int val = result(node.key);
            EventExecutor.sched("slowvalueprovider", p.delay, () -> {
                System.out.println(
                        "provider finished: " + node.key + ", count=" + p.count);
                f.complete(val);
            });
            count++;
            return f;
        }

        int result(DdllKey key) {
            int pkey = (int) key.getRawKey();
            return count * 1000 + pkey;
        }
    }
    
    public static class ErrorProvider extends RQAdapter<Integer> {
        public ErrorProvider(Consumer<RemoteValue<Integer>> resultsReceiver) {
            super(resultsReceiver);
        }
        @Override
        public CompletableFuture<Integer> get(RQAdapter<Integer> received,
                LocalNode node) {
            throw new Error("Error(" + node.key + ")");
        }
    }
}