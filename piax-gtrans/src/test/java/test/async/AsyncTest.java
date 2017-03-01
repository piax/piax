package test.async;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventException.TimeoutException;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.LatencyProvider.StarLatencyProvider;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidates;
import org.piax.gtrans.ov.async.ddll.DdllStrategy;
import org.piax.gtrans.ov.async.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.gtrans.ov.async.ddll.DdllStrategy.SetRNakMode;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.rq.RQValueProvider;
import org.piax.gtrans.ov.async.rq.RQValueProvider.CacheProvider;
import org.piax.gtrans.ov.async.rq.RQValueProvider.InsertionPointProvider;
import org.piax.gtrans.ov.async.rq.RQValueProvider.KeyProvider;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.UniqId;

public class AsyncTest {
    public static class Indirect<U> {
        U obj;
    }

    static LocalNode[] nodes;
    static StarLatencyProvider latencyProvider;

    static boolean REALTIME = false;

    private static LocalNode createNode(NodeFactory factory, int key) {
        return createNode(factory, key, NetworkParams.HALFWAY_DELAY);
    }

    private static LocalNode createNode(NodeFactory factory, int key, int latency) {
        return createNode(factory, key, "P" + key, latency);
    }

    private static LocalNode createNode(NodeFactory factory, int key,
            String peerIdStr, int latency) {
        TransportId transId = new TransportId("SimTrans");
        if (REALTIME) {
            Peer peer = Peer.getInstance(new PeerId(peerIdStr));
            DdllKey k = new DdllKey(key, new UniqId(peer.getPeerId()), "", null);
            PeerLocator loc = newLocator("emu", key);
            ChannelTransport<?> trans;
            try {
                trans = peer.newBaseChannelTransport(loc);
                LocalNode n = factory.createNode(transId, trans, k);
                latencyProvider.add(n, latency);
                return n;
            } catch (IOException | IdConflictException e) {
                throw new Error("something wrong!", e);
            }
        } else {
            UniqId p = new UniqId(peerIdStr);
            DdllKey k = new DdllKey(key, p, "", null);
            try {
                LocalNode n = factory.createNode(null, null, k);
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
            nodes[i] = createNode(factory, key, key2IdMapper.apply(key), 100);
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

    /*
     * reset the simulation environment.
     */
    public static void init() {
        Log.verbose = true;
        if (REALTIME) {
            EventExecutor.realtime.set(true);
        }
        DdllStrategy.pingPeriod.set(10000);
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
        System.gc();  // force gc for cleaning Node.instances
    }
    
    public static void dump(LocalNode[] nodes) {
        System.out.println("node dump:");
        for (int i = 0; i < nodes.length; i++) {
            System.out.println(i + ": " + nodes[i].toStringDetail());
        }
    }

    @SafeVarargs
    final void checkCompleted(CompletableFuture<Boolean>... futures) {
        for (int i = 0; i < futures.length; i++) {
            CompletableFuture<Boolean> f = futures[i];
            assertTrue(f.isDone());
            try {
                assertTrue(f.get());
            } catch (InterruptedException | ExecutionException e) {
                fail(e.toString());
            }
        }
    }

    void checkConsistent(LocalNode... nodes) {
        int s = nodes.length;
        for (int i = 0; i < s ; i++) {
            assertTrue(nodes[i].succ == nodes[(i + 1) % s]);
            assertTrue(nodes[i].pred == nodes[(i - 1 + s) % s]);
        }
    }
    
    void checkMemoryLeakage(LocalNode... nodes) {
        int s = nodes.length;
        for (int i = 0; i < s ; i++) {
            Map<Integer, RequestEvent<?, ?>> m1 = (Map)getPrivateField(nodes[i], "ongoingRequests");
            Optional<RequestEvent<?, ?>> o1 = m1.values().stream()
                .filter(req -> !(req instanceof GetCandidates))
                .findAny();
            if (o1.isPresent()) {
                System.out.println(nodes[i] + ": ongoingRequests: " + m1);
                fail();
            }
            Map<Integer, RequestEvent<?, ?>> m2 = (Map)getPrivateField(nodes[i], "unAckedRequests");
            Optional<RequestEvent<?, ?>> o2 = m2.values().stream()
                    .filter(req -> !(req instanceof GetCandidates))
                    .findAny();
            if (o2.isPresent()) {
                System.out.println(nodes[i] + ": unAckedRequests: " + m2);
                fail();
            }
        }
    }

    @Test
    public void testDdllBasicInsDel() {
        testBasicInsDel(new DdllNodeFactory());
    }

    @Test
    public void testSuzakuBasicInsDel() {
        testBasicInsDel(new SuzakuNodeFactory(3));
    }

    private void testBasicInsDel(NodeFactory factory) {
        System.out.println("** testBasicInsDel");
        init();
        nodes = createNodes(factory, 2);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Boolean> f = nodes[1].joinAsync(nodes[0]);
            EventExecutor.startSimulation(30000);
            checkCompleted(f);
        }
        checkConsistent(nodes);
        {
            CompletableFuture<Boolean> f = nodes[1].leaveAsync();
            EventExecutor.startSimulation(30000);
            checkCompleted(f);
            checkConsistent(nodes[0]);
            checkMemoryLeakage(nodes[1]);
        }

        {
            CompletableFuture<Boolean> f = nodes[0].leaveAsync();
            EventExecutor.startSimulation(30000);
            checkCompleted(f);
            checkMemoryLeakage(nodes[0]);
        }
    }

    @Test
    public void testDdllParallelInsDel() {
        testParallelInsDel(new DdllNodeFactory());
    }

    @Test
    public void testSuzakuParallelInsDel() {
        testParallelInsDel(new SuzakuNodeFactory(3));
    }

    private void testParallelInsDel(NodeFactory factory) {
        System.out.println("** testParallelInsDel");
        init();
        nodes = createNodes(factory, 4);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
            CompletableFuture<Boolean> f2 = nodes[2].joinAsync(nodes[0]);
            CompletableFuture<Boolean> f3 = nodes[3].joinAsync(nodes[0]);
            EventExecutor.startSimulation(30000);
            checkCompleted(f1, f2, f3);
            checkConsistent(nodes);
        }
        
        {
            CompletableFuture<Boolean> f1 = nodes[1].leaveAsync();
            CompletableFuture<Boolean> f2 = nodes[2].leaveAsync();
            CompletableFuture<Boolean> f3 = nodes[3].leaveAsync();
            EventExecutor.startSimulation(30000);
            checkCompleted(f1, f2, f3);
            checkMemoryLeakage(nodes[1], nodes[2], nodes[3]);
            checkConsistent(nodes[0]);
        }
    }

    @Test
    public void testDdllFix1() {
        testFix1(new DdllNodeFactory());
    }

    @Test
    public void testSuzakuFix1() {
        testFix1(new SuzakuNodeFactory(3));
    }

    private void testFix1(NodeFactory factory) {
        System.out.println("** testFix");
        init();
        nodes = createNodes(factory, 4);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
            CompletableFuture<Boolean> f2 = nodes[2].joinAsync(nodes[0]);
            CompletableFuture<Boolean> f3 = nodes[3].joinAsync(nodes[0]);
            EventExecutor.sched(10000, () -> {
                nodes[1].fail();
                nodes[2].fail();  
            });
            EventExecutor.startSimulation(30000);
            checkCompleted(f1, f2, f3);
            dump(nodes);
            checkConsistent(nodes[0], nodes[3]);
        }
    }

    @Test
    public void testDdllFix2() {
        testFix2(new DdllNodeFactory());
    }

    @Test
    public void testSuzakuFix2() {
        testFix2(new SuzakuNodeFactory(3));
    }

    private void testFix2(NodeFactory factory) {
        System.out.println("** testFix2");
        init();
        nodes = createNodes(factory, 3);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
            CompletableFuture<Boolean> f2 = nodes[2].joinAsync(nodes[0]);
            Indirect<CompletableFuture<Boolean>> f3 = new Indirect<>();
            EventExecutor.sched(10000, () -> {
                nodes[1].fail();
                f3.obj = nodes[2].leaveAsync();
            });
            EventExecutor.startSimulation(30000);
            assertNotNull(f3.obj);
            checkCompleted(f1, f2, f3.obj);
            checkMemoryLeakage(nodes[2]);
            dump(nodes);
            checkConsistent(nodes[0]);
        }
    }

    @Test
    public void testDdllFix3() {
        testFix3(new DdllNodeFactory());
    }

    @Test
    public void testSuzakuFix3() {
        testFix3(new SuzakuNodeFactory(3));
    }

    private void testFix3(NodeFactory factory) {
        System.out.println("** testFix3");
        init();
        nodes = createNodes(factory, 2);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
            Indirect<CompletableFuture<Boolean>> f2 = new Indirect<>();
            EventExecutor.sched(10000, () -> {
                nodes[0].fail();
                f2.obj = nodes[1].leaveAsync();
            });
            EventExecutor.startSimulation(30000);
            assertNotNull(f2.obj);
            dump(nodes);
            checkCompleted(f1, f2.obj);
            checkMemoryLeakage(nodes[1]);
        }
    }
    
    @Test
    public void testDdllJoinFail() {
        System.out.println("** testDdllJoinFail");
        init();
        nodes = createNodes(new DdllNodeFactory(), 2);
        nodes[0].joinInitialNode();
        nodes[0].fail();
        {
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
            f1.whenComplete((rc, exc) -> 
                System.out.println("rc=" + rc + ", exc=" + exc));
            EventExecutor.startSimulation(30000);
            dump(nodes);
            assertTrue(f1.isDone());
            try {
                f1.get();
                fail();
            } catch (InterruptedException | ExecutionException e) {
                assertTrue(e.getCause() instanceof TimeoutException);
            }
        }
    }
    
    @Test
    public void testRQ1Aggregate() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new DdllNodeFactory(), opts, new FastValueProvider(),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    @Test
    public void testRQ1Direct() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ1(new DdllNodeFactory(), opts, new FastValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }
    
    @Test
    public void testRQ1NoResponse() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.NO_RESPONSE);
        testRQ1(new DdllNodeFactory(), opts, new FastValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testRQ1AggregateSlow() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new DdllNodeFactory(), opts, new SlowValueProvider(2000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testRQ1Timeout() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        opts.setTimeout(10000);
        testRQ1(new DdllNodeFactory(), opts, new SlowValueProvider(20000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testRQ1AggregateSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new SuzakuNodeFactory(3), opts, new FastValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testRQ1DirectSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ1(new SuzakuNodeFactory(3), opts, new FastValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    private void testRQ1(NodeFactory base, 
            TransOptions opts, RQValueProvider<Integer> provider,
            Range<Integer> range, List<Integer> expect) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testRQ1");
        init();
        createAndInsert(factory, 5);
        {
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Integer>> results = new ArrayList<>();
            nodes[0].rangeQueryAsync(ranges, provider, 
                    opts, (ret) -> {
                System.out.println("GOT RESULT: " + ret);
                results.add(ret);
            });
            EventExecutor.startSimulation(30000);
            assertTrue(!results.isEmpty());
            assertTrue(results.get(results.size() - 1 ) == null);
            List<?> rvals = results.stream()
                    .filter(Objects::nonNull)
                    .map(rv -> rv.getValue())   // extract Integer
                    .sorted()
                    .collect(Collectors.toList());
            System.out.println("RVALS = " + rvals);
            System.out.println("EXPECT = " + expect);
            assertTrue(rvals.equals(expect));
            checkMemoryLeakage(nodes);
        }
    }
    
    @Test
    public void testRQInsertionPointProvider() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ2(new SuzakuNodeFactory(3), opts, new InsertionPointProvider(),
                new Range<Integer>(150), "[N100!P100, N200!P200]");
    }

    private void testRQ2(NodeFactory base, 
            TransOptions opts, RQValueProvider<Node[]> provider,
            Range<Integer> range, String expect) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testRQ2");
        init();
        createAndInsert(factory, 5);
        {
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Node[]>> results = new ArrayList<>();
            nodes[0].rangeQueryAsync(ranges, provider, 
                    opts, (ret) -> {
                System.out.println("GOT RESULT: " + ret);
                results.add(ret);
            });
            EventExecutor.startSimulation(30000);
            assertTrue(!results.isEmpty());
            assertTrue(results.get(results.size() - 1 ) == null);
            List<Node> rvals = results.stream()
                    .filter(Objects::nonNull)
                    .map(rv -> rv.getValue())   // extract Node[]
                    .flatMap((Node[] ns) -> Stream.of(ns))
                    .collect(Collectors.toList());
            assertTrue(rvals.size() == 2);
            String rstr = rvals.toString();
            System.out.println("RVALS = " + rstr);
            System.out.println("EXPECT = " + expect);
            assertTrue(rstr.toString().equals(expect));
            checkMemoryLeakage(nodes);
        }
    }

    @Test
    public void testRetransDirectSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRetrans(new SuzakuNodeFactory(3), opts, new FastValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
    }

    @Test
    public void testSlowRetransAggregateSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        opts.setRetransMode(RetransMode.SLOW);
        opts.setTimeout(15*1000);
        testRetrans(new SuzakuNodeFactory(3), opts, new FastValueProvider(),
                new Range<Integer>(100, true, 400, true),
                Arrays.asList(100, 300, 400));
    }

    @Test
    public void testFastRetransAggregateSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        opts.setRetransMode(RetransMode.FAST);
        opts.setTimeout(15*1000);
        testRetrans(new SuzakuNodeFactory(3), opts, new FastValueProvider(),
                new Range<Integer>(100, true, 400, true),
                Arrays.asList(100, 300, 400));
    }

    @Test
    public void testCache() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRetrans(new SuzakuNodeFactory(3), opts, 
                new SlowCacheValueProvider(10000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
        System.out.println(getPrivateField(EventExecutor.class, "timeq"));
    }

    private void testRetrans(NodeFactory base,
            TransOptions opts, RQValueProvider<Integer> provider,
            Range<Integer> range, List<Integer> expect) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testSlowRetrans");
        init();
        createAndInsert(factory, 5);
        {
            nodes[2].fail();
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Integer>> results = new ArrayList<>();
            nodes[0].rangeQueryAsync(ranges, provider, 
                    opts, (ret) -> {
                System.out.println("GOT RESULT: " + ret);
                results.add(ret);
            });
            EventExecutor.startSimulation(30000);
            assertTrue(!results.isEmpty());
            assertTrue(results.get(results.size() - 1 ) == null);
            List<?> rvals = results.stream()
                    .filter(Objects::nonNull)
                    .map(rv -> rv.getValue())   // extract Integer
                    .sorted()
                    .collect(Collectors.toList());
            System.out.println("RVALS = " + rvals);
            System.out.println("EXPECT = " + expect);
            assertTrue(rvals.equals(expect));
            checkMemoryLeakage(nodes[0], nodes[1], nodes[3], nodes[4]);
        }
    }
    
    @Test
    public void testMultikeySuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testMultikey(new SuzakuNodeFactory(3), opts, new KeyProvider(),
                new Range<Integer>(0, true, 500, false),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    private void testMultikey(NodeFactory base,
            TransOptions opts, RQValueProvider<DdllKey> provider,
            Range<Integer> range, List<Integer> expect) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testMultikey");
        init();
        createAndInsert(factory, 5, k -> {
            if (k == 300 || k == 400) {
                return "P100";
            } else {
                return "P" + k;
            }
        });
        {
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<DdllKey>> results = new ArrayList<>();
            nodes[0].rangeQueryAsync(ranges, provider, 
                    opts, (ret) -> {
                System.out.println("GOT RESULT: " + ret);
                results.add(ret);
            });
            EventExecutor.startSimulation(30000);
            assertTrue(!results.isEmpty());
            assertTrue(results.get(results.size() - 1 ) == null);
            List<?> rvals = results.stream()
                    .filter(Objects::nonNull)
                    .map(rv -> rv.getValue())   // extract DdllKey
                    .map(rv -> rv.getPrimaryKey()) // extract Comparable
                    .sorted()
                    .collect(Collectors.toList());
            System.out.println("RVALS = " + rvals);
            System.out.println("EXPECT = " + expect);
            assertTrue(rvals.equals(expect));
            checkMemoryLeakage(nodes[0], nodes[1], nodes[3], nodes[4]);
        }
    }

    private void createAndInsert(NodeFactory factory, int num) {
        nodes = createNodes(factory, num);
        insertAll();
    }

    private void createAndInsert(NodeFactory factory, int num, 
            Function<Integer, String> mapper) {
        nodes = createNodes(factory, num, mapper);
        insertAll();
    }

    private void insertAll() { 
        int num = nodes.length;
        nodes[0].joinInitialNode();
        @SuppressWarnings("unchecked")
        CompletableFuture<Boolean>[] futures = new CompletableFuture[num];
        for (int i = 1; i < num; i++) {
            futures[i] = nodes[i].joinAsync(nodes[0]);
        }
        EventExecutor.startSimulation(30000);
        for (int i = 1; i < num; i++) {
            checkCompleted(futures[i]);
        }
        checkConsistent(nodes);
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
    public static class FastValueProvider extends RQValueProvider<Integer> {
        @Override
        public CompletableFuture<Integer> get(LocalNode node, DdllKey key) {
            return CompletableFuture.completedFuture(result(key));
        }
        int result(DdllKey key) {
            int pkey = (int)key.getPrimaryKey();
            return pkey;
        }
    }
    
    public static class SlowValueProvider extends RQValueProvider<Integer> {
        final int delay;
        public SlowValueProvider(int delay) {
            this.delay = delay;
        }
        @Override
        public CompletableFuture<Integer> get(LocalNode node, DdllKey key) {
            CompletableFuture<Integer> f = new CompletableFuture<>();
            EventExecutor.sched("slowvalueprovider", delay, () -> {
                System.out.println("provider finished: " + key);
                f.complete(result(key));
            });
            return f;
        }
        int result(DdllKey key) {
            int pkey = (int)key.getPrimaryKey();
            return pkey;
        }
    }
    
    public static class SlowCacheValueProvider extends CacheProvider<Integer> {
        int count;
        final int delay;
        public SlowCacheValueProvider(int delay) {
            super(30 * 1000);
            this.delay = delay;
        }
        @Override
        public CompletableFuture<Integer> get(LocalNode node, DdllKey key) {
            CompletableFuture<Integer> f = new CompletableFuture<>();
            int val = result(key);
            EventExecutor.sched("slowvalueprovider", delay, () -> {
                System.out.println("provider finished: " + key + ", count=" + count);
                f.complete(val);
            });
            count++;
            return f;
        }
        int result(DdllKey key) {
            int pkey = (int)key.getPrimaryKey();
            return count * 1000 + pkey;
        }
    }
}