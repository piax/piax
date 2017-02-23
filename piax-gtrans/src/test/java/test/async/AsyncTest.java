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
import java.util.stream.Collectors;

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
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventException.TimeoutException;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.Sim;
import org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidates;
import org.piax.gtrans.ov.async.ddll.DdllStrategy;
import org.piax.gtrans.ov.async.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.gtrans.ov.async.ddll.DdllStrategy.SetRNakMode;
import org.piax.gtrans.ov.async.rq.RQValueProvider;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.rq.RQValueProvider.SimpleValueProvider;
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
    static boolean REALTIME = false;

    private static LocalNode createNode(NodeFactory factory, int key) {
        return createNode(factory, key, NetworkParams.HALFWAY_DELAY);
    }

    private static LocalNode createNode(NodeFactory factory, int key, int latency) {
        TransportId transId = new TransportId("SimTrans");
        if (REALTIME) {
            Peer peer = Peer.getInstance(new PeerId("P" + key));
            DdllKey k = new DdllKey(key, new UniqId(peer.getPeerId()), "", null);
            PeerLocator loc = newLocator("emu", key);
            ChannelTransport<?> trans;
            try {
                trans = peer.newBaseChannelTransport(loc);
                return factory.createNode(transId, trans, k, latency);
            } catch (IOException | IdConflictException e) {
                throw new Error("something wrong!", e);
            }
        } else {
            UniqId p = new UniqId("P" + key);
            DdllKey k = new DdllKey(key, p, "", null);
            try {
                return factory.createNode(null, null, k, latency);
            } catch (IOException | IdConflictException e) {
                throw new Error("something wrong!", e);
            }
        }
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
        Sim.verbose = true;
        if (REALTIME) {
            EventExecutor.realtime.set(true);
        }
        DdllStrategy.pingPeriod.set(10000);
        DdllStrategy.setrnakmode.set(SetRNakMode.SETRNAK_OPT2);
        EventExecutor.reset();
    }
    
    static LocalNode[] createNodes(NodeFactory factory, int num) {
        nodes = new LocalNode[num];
        for (int i = 0; i < num; i++) {
            nodes[i] = createNode(factory, i * 100, 100);
        }
        return nodes;
    }
    
    public static void dump(LocalNode[] nodes) {
        System.out.println("node dump:");
        for (int i = 0; i < nodes.length; i++) {
            System.out.println(i + ": " + nodes[i].toStringDetail()
                    + ", latency=" + nodes[i].latency);
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
        testRQ1(new DdllNodeFactory(), opts, new SimpleValueProvider(),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    @Test
    public void testRQ1Direct() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ1(new DdllNodeFactory(), opts, new SimpleValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
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
    public void testRQ1DirectSlow() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        opts.setTimeout(10000);
        testRQ1(new DdllNodeFactory(), opts, new SlowValueProvider(20000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }


    @Test
    public void testRQ1AggregateSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new SuzakuNodeFactory(3), opts, new SimpleValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testRQ1DirectSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ1(new SuzakuNodeFactory(3), opts, new SimpleValueProvider(),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    private void testRQ1(NodeFactory base, 
            TransOptions opts, RQValueProvider<DdllKey> provider,
            Range<Integer> range, List<Integer> expect) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testRQ1");
        init();
        createAndInsert(factory, 5);
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
            checkMemoryLeakage(nodes);
        }
    }

    private void createAndInsert(NodeFactory factory, int num) {
        nodes = createNodes(factory, num);
        nodes[0].joinInitialNode();
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
    
    public static class SlowValueProvider implements RQValueProvider<DdllKey> {
        final int delay;
        public SlowValueProvider(int delay) {
            this.delay = delay;
        }
        @Override
        public CompletableFuture<DdllKey> get(DdllKey key) {
            CompletableFuture<DdllKey> f = new CompletableFuture<>();
            EventExecutor.sched("slowvalueprovider", delay, () -> {
                System.out.println("provider finished: " + key);
                f.complete(key);
            });
            return f;
        }
    }
}