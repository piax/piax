package test.async;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.piax.ayame.EventException.NetEventException;
import org.piax.ayame.EventException.TimeoutException;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.Indirect;
import org.piax.ayame.Node;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.ov.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQAdapter.InsertionPointAdapter;
import org.piax.ayame.ov.rq.RQAdapter.KeyAdapter;
import org.piax.ayame.ov.rq.RQStrategy.RQNodeFactory;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.common.DdllKey;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAsync extends AsyncTestBase {
    private static final Logger logger = LoggerFactory
            .getLogger(TestAsync.class);
    @Test
    public void testTerminate1() {
        EventExecutor.reset();
        Indirect<Boolean> chk1 = new Indirect<>(false), chk2 = new Indirect<>(false);
        EventExecutor.sched("test.testTerminate1-1", 50, () -> {
            logger.debug("chk1: " + EventExecutor.getVTime());
            chk1.val = true;
        });
        EventExecutor.sched("test.testTerminate1-2", 150, () -> {
            logger.debug("chk2: "+ EventExecutor.getVTime());
            chk2.val = true;
        });

        EventExecutor.startExecutorThread();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {}
        EventExecutor.terminate();
        EventExecutor.startExecutorThread();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {}
        EventExecutor.terminate();
        EventExecutor.realtime.set(false);
        assertTrue(chk1.val);
        assertTrue(chk2.val);
    }

    @Test
    public void testTerminate2() {
        EventExecutor.reset();
        Indirect<Boolean> chk1 = new Indirect<>(false), chk2 = new Indirect<>(false);
        EventExecutor.sched("test.testTerminate2-1", 50, () -> {
            logger.debug("chk1: " + EventExecutor.getVTime());
            chk1.val = true;
        });
        EventExecutor.sched("test.testTerminate2-2", 1000, () -> {
            logger.debug("chk2: "+ EventExecutor.getVTime());
            chk2.val = true;
        });
        EventExecutor.startExecutorThread();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {}
        EventExecutor.terminate();
        EventExecutor.reset();
        EventExecutor.startExecutorThread();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {}
        EventExecutor.terminate();
        EventExecutor.realtime.set(false);
        assertTrue(chk1.val);
        assertFalse(chk2.val);
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
        logger.debug("** testBasicInsDel");
        init();
        nodes = createNodes(factory, 2);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Void> f = nodes[1].joinAsync(nodes[0]);
            EventExecutor.startSimulation(30000);
            checkCompleted(f);
        }
        checkConsistent(nodes);
        {
            CompletableFuture<Void> f = nodes[1].leaveAsync();
            EventExecutor.startSimulation(30000);
            checkCompleted(f);
            checkConsistent(nodes[0]);
            checkMemoryLeakage(nodes[1]);
        }

        {
            CompletableFuture<Void> f = nodes[0].leaveAsync();
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
        logger.debug("** testParallelInsDel");
        init();
        nodes = createNodes(factory, 4);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Void> f1 = nodes[1].joinAsync(nodes[0]);
            CompletableFuture<Void> f2 = nodes[2].joinAsync(nodes[0]);
            CompletableFuture<Void> f3 = nodes[3].joinAsync(nodes[0]);
            EventExecutor.startSimulation(30000);
            checkCompleted(f1, f2, f3);
            checkConsistent(nodes);
        }
        
        {
            CompletableFuture<Void> f1 = nodes[1].leaveAsync();
            CompletableFuture<Void> f2 = nodes[2].leaveAsync();
            CompletableFuture<Void> f3 = nodes[3].leaveAsync();
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

    // simple failure
    private void testFix1(NodeFactory factory) {
        logger.debug("** testFix");
        init();
        nodes = createNodes(factory, 4);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Void> f1 = nodes[1].joinAsync(nodes[0]);
            CompletableFuture<Void> f2 = nodes[2].joinAsync(nodes[0]);
            CompletableFuture<Void> f3 = nodes[3].joinAsync(nodes[0]);
            EventExecutor.sched("test.testFix1", 10000, () -> {
                nodes[1].fail();
                nodes[2].fail();  
            });
            EventExecutor.startSimulation(30000);
            checkCompleted(f1, f2, f3);
            dump(nodes);
            checkConsistent(nodes[0], nodes[3]);
            @SuppressWarnings("unchecked")
            Set<Node> failed3 = (Set<Node>)getPrivateField(nodes[3],
                    "possiblyFailedNodes");
            assertTrue(!failed3.isEmpty());
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

    // leave and fail (3 nodes)
    private void testFix2(NodeFactory factory) {
        logger.debug("** testFix2");
        init();
        nodes = createNodes(factory, 3);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Void> f1 = nodes[1].joinAsync(nodes[0]);
            CompletableFuture<Void> f2 = nodes[2].joinAsync(nodes[0]);
            Indirect<CompletableFuture<Void>> f3 = new Indirect<>();
            EventExecutor.sched("test.testFix2", 10000, () -> {
                nodes[1].fail();
                f3.val = nodes[2].leaveAsync();
            });
            EventExecutor.startSimulation(30000);
            assertNotNull(f3.val);
            checkCompleted(f1, f2, f3.val);
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

    // leave and fail (2 nodes)
    private void testFix3(NodeFactory factory) {
        logger.debug("** testFix3");
        init();
        nodes = createNodes(factory, 2);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Void> f1 = nodes[1].joinAsync(nodes[0]);
            Indirect<CompletableFuture<Void>> f2 = new Indirect<>();
            EventExecutor.sched("test.testFix3", 10000, () -> {
                nodes[0].fail();
                f2.val = nodes[1].leaveAsync();
            });
            EventExecutor.startSimulation(30000);
            assertNotNull(f2.val);
            dump(nodes);
            checkCompleted(f1, f2.val);
            checkMemoryLeakage(nodes[1]);
        }
    }
    
    // join and fail
    @Test
    public void testDdllJoinFail1() {
        logger.debug("** testDdllJoinFail1");
        init();
        nodes = createNodes(new DdllNodeFactory(), 2);
        nodes[0].joinInitialNode();
        nodes[0].fail();
        {
            CompletableFuture<Void> f1 = nodes[1].joinAsync(nodes[0]);
            f1.whenComplete((rc, exc) -> 
                logger.debug("rc=" + rc + ", exc=" + exc));
            EventExecutor.startSimulation(40000);
            dump(nodes);
            assertTrue(f1.isDone());
            try {
                f1.get();
                fail("");
            } catch (InterruptedException | ExecutionException e) {
                assertTrue(e.getCause() instanceof TimeoutException
                        || e.getCause() instanceof NetEventException);
            }
        }
    }

    // join and fail
    @Test
    public void testDdllJoinFail2() {
        logger.debug("** testDdllJoinFail2");
        init();
        nodes = createNodes(new DdllNodeFactory(), 3);
        nodes[0].joinInitialNode();
        {
            Indirect<CompletableFuture<Void>> f2 = new Indirect<>();
            CompletableFuture<Void> f1 = nodes[1].joinAsync(nodes[0]);
            f1.thenRun(() -> {
                nodes[1].fail();
                f2.val = nodes[2].joinAsync(nodes[0]);
            });
            EventExecutor.startSimulation(30000);
            dump(nodes);
            assertTrue(f1.isDone());
            assertTrue(f2.val.isDone());
            checkConsistent(nodes[0], nodes[2]);
        }
    }
    
    // join and fail (RQStrategy)
    @Test
    public void testDdllJoinFail3() {
        logger.debug("** testDdllJoinFail3");
        init();
        nodes = createNodes(new RQNodeFactory(new SuzakuNodeFactory(3)), 3);
        nodes[0].joinInitialNode();
        {
            Indirect<CompletableFuture<Void>> f2 = new Indirect<>();
            CompletableFuture<Void> f1 = nodes[1].joinAsync(nodes[0]);
            f1.thenRun(() -> {
                nodes[1].fail();
                f2.val = nodes[2].joinAsync(nodes[0]);
            });
            EventExecutor.startSimulation(30000);
            dump(nodes);
            assertTrue(f1.isDone());
            assertTrue(f2.val.isDone());
            checkConsistent(nodes[0], nodes[2]);
        }
    }

    @Test
    public void testRQ1AggregateNone() {
        testRQ1With(ResponseType.AGGREGATE, RetransMode.NONE);
    }

    @Test
    public void testRQ1AggregateReliable() {
        testRQ1With(ResponseType.AGGREGATE, RetransMode.RELIABLE);
    }

    @Test
    public void testRQ1AggregateFast() {
        testRQ1With(ResponseType.AGGREGATE, RetransMode.FAST);
    }

    @Test
    public void testRQ1DirectNone() {
        testRQ1With(ResponseType.DIRECT, RetransMode.NONE);
    }

    @Test
    public void testRQ1DirectReliable() {
        testRQ1With(ResponseType.DIRECT, RetransMode.RELIABLE);
    }

    @Test
    public void testRQ1DirectFast() {
        testRQ1With(ResponseType.DIRECT, RetransMode.FAST);
    }

    private void testRQ1With(ResponseType response, RetransMode retrans) {
        TransOptions opts = new TransOptions(response, retrans);
        testRQ1(new DdllNodeFactory(), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    @Test
    public void testRQ1NoResponse() {
        TransOptions opts = new TransOptions(ResponseType.NO_RESPONSE);
        testRQ1(new DdllNodeFactory(), opts, 
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testRQ1AggregateSlowProvider() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testRQ1(new DdllNodeFactory(), opts,
                (receiver) -> new SlowValueProvider(receiver, 3000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testRQ1Timeout() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT).timeout(10000);
        testRQ1(new DdllNodeFactory(), opts,
                (receiver) -> new SlowValueProvider(receiver, 20000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testRQ1AggregateSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testRQ1(new SuzakuNodeFactory(3), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testRQ1DirectSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT);
        testRQ1(new SuzakuNodeFactory(3), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }
    
    @Test
    public void testRQ1ExceptionInProvider() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testRQ1(new DdllNodeFactory(), opts, 
                receiver -> new ErrorProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(), 
                "[Error(0!P0), Error(100!P100), Error(200!P200), Error(300!P300), Error(400!P400)]");
    }


    private void testRQ1(NodeFactory base, 
            TransOptions opts,
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, List<Integer> expect) {
        testRQ1(base, opts, providerFactory, range, expect, "[]");
    }

    private void testRQ1(NodeFactory base, 
            TransOptions opts,
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, List<Integer> expect, String expectedErr) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testRQ1");
        init();
        RQAdapter<Integer> nodeProvider = providerFactory.apply(null);
        createAndInsert(factory, 5, nodeProvider);

        Collection<Range<Integer>> ranges = Collections.singleton(range);
        List<RemoteValue<Integer>> results = new ArrayList<>();
        RQAdapter<Integer> provider = providerFactory.apply((ret) -> {
            logger.debug("GOT RESULT: " + ret);
            results.add(ret);
        });
        nodes[0].rangeQueryAsync(ranges, provider, opts); 
        EventExecutor.startSimulation(30000);
        assertTrue(!results.isEmpty());
        assertTrue(results.get(results.size() - 1 ) == null);
        List<?> rvals = results.stream()
                .filter(Objects::nonNull)
                .filter(rv -> rv.getException() == null)
                .map(rv -> rv.getValue())
                .sorted()
                .collect(Collectors.toList());
        List<?> evals = results.stream()
                .filter(Objects::nonNull)
                .filter(rv -> rv.getException() != null)
                .map(rv -> rv.getException().getMessage())
                .sorted()
                .collect(Collectors.toList());
        logger.debug("RVALS = " + rvals);
        logger.debug("EXCEPTIONS = " + evals);
        logger.debug("EXPECTED = " + expect);
        assertTrue(rvals.equals(expect));
        assertTrue(evals.toString().equals(expectedErr));
        checkConsistent(nodes);
        checkMemoryLeakage(nodes);
    }
    
    @Test
    public void testRQInsertionPointProvider() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT);
        testRQ2(new SuzakuNodeFactory(3), opts,
                (receiver) -> new InsertionPointAdapter(receiver),
                new Range<Integer>(150), "[N100!P100, N200!P200]");
    }

    private void testRQ2(NodeFactory base, TransOptions opts,
            Function<Consumer<RemoteValue<Node[]>>, RQAdapter<Node[]>> providerFactory,
            Range<Integer> range, String expect) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testRQ2");
        init();
        RQAdapter<Node[]> baseProvider = providerFactory.apply(null);
        createAndInsert(factory, 5, baseProvider);
        {
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Node[]>> results = new ArrayList<>();
            RQAdapter<Node[]> provider = providerFactory.apply(
                    ret -> {
                        logger.debug("GOT RESULT: " + ret);
                        results.add(ret);
                    });

            nodes[0].rangeQueryAsync(ranges, provider, opts); 
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
            logger.debug("RVALS = " + rstr);
            logger.debug("EXPECT = " + expect);
            assertTrue(rstr.toString().equals(expect));
            checkMemoryLeakage(nodes);
        }
    }

    @Test
    public void testRetransDirectNoneSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT, RetransMode.NONE);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
    }

    @Test
    public void testRetransDirectNoneAckSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT, RetransMode.NONE_ACK);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
    }

    @Test
    public void testRetransDirectSlowSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT, RetransMode.SLOW);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
    }

    @Test
    public void testRetransAggregateSlowSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE, RetransMode.SLOW)
                .timeout(15*1000);
        testRetrans(new SuzakuNodeFactory(3), opts, 
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(100, true, 400, true),
                Arrays.asList(100, 300, 400));
    }

    @Test
    public void testRetransAggregateFastSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE, RetransMode.FAST)
        .timeout(15*1000);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(100, true, 400, true),
                Arrays.asList(100, 300, 400));
    }

    @Test
    public void testCache() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testRetrans(new SuzakuNodeFactory(3), opts, 
                receiver -> new SlowCacheValueProvider(receiver, 10000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
        logger.debug(getPrivateField(EventExecutor.class, "timeq").toString());
    }

    private void testRetrans(NodeFactory base,
            TransOptions opts, 
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, List<Integer> expect) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testSlowRetrans");
        init();
        RQAdapter<Integer> baseProvider = providerFactory.apply(null);
        createAndInsert(factory, 5, baseProvider);
        {
            nodes[2].fail();
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Integer>> results = new ArrayList<>();
            RQAdapter<Integer> p = providerFactory.apply(ret -> {
                logger.debug("GOT RESULT: " + ret);
                results.add(ret);
                
            });
            nodes[0].rangeQueryAsync(ranges, p, opts); 
            // should be larger than the default TransOptions timeout. 
            EventExecutor.startSimulation(60000);
            assertTrue(!results.isEmpty());
            assertTrue(results.get(results.size() - 1 ) == null);
            List<?> rvals = results.stream()
                    .filter(Objects::nonNull)
                    .map(rv -> rv.getValue())   // extract Integer
                    .sorted()
                    .collect(Collectors.toList());
            logger.debug("RVALS = " + rvals);
            logger.debug("EXPECT = " + expect);
            assertTrue(rvals.equals(expect));
            checkMemoryLeakage(nodes[0], nodes[1], nodes[3], nodes[4]);
        }
    }

    @Test
    public void testMultikeySuzaku() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testMultikey(new SuzakuNodeFactory(3), opts,
                receiver -> new KeyAdapter(receiver),
                k -> {
                    if (k == 300 || k == 400) {
                        return "P100";
                    } else {
                        return "P" + k;
                    }
                },
                new Range<Integer>(0, true, 500, false),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    @Test
    public void testMultikeySuzakuOnOneNode() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testMultikey(new SuzakuNodeFactory(3), opts,
                receiver -> new KeyAdapter(receiver),
                k -> {
                    return "P100";
                },
                new Range<Integer>(0, true, 500, false),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    private void testMultikey(NodeFactory base, TransOptions opts,
            Function<Consumer<RemoteValue<DdllKey>>, RQAdapter<DdllKey>> providerFactory,
            Function<Integer, String> mapper,
            Range<Integer> range, List<Integer> expect) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testMultikey");
        init();
        createAndInsert(factory, 5, mapper);
        {
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<DdllKey>> results = new ArrayList<>();
            RQAdapter<DdllKey> p = providerFactory.apply(ret -> {
                logger.debug("GOT RESULT: " + ret);
                results.add(ret);
            });
            nodes[0].rangeQueryAsync(ranges, p, opts);
            EventExecutor.startSimulation(30000);
            assertTrue(!results.isEmpty());
            assertTrue(results.get(results.size() - 1 ) == null);
            List<?> rvals = results.stream()
                    .filter(Objects::nonNull)
                    .map(rv -> rv.getValue())   // extract DdllKey
                    .map(rv -> rv.getRawKey()) // extract Comparable
                    .sorted()
                    .collect(Collectors.toList());
            logger.debug("RVALS = " + rvals);
            logger.debug("EXPECT = " + expect);
            assertTrue(rvals.equals(expect));
            checkMemoryLeakage(nodes[0], nodes[1], nodes[3], nodes[4]);
        }
    }
}

