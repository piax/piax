package test.async;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.async.EventException.NetEventException;
import org.piax.gtrans.async.EventException.TimeoutException;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.Indirect;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.ov.async.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.gtrans.ov.async.rq.RQAdapter;
import org.piax.gtrans.ov.async.rq.RQAdapter.InsertionPointAdapter;
import org.piax.gtrans.ov.async.rq.RQAdapter.KeyAdapter;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.gtrans.ov.ddll.DdllKey;

public class AsyncTest extends AsyncTestBase {
    @Test
    public void testTerminate1() {
        EventExecutor.reset();
        Indirect<Boolean> chk1 = new Indirect<>(false), chk2 = new Indirect<>(false);
        EventExecutor.sched(50, () -> {
            System.out.println("chk1: " + EventExecutor.getVTime());
            chk1.val = true;
        });
        EventExecutor.sched(150, () -> {
            System.out.println("chk2: "+ EventExecutor.getVTime());
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
        EventExecutor.sched(50, () -> {
            System.out.println("chk1: " + EventExecutor.getVTime());
            chk1.val = true;
        });
        EventExecutor.sched(1000, () -> {
            System.out.println("chk2: "+ EventExecutor.getVTime());
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

    // simple failure
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

    // leave and fail (3 nodes)
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
        System.out.println("** testFix3");
        init();
        nodes = createNodes(factory, 2);
        nodes[0].joinInitialNode();
        {
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
            Indirect<CompletableFuture<Boolean>> f2 = new Indirect<>();
            EventExecutor.sched(10000, () -> {
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
        System.out.println("** testDdllJoinFail1");
        init();
        nodes = createNodes(new DdllNodeFactory(), 2);
        nodes[0].joinInitialNode();
        nodes[0].fail();
        {
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
            f1.whenComplete((rc, exc) -> 
                System.out.println("rc=" + rc + ", exc=" + exc));
            EventExecutor.startSimulation(40000);
            dump(nodes);
            assertTrue(f1.isDone());
            try {
                f1.get();
                fail();
            } catch (InterruptedException | ExecutionException e) {
                assertTrue(e.getCause() instanceof TimeoutException
                        || e.getCause() instanceof NetEventException);
            }
        }
    }

    // join and fail
    @Test
    public void testDdllJoinFail2() {
        System.out.println("** testDdllJoinFail2");
        init();
        nodes = createNodes(new DdllNodeFactory(), 3);
        nodes[0].joinInitialNode();
        {
            Indirect<CompletableFuture<Boolean>> f2 = new Indirect<>();
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
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
        System.out.println("** testDdllJoinFail3");
        init();
        nodes = createNodes(new RQNodeFactory(new SuzakuNodeFactory(3)), 3);
        nodes[0].joinInitialNode();
        {
            Indirect<CompletableFuture<Boolean>> f2 = new Indirect<>();
            CompletableFuture<Boolean> f1 = nodes[1].joinAsync(nodes[0]);
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
        TransOptions opts = new TransOptions();
        opts.setResponseType(response);
        opts.setRetransMode(retrans);
        testRQ1(new DdllNodeFactory(), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    @Test
    public void testRQ1NoResponse() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.NO_RESPONSE);
        testRQ1(new DdllNodeFactory(), opts, 
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testRQ1AggregateSlowProvider() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new DdllNodeFactory(), opts,
                (receiver) -> new SlowValueProvider(receiver, 3000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testRQ1Timeout() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        opts.setTimeout(10000);
        testRQ1(new DdllNodeFactory(), opts,
                (receiver) -> new SlowValueProvider(receiver, 20000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testRQ1AggregateSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new SuzakuNodeFactory(3), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testRQ1DirectSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ1(new SuzakuNodeFactory(3), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }
    
    @Test
    public void testRQ1ExceptionInProvider() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
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
        System.out.println("** testRQ1");
        init();
        RQAdapter<Integer> nodeProvider = providerFactory.apply(null);
        createAndInsert(factory, 5, nodeProvider);

        Collection<Range<Integer>> ranges = Collections.singleton(range);
        List<RemoteValue<Integer>> results = new ArrayList<>();
        RQAdapter<Integer> provider = providerFactory.apply((ret) -> {
            System.out.println("GOT RESULT: " + ret);
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
        System.out.println("RVALS = " + rvals);
        System.out.println("EXCEPTIONS = " + evals);
        System.out.println("EXPECTED = " + expect);
        assertTrue(rvals.equals(expect));
        assertTrue(evals.toString().equals(expectedErr));
        checkConsistent(nodes);
        checkMemoryLeakage(nodes);
    }
    
    @Test
    public void testRQInsertionPointProvider() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ2(new SuzakuNodeFactory(3), opts,
                (receiver) -> new InsertionPointAdapter(receiver),
                new Range<Integer>(150), "[N100!P100, N200!P200]");
    }

    private void testRQ2(NodeFactory base, TransOptions opts,
            Function<Consumer<RemoteValue<Node[]>>, RQAdapter<Node[]>> providerFactory,
            Range<Integer> range, String expect) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testRQ2");
        init();
        RQAdapter<Node[]> baseProvider = providerFactory.apply(null);
        createAndInsert(factory, 5, baseProvider);
        {
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Node[]>> results = new ArrayList<>();
            RQAdapter<Node[]> provider = providerFactory.apply(
                    ret -> {
                        System.out.println("GOT RESULT: " + ret);
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
            System.out.println("RVALS = " + rstr);
            System.out.println("EXPECT = " + expect);
            assertTrue(rstr.toString().equals(expect));
            checkMemoryLeakage(nodes);
        }
    }

    @Test
    public void testRetransDirectNoneSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        opts.setRetransMode(RetransMode.NONE);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
    }

    @Test
    public void testRetransDirectNoneAckSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        opts.setRetransMode(RetransMode.NONE_ACK);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
    }

    @Test
    public void testRetransDirectSlowSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        opts.setRetransMode(RetransMode.SLOW);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
    }

    @Test
    public void testRetransAggregateSlowSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        opts.setRetransMode(RetransMode.SLOW);
        opts.setTimeout(15*1000);
        testRetrans(new SuzakuNodeFactory(3), opts, 
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(100, true, 400, true),
                Arrays.asList(100, 300, 400));
    }

    @Test
    public void testRetransAggregateFastSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        opts.setRetransMode(RetransMode.FAST);
        opts.setTimeout(15*1000);
        testRetrans(new SuzakuNodeFactory(3), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(100, true, 400, true),
                Arrays.asList(100, 300, 400));
    }

    @Test
    public void testCache() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRetrans(new SuzakuNodeFactory(3), opts, 
                receiver -> new SlowCacheValueProvider(receiver, 10000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(300));
        System.out.println(getPrivateField(EventExecutor.class, "timeq"));
    }

    private void testRetrans(NodeFactory base,
            TransOptions opts, 
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, List<Integer> expect) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testSlowRetrans");
        init();
        RQAdapter<Integer> baseProvider = providerFactory.apply(null);
        createAndInsert(factory, 5, baseProvider);
        {
            nodes[2].fail();
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Integer>> results = new ArrayList<>();
            RQAdapter<Integer> p = providerFactory.apply(ret -> {
                System.out.println("GOT RESULT: " + ret);
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
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
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
        System.out.println("** testMultikey");
        init();
        createAndInsert(factory, 5, mapper);
        {
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<DdllKey>> results = new ArrayList<>();
            RQAdapter<DdllKey> p = providerFactory.apply(ret -> {
                System.out.println("GOT RESULT: " + ret);
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
            System.out.println("RVALS = " + rvals);
            System.out.println("EXPECT = " + expect);
            assertTrue(rvals.equals(expect));
            checkMemoryLeakage(nodes[0], nodes[1], nodes[3], nodes[4]);
        }
    }
}

