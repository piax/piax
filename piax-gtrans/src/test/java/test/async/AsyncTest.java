package test.async;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.async.EventException.TimeoutException;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.ov.async.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.rq.RQValueProvider;
import org.piax.gtrans.ov.async.rq.RQValueProvider.InsertionPointProvider;
import org.piax.gtrans.ov.async.rq.RQValueProvider.KeyProvider;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.gtrans.ov.ddll.DdllKey;

public class AsyncTest extends AsyncTestBase {
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
}
