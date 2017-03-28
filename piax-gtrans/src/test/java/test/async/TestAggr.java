package test.async;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.ov.async.ddll.DdllStrategy;
import org.piax.gtrans.ov.async.rq.RQAggregateFlavor;
import org.piax.gtrans.ov.async.rq.RQFlavor;
import org.piax.gtrans.ov.async.rq.RQRange;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;

public class TestAggr extends AsyncTestBase {
    @Test
    public void testAggrQuery1() {
        SuzakuStrategy.UPDATE_FINGER_PERIOD.set(10*1000);
        DdllStrategy.pingPeriod.set(0);
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new SuzakuNodeFactory(3), opts,
                receiver -> new AggrProvider(receiver),
                new Range<Integer>(100, true, 400, false),
                Arrays.asList(1110), "[]");
    }

    @Test
    public void testAggrQuery2() {
        SuzakuStrategy.UPDATE_FINGER_PERIOD.set(10*1000);
        DdllStrategy.pingPeriod.set(0);
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testRQ1(new SuzakuNodeFactory(3), opts,
                receiver -> new AggrProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(11111), "[]");
    }

    private void testRQ1(NodeFactory base, 
            TransOptions opts, 
            Function<Consumer<RemoteValue<Integer>>, RQFlavor<Integer>> flavorFactory,
            Range<Integer> range, List<Integer> expect, String expectedErr) {
        NodeFactory factory = new RQNodeFactory(base);
        System.out.println("** testRQ1");
        init();
        RQFlavor<Integer> flavor = flavorFactory.apply(null);
        System.out.println("flavor=" + flavor);
        createAndInsert(factory, 5, flavor, null, 10*60*1000);
        {
            List<RemoteValue<Integer>> results = new ArrayList<>();
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            RQFlavor<Integer> f = flavorFactory.apply(ret -> {
                System.out.println("GOT RESULT: " + ret);
                results.add(ret);
            });
            nodes[0].rangeQueryAsync(ranges, f, opts);
            EventExecutor.startSimulation(30000);
            dump(nodes);
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
            checkMemoryLeakage(nodes);
        }
    }

    public static class AggrProvider extends RQAggregateFlavor<Integer> {
        public AggrProvider(Consumer<RemoteValue<Integer>> resultReceiver) {
            super(resultReceiver);
        }

        @Override
        public CompletableFuture<Integer> get(RQFlavor<Integer> received,
                DdllKey key) {
            //  key     value
            //    0  ->     1
            //  100  ->    10
            //  200  ->   100
            //  300  ->  1000
            int k = (int)(key.getPrimaryKey()) / 100;
            k = (int)Math.pow(10, k);
            return CompletableFuture.completedFuture(k);
        }
        @Override
        public boolean match(RQRange queryRange, DdllKeyRange range,
                Integer val) {
            return queryRange.contains(range);
        }
        @Override
        public Integer reduce(Integer a, Integer b) {
            System.out.println("REDUCE: " + a + " and " + b);
            return a + b;
        }
    }
}
