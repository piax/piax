package test.async;

import static org.junit.jupiter.api.Assertions.*;

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

import org.junit.jupiter.api.Test;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.LocalNode;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.ov.ddll.DdllKeyRange;
import org.piax.ayame.ov.ddll.DdllStrategy;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQAggregateAdapter;
import org.piax.ayame.ov.rq.RQConditionalAdapter;
import org.piax.ayame.ov.rq.RQStrategy.RQNodeFactory;
import org.piax.ayame.ov.suzaku.SuzakuStrategy;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.common.DdllKey;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAggr extends AsyncTestBase {
    private static final Logger logger = LoggerFactory
            .getLogger(TestAggr.class);
    @Test
    public void testAggrQuery1() {
        SuzakuStrategy.UPDATE_FINGER_PERIOD.set(10*1000);
        DdllStrategy.pingPeriod.set(0);
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new SuzakuNodeFactory(3), opts,
                receiver -> new AggrTestAdapter(receiver),
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
                receiver -> new AggrTestAdapter(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(11111), "[]");
    }

    @Test
    public void testCondQuery1() {
        SuzakuStrategy.UPDATE_FINGER_PERIOD.set(10*1000);
        DdllStrategy.pingPeriod.set(0);
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testRQ1(new SuzakuNodeFactory(3), opts,
                receiver -> new CondTestAdapter(receiver, 0b10101),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(0, 200, 400), "[]");
    }

    private void testRQ1(NodeFactory base, 
            TransOptions opts, 
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> adapterFactory,
            Range<Integer> range, List<Integer> expect, String expectedErr) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testRQ1");
        init();
        RQAdapter<Integer> adapter = adapterFactory.apply(null);
        logger.debug("adapter=" + adapter);
        createAndInsert(factory, 5, adapter, null, 10*60*1000);
        {
            List<RemoteValue<Integer>> results = new ArrayList<>();
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            RQAdapter<Integer> f = adapterFactory.apply(ret -> {
                logger.debug("GOT RESULT: " + ret);
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
            logger.debug("RVALS = " + rvals);
            logger.debug("EXCEPTIONS = " + evals);
            logger.debug("EXPECTED = " + expect);
            assertTrue(rvals.equals(expect));
            assertTrue(evals.toString().equals(expectedErr));
            checkMemoryLeakage(nodes);
        }
    }

    public static class AggrTestAdapter extends RQAggregateAdapter<Integer> {
        public AggrTestAdapter(Consumer<RemoteValue<Integer>> resultReceiver) {
            super(resultReceiver);
        }

        @Override
        public CompletableFuture<Integer> get(RQAdapter<Integer> received,
                DdllKey key) {
            //  key     value
            //    0  ->     1
            //  100  ->    10
            //  200  ->   100
            //  300  ->  1000
            int k = (Integer)(key.getRawKey()) / 100;
            k = (int)Math.pow(10, k);
            return CompletableFuture.completedFuture(k);
        }
        @Override
        public Integer reduce(Integer a, Integer b) {
            logger.debug("REDUCE: " + a + " and " + b);
            return a + b;
        }
    }

    public static class CondTestAdapter extends RQConditionalAdapter<Integer, Integer> {
        int bitmap;
        public CondTestAdapter(Consumer<RemoteValue<Integer>> resultReceiver, int bitmap) {
            super(resultReceiver);
            this.bitmap = bitmap;
        }

        @Override
        public CompletableFuture<Integer> get(RQAdapter<Integer> received,
                DdllKey key) {
            int k = (Integer)key.getRawKey();
            return CompletableFuture.completedFuture(k);
        }
        @Override
        public Integer getCollectedData(LocalNode localNode) {
            //  key     value (in binrary)
            //    0  ->     1
            //  100  ->    10
            //  200  ->   100
            //  300  ->  1000
            int k = (Integer)(localNode.key.getRawKey()) / 100;
            return 1 << k;
        }
        @Override
        public boolean match(DdllKeyRange range, Integer val) {
            boolean rc = (this.bitmap & val) != 0;
            logger.debug("bitmap=" + bitmap
                    + ", val=" + val + ", range=" + range + ", rc=" + rc);
            return rc;
        }
        @Override
        public Integer reduce(Integer a, Integer b) {
            return a | b;
        }
    }
}
