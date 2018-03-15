package test.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.piax.ayame.Event;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.ov.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQStrategy.RQNodeFactory;
import org.piax.ayame.ov.rq.csf.CSFHook;
import org.piax.ayame.ov.rq.csf.RQBundledRequest;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public class TestAsyncCSF extends AsyncTestBase {
    private static final Logger logger = LoggerFactory
            .getLogger(TestAsyncCSF.class);
    
    @Test
    public void testCSFAggregateNone() {
        testCSFWith(ResponseType.AGGREGATE, RetransMode.NONE);
    }

    @Test
    public void testCSFAggregateNoneAck() {
        testCSFWith(ResponseType.AGGREGATE, RetransMode.NONE_ACK);
    }

    @Test
    public void testCSFAggregateReliable() {
        testCSFWith(ResponseType.AGGREGATE, RetransMode.RELIABLE);
    }

    @Test
    public void testCSFAggregateFast() {
        testCSFWith(ResponseType.AGGREGATE, RetransMode.FAST);
    }

    @Test
    public void testCSFDirectNone() {
        testCSFWith(ResponseType.DIRECT, RetransMode.NONE);
    }

    @Test
    public void testCSFDirectNoneAck() {
        testCSFWith(ResponseType.DIRECT, RetransMode.NONE_ACK);
    }

    @Test
    public void testCSFDirectReliable() {
        testCSFWith(ResponseType.DIRECT, RetransMode.RELIABLE);
    }

    @Test
    public void testCSFDirectFast() {
        testCSFWith(ResponseType.DIRECT, RetransMode.FAST);
    }

    private void testCSFWith(ResponseType response, RetransMode retrans) {
        TransOptions opts = new TransOptions(response, retrans);
        testCSFPatterns(new DdllNodeFactory(), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true));
    }

    @Test
    public void testCSFNoResponse() {
        TransOptions opts = new TransOptions(ResponseType.NO_RESPONSE);
        testCSFPatterns(new DdllNodeFactory(), opts, 
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false));
    }

    @Test
    public void testCSFAggregateSlowProvider() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testCSFPatterns(new DdllNodeFactory(), opts,
                (receiver) -> new SlowValueProvider(receiver, 3000),
                new Range<Integer>(200, true, 400, false));
    }

    @Test
    public void testCSFTimeout() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT).timeout(10000);
        testCSFPatterns(new DdllNodeFactory(), opts,
                (receiver) -> new SlowValueProvider(receiver, 20000),
                new Range<Integer>(200, true, 400, false));
    }

    @Test
    public void testCSFAggregateSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testCSFPatterns(new SuzakuNodeFactory(3), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 400, false));
    }

    @Test
    public void testCSFDirectSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT);
        testCSFPatterns(new SuzakuNodeFactory(3), opts,
                (receiver) -> new FastValueProvider(receiver),
                new Range<Integer>(200, true, 300, false));
    }

    private void testCSFPatterns(NodeFactory base, 
            TransOptions opts,
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> providerFactory,
            Range<Integer> range) {
        // store and forward
        testCSF(base, new TransOptions(opts).extraTime(30L), new TransOptions(opts).period(10L), providerFactory, range);
        // immediately forward
        testCSF(base, new TransOptions(opts).extraTime(10L), new TransOptions(opts).period(20L), providerFactory, range);
        // timeout
        testCSF(base, new TransOptions(opts).extraTime(10L), opts, providerFactory, range);
    }

    private void testCSF(NodeFactory base, 
            TransOptions opts1,
            TransOptions opts2,
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> providerFactory,
            Range<Integer> range) {
        //TestLoggerFactory.getInstance().setPrintLevel(Level.DEBUG);
        TestLogger loggerCSFHook = TestLoggerFactory.getTestLogger(CSFHook.class);
        TestLogger loggerRQMultiRequest = TestLoggerFactory.getTestLogger(RQBundledRequest.class);
        TestLogger loggerEvent = TestLoggerFactory.getTestLogger(Event.class);

        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testCSF");
        logger.debug("ResponseType: {}", opts1.getResponseType());
        logger.debug("RetransMode: {}", opts1.getRetransMode());
        logger.debug("Deadline of stored message: {}", opts1.getExtraTime());
        logger.debug("Period of relay message: {}", opts2.getPeriod());
        try {
            init();
            RQAdapter<Integer> nodeProvider = providerFactory.apply(null);
            createAndInsert(factory, 4, nodeProvider);
            Collection<Range<Integer>> ranges = Collections.singleton(range);
            List<RemoteValue<Integer>> results = new ArrayList<>();
            for (int i = 0; i < nodes.length; i++) {
                nodes[i].setCSFHook(new CSFHook<Integer>("H" + i * 100, nodes[i]));
            }
            RQAdapter<Integer> providerDeadline = providerFactory.apply((ret) -> {
                logger.debug("GOT RESULT: " + ret);
                results.add(ret);
            });
            nodes[0].rangeQueryAsync(ranges, providerDeadline, opts1); 
            EventExecutor.startSimulation(3000);
            nodes[0].rangeQueryAsync(ranges, providerDeadline, opts1); 
            EventExecutor.startSimulation(3000);
            if (opts2.getPeriod() != null) {
                RQAdapter<Integer> providerPeriod = providerFactory.apply((ret) -> {
                    logger.debug("GOT RESULT: " + ret);
                    results.add(ret);
                });
                nodes[1].rangeQueryAsync(ranges,  providerPeriod, opts2);
            }
            EventExecutor.startSimulation(50000);
            long mergeCount = loggerCSFHook.getLoggingEvents().stream()
                    .filter(e -> e.getMessage().toLowerCase().contains("merged"))
                    .count();
            long runCount =  loggerRQMultiRequest.getLoggingEvents().stream()
                    .filter(e -> e.getMessage().toLowerCase().contains("run bundled"))
                    .count();
            long ackTimeoutCount = loggerEvent.getLoggingEvents().stream()
                    .filter(e -> e.getMessage().toLowerCase().contains("AckTimeoutExceptionrun".toLowerCase()))
                    .count();
            assertEquals(mergeCount, runCount);
            assertEquals(ackTimeoutCount, 0);
            System.out.println(mergeCount + " messages merged.");
        } finally {
            TestLoggerFactory.clear();
        }
    }
}

