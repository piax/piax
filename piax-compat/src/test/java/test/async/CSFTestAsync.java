package test.async;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.ov.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQRequest;
import org.piax.ayame.ov.rq.RQStrategy.RQNodeFactory;
import org.piax.ayame.ov.rq.csf.CSFHook;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSFTestAsync extends AsyncTestBase {
    private static final Logger logger = LoggerFactory
            .getLogger(CSFTestAsync.class);
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

    public static interface CSFProvider {
    		public CSFContent getCSF();
    }

    public static class FastCSFProvider extends FastValueProvider implements CSFProvider {
        public CSFContent csf;

        public FastCSFProvider(Consumer<RemoteValue<Integer>> resultsReceiver, CSFContent csf) {
            super(resultsReceiver);
            this.csf = csf;
        }
        
        public CSFContent getCSF() {
        		return csf;
        }
    }

    public static class SlowCSFProvider extends SlowValueProvider implements CSFProvider {
        public CSFContent csf;

        public SlowCSFProvider(Consumer<RemoteValue<Integer>> resultsReceiver, CSFContent csf, int delay) {
            super(resultsReceiver, delay);
            this.csf = csf;
        }
        public CSFContent getCSF() {
    			return csf;
        }
    }

    public static class ErrorCSFProvider extends ErrorProvider implements CSFProvider {
        public CSFContent csf;

        public ErrorCSFProvider(Consumer<RemoteValue<Integer>> resultsReceiver, CSFContent csf) {
            super(resultsReceiver);
            this.csf = csf;
        }
        public CSFContent getCSF() {
        		return csf;
        }
    }

    private void testCSFWith(ResponseType response, RetransMode retrans) {
        TransOptions opts = new TransOptions();
        opts.setResponseType(response);
        opts.setRetransMode(retrans);
        testCSF(new DdllNodeFactory(), opts,
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(0, 100, 200, 300, 400));
    }

    @Test
    public void testCSFNoResponse() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.NO_RESPONSE);
        testCSF(new DdllNodeFactory(), opts, 
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testCSFAggregateSlowProvider() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testCSF(new DdllNodeFactory(), opts,
                (receiver, csf) -> new SlowCSFProvider(receiver, csf, 3000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testCSFTimeout() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        opts.setTimeout(10000);
        testCSF(new DdllNodeFactory(), opts,
                (receiver, csf) -> new SlowCSFProvider(receiver, csf, 20000),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList());
    }

    @Test
    public void testCSFAggregateSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.AGGREGATE);
        testCSF(new SuzakuNodeFactory(3), opts,
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(200, true, 400, false),
                Arrays.asList(200, 300));
    }

    @Test
    public void testCSFDirectSuzaku() {
        TransOptions opts = new TransOptions();
        opts.setResponseType(ResponseType.DIRECT);
        testCSF(new SuzakuNodeFactory(3), opts,
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(200, true, 300, false),
                Arrays.asList(400));
    }
    
    private void testCSF(NodeFactory base, 
            TransOptions opts,
            BiFunction<Consumer<RemoteValue<Integer>>, CSFContent, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, List<Integer> expect) {
        testCSF(base, opts, providerFactory, range, expect, "[]");
    }

    private void testCSF(NodeFactory base, 
            TransOptions opts,
            BiFunction<Consumer<RemoteValue<Integer>>, CSFContent, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, List<Integer> expect, String expectedErr) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testCSF");
        logger.debug("ResponseType: {}", opts.getResponseType());
        logger.debug("RetransMode: {}", opts.getRetransMode());
        init();
        RQAdapter<Integer> nodeProvider = providerFactory.apply(null, null);
        createAndInsert(factory, 4, nodeProvider);
        Collection<Range<Integer>> ranges = Collections.singleton(range);
        List<RemoteValue<Integer>> results = new ArrayList<>();
        for (int i = 0; i < nodes.length; i++) {
    			nodes[i].setCSFHook(new CSFHook<Integer>("H" + i * 100) {
    				public void setupRQ(RQRequest<Integer> req) {
    					assertTrue(req.adapter instanceof CSFProvider);
    					req.topic = ((CSFProvider)req.adapter).getCSF().topic;
    					req.deadline = ((CSFProvider)req.adapter).getCSF().deadline;
    					req.period = ((CSFProvider)req.adapter).getCSF().period;
    				}
    			});
        }
        RQAdapter<Integer> providerDeadline = providerFactory.apply((ret) -> {
            logger.debug("GOT RESULT: " + ret);
            results.add(ret);
        }, new CSFContent("topic", EventExecutor.getVTime() + 30 * 1000, null));
        nodes[0].rangeQueryAsync(ranges, providerDeadline, opts); 
        EventExecutor.startSimulation(3000);
        //nodes[0].rangeQueryAsync(ranges, providerDeadline, opts); 
        RQAdapter<Integer> providerPeriod = providerFactory.apply((ret) -> {
            logger.debug("GOT RESULT: " + ret);
            results.add(ret);
        }, new CSFContent("topic", null, (long)20 * 1000));
        nodes[1].rangeQueryAsync(ranges,  providerPeriod, opts);
        EventExecutor.startSimulation(50000);
    }
}

