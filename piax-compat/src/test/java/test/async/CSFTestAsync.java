package test.async;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.NetworkParams;
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
        TransOptions opts = new TransOptions(response, retrans);
        testCSFPatterns(new DdllNodeFactory(), opts,
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(0, true, 500, true));
    }

    @Test
    public void testCSFNoResponse() {
        TransOptions opts = new TransOptions(ResponseType.NO_RESPONSE);
        testCSFPatterns(new DdllNodeFactory(), opts, 
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(200, true, 400, false));
    }

    @Test
    public void testCSFAggregateSlowProvider() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testCSFPatterns(new DdllNodeFactory(), opts,
                (receiver, csf) -> new SlowCSFProvider(receiver, csf, 3000),
                new Range<Integer>(200, true, 400, false));
    }

    @Test
    public void testCSFTimeout() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT).timeout(10000);
        testCSFPatterns(new DdllNodeFactory(), opts,
                (receiver, csf) -> new SlowCSFProvider(receiver, csf, 20000),
                new Range<Integer>(200, true, 400, false));
    }
    
    @Test
    public void testCSFAggregateSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.AGGREGATE);
        testCSFPatterns(new SuzakuNodeFactory(3), opts,
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(200, true, 400, false));
    }

    @Test
    public void testCSFDirectSuzaku() {
        TransOptions opts = new TransOptions(ResponseType.DIRECT);
        testCSFPatterns(new SuzakuNodeFactory(3), opts,
                (receiver, csf) -> new FastCSFProvider(receiver, csf),
                new Range<Integer>(200, true, 300, false));
    }
    
    private void testCSFPatterns(NodeFactory base, 
            TransOptions opts,
            BiFunction<Consumer<RemoteValue<Integer>>, CSFContent, RQAdapter<Integer>> providerFactory,
            Range<Integer> range) {
    		// store and forward
		testCSF(base, opts, providerFactory, range, new CSFContent(30L, null), new CSFContent(null, 10L));
		// immediately forward
		testCSF(base, opts, providerFactory, range, new CSFContent(10L, null), new CSFContent(null, 20L));
		// timeout
		testCSF(base, opts, providerFactory, range, new CSFContent(10L, null), null);
    }

    private void testCSF(NodeFactory base, 
            TransOptions opts,
            BiFunction<Consumer<RemoteValue<Integer>>, CSFContent, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, CSFContent c1, CSFContent c2) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testCSF");
        logger.debug("ResponseType: {}", opts.getResponseType());
        logger.debug("RetransMode: {}", opts.getRetransMode());
        logger.debug("Deadline of stored message: {}", c1 != null? c1.deadline: "null");
        logger.debug("Period of relay message: {}", c2 != null? c2.period: "null");
        init();
        RQAdapter<Integer> nodeProvider = providerFactory.apply(null, null);
        createAndInsert(factory, 4, nodeProvider);
        Collection<Range<Integer>> ranges = Collections.singleton(range);
        List<RemoteValue<Integer>> results = new ArrayList<>();
        for (int i = 0; i < nodes.length; i++) {
    			nodes[i].setCSFHook(new CSFHook<Integer>("H" + i * 100, nodes[i]) {
    				public boolean setupRQ(RQRequest<Integer> req) {
    					if (!super.setupRQ(req))
    						return false;
    					assertTrue(req.adapter instanceof CSFProvider);
    					CSFProvider provider = (CSFProvider)req.adapter;
    					if (provider.getCSF().deadline != null) {
    						req.deadline = EventExecutor.getVTime() + NetworkParams.toVTime(provider.getCSF().deadline * 1000);
    					}
    					if (provider.getCSF().period != null) {
        					req.period = NetworkParams.toVTime(provider.getCSF().period * 1000);
    					}
    					return true;
    				}
    			});
        }
        RQAdapter<Integer> providerDeadline = providerFactory.apply((ret) -> {
            logger.debug("GOT RESULT: " + ret);
            results.add(ret);
        }, c1);
        nodes[0].rangeQueryAsync(ranges, providerDeadline, opts); 
        EventExecutor.startSimulation(3000);
        nodes[0].rangeQueryAsync(ranges, providerDeadline, opts); 
        EventExecutor.startSimulation(3000);
        if (c2 != null) {
        		RQAdapter<Integer> providerPeriod = providerFactory.apply((ret) -> {
        			logger.debug("GOT RESULT: " + ret);
        			results.add(ret);
        		}, c2);
        		nodes[1].rangeQueryAsync(ranges,  providerPeriod, opts);
        }
        EventExecutor.startSimulation(50000);
    }
}

