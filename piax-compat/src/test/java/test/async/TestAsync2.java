package test.async;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.FTEntry;
import org.piax.ayame.Indirect;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.ov.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQStrategy.RQNodeFactory;
import org.piax.ayame.ov.suzaku.SuzakuStrategy;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAsync2 extends AsyncTestBase {
    private static final Logger logger = LoggerFactory
            .getLogger(TestAsync2.class);
    @Test
    public void testSuzakuBase4() {
        SuzakuStrategy.B.set(2);
        assertTrue(SuzakuStrategy.K == 4);
        SuzakuStrategy.USE_SUCCESSOR_LIST.set(true);
        SuzakuStrategy.SUCCESSOR_LIST_SIZE = 4;
        SuzakuStrategy.UPDATE_FINGER_PERIOD.set(60*1000);
        testBasicInsDel(new SuzakuNodeFactory(3));
    }

    private void testBasicInsDel(NodeFactory factory) {
        logger.debug("** testBasicInsDel");
        init(0);
        nodes = createNodes(factory, 16);
        insertAll(20*60*1000);
        dump(nodes);
        for (int i = 0; i < nodes.length; i++) {
            checkFingerTable(i);
        }
    }
    
    @Test
    public void testForwardQueryLeft1() {
        // normal case
        TransOptions opts = new TransOptions();
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(200, 300, 400), "[]", -1);
    }

    @Test
    public void testForwardQueryLeft1NoResponse() {
        // normal case
        TransOptions opts = new TransOptions(ResponseType.NO_RESPONSE);
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(), "[]", -1);
    }

    @Test
    public void testForwardQueryLeft2() {
        // less than NUM case
        TransOptions opts = new TransOptions();
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(100, true, 200, true),
                Arrays.asList(100, 200), "[]", -1);
    }

    @Test
    public void testForwardQueryLeft3() {
        // wrap around range case
        TransOptions opts = new TransOptions();
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(true, 400, true, 100, true),
                Arrays.asList(0, 100, 400), "[]", -1);
    }
    
    @Test
    public void testForwardQueryLeft4() {
        // no result case
        TransOptions opts = new TransOptions();
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(true, 150, true, 160, true),
                Arrays.asList(), "[]", -1);
    }

    @Test
    public void testForwardQueryLeftFail1() {
        // middle node failure
        TransOptions opts = new TransOptions();
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(100, 200, 400), "[]", 3);
    }

    @Test
    public void testForwardQueryLeftFail2() {
        // right-most node failure
        TransOptions opts = new TransOptions(RetransMode.RELIABLE);
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(100, 200, 300), "[]", 4);
    }

    @Test
    public void testForwardQueryLeftFail3() {
        // right-most node failure
        TransOptions opts = new TransOptions(RetransMode.SLOW)
        .timeout(2000);
        testFQLeft(new DdllNodeFactory(), opts,
                receiver -> new FastValueProvider(receiver),
                new Range<Integer>(0, true, 500, true),
                Arrays.asList(100, 200, 300), "[]", 4);
    }

    private void testFQLeft(NodeFactory base, 
            TransOptions opts,
            Function<Consumer<RemoteValue<Integer>>, RQAdapter<Integer>> providerFactory,
            Range<Integer> range, List<Integer> expect, String expectedErr,
            int failNode) {
        NodeFactory factory = new RQNodeFactory(base);
        logger.debug("** testFQLeft");
        init();
        Indirect<Long> startTime = new Indirect<>(EventExecutor.getVTime());
        Indirect<Long> endTime = new Indirect<>();
        List<RemoteValue<Integer>> results = new ArrayList<>();
        RQAdapter<Integer> provider = providerFactory
                .apply((RemoteValue<Integer> ret) -> {
                    logger.debug("GOT RESULT: " + ret);
                    results.add(ret);
                    if (ret == null) {
                        endTime.val = EventExecutor.getVTime();
                    }
                });
        createAndInsert(factory, 5, provider, () -> {
            if (failNode >= 0) {
                nodes[failNode].fail();
            }
            nodes[0].forwardQueryLeftAsync(range, 3, provider, opts);

        });
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
        logger.debug("Elapsed = " + (endTime.val - startTime.val));
        assertTrue(rvals.equals(expect));
        assertTrue(evals.toString().equals(expectedErr));
        IntStream.range(0, nodes.length)
        .filter(i -> i != failNode)
        .forEach(i -> checkMemoryLeakage(nodes[i]));
    }

    void checkFingerTable(int index) {
        LocalNode n = nodes[index];
        SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(n);
        logger.debug("checking: " + n);
        int s = szk.getFingerTableSize();
        for (int i = 0; i < s; i++) {
            FTEntry ent = szk.getFingerTableEntry(i); 
            int d = distance(i);
            Node shouldbe = nodes[(index + d) % nodes.length];
            assertTrue(ent.getNode() == shouldbe);
        }
    }

    int distance(int index) {
        int base = 1 << (SuzakuStrategy.B.value() * (index / (SuzakuStrategy.K - 1))); 
        return base * (1 + index % (SuzakuStrategy.K - 1)); 
    }
}
