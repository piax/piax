package test.async;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import org.junit.Test;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.gtrans.ov.szk.Suzaku;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManyNodeChallenge extends AsyncTestBase {
    private static final Logger logger = LoggerFactory
            .getLogger(ManyNodeChallenge.class);
    @Test
    public void testManyNodes() {
        int num = 10000;
        testMany(new SuzakuNodeFactory(3), num);
    }
    
    public void testMany(NodeFactory base, int num) { 
        init();
        logger.debug("** testMany");
        NodeFactory factory = new RQNodeFactory(base);
        nodes = createNodes(factory, num);
        // search: 100 * 2 hop = 200msec
        // DDLL join: SetR + SetRAck = 200msec
        // 合計 400msec / 1 node
        insertAll(400 * num + 60*1000);

        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        MemoryUsage usage = mbean.getHeapMemoryUsage();
        int mb = 1024 * 1024;
        logger.debug("Used：     %8d MB%n", usage.getUsed() / mb);
        logger.debug("Committed: %8d MB%n", usage.getCommitted() / mb);
        logger.debug("Max:       %8d MB%n", usage.getMax() / mb);
    }
}