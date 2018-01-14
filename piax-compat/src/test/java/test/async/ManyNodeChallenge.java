package test.async;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import org.piax.ayame.NodeFactory;
import org.piax.ayame.ov.rq.RQStrategy.RQNodeFactory;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManyNodeChallenge extends AsyncTestBase {
    private static final Logger logger = LoggerFactory
            .getLogger(ManyNodeChallenge.class);

    public static void main(String[] args) {
        int num = 1000;
        new ManyNodeChallenge().testMany(new SuzakuNodeFactory(3), num);
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
        System.gc();
        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        MemoryUsage usage = mbean.getHeapMemoryUsage();
        int mb = 1024 * 1024;
        System.out.printf("Used：     %8d MB%n", usage.getUsed() / mb);
        System.out.printf("Committed: %8d MB%n", usage.getCommitted() / mb);
        System.out.printf("Max:       %8d MB%n", usage.getMax() / mb);
    }
}