package test.async;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import org.junit.Test;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;

public class ManyNodeChallenge extends AsyncTestBase {

    @Test
    public void testManyNodes() {
        int num = 10000;
        testMany(new SuzakuNodeFactory(3), num);
    }
    
    public void testMany(NodeFactory base, int num) { 
        init();
        Log.verbose = false;
        System.out.println("** testMany");
        NodeFactory factory = new RQNodeFactory(base);
        nodes = createNodes(factory, num);
        // search: 100 * 2 hop = 200msec
        // DDLL join: SetR + SetRAck = 200msec
        // 合計 400msec / 1 node
        insertAll(400 * num + 60*1000);

        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        MemoryUsage usage = mbean.getHeapMemoryUsage();
        int mb = 1024 * 1024;
        System.out.printf("Used：     %8d MB%n", usage.getUsed() / mb);
        System.out.printf("Committed: %8d MB%n", usage.getCommitted() / mb);
        System.out.printf("Max:       %8d MB%n", usage.getMax() / mb);
    }
}