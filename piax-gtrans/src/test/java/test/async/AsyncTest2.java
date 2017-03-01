package test.async;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.ov.async.suzaku.FTEntry;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;

public class AsyncTest2 extends AsyncTestBase {
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
        System.out.println("** testBasicInsDel");
        init(0);
        nodes = createNodes(factory, 16);
        insertAll(20*60*1000);
        dump(nodes);
        for (int i = 0; i < nodes.length; i++) {
            checkFingerTable(i);
        }
    }
    
    void checkFingerTable(int index) {
        LocalNode n = nodes[index];
        SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(n);
        System.out.println("checking: " + n);
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
