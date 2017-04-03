/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestAggSG.java 823 2013-08-25 00:41:53Z yos $
 */

package test.chordsharp;

import org.piax.common.PeerLocator;
import org.piax.gtrans.ov.ring.RingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingInsertionPara extends
        RingExpBase<RingManager<PeerLocator>> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RingInsertionPara.class);

    RingInsertionPara(int nodes) {
        super(nodes);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 0 && args.length != 1) {
            System.err.println("usage: <cmd> numNode");
            System.err.println(" ex. <cmd> 100");
            System.exit(-1);
        }
        
        int nNodes = 10;
        if (args.length == 1) {
            nNodes = Integer.parseInt(args[0]);
        }

        RingInsertionPara exp = new RingInsertionPara(nNodes);
        exp.locatorType = "emu";
        exp.test1(nNodes);
        //exp.test2(nNodes);
        exp.finAll();

        System.out.printf("%n** End of Simulation.%n");
    }

    private void test1(int n) throws Exception {
        init(n);
        insertAllInParallel();
        System.out.println("##END");
        dump();
        //outputTopology(new File("insert.dot"));
    }

    private void test2(int n) throws Exception {
        init(n);
        insertAllSequentiallyRandomIntroducer();
        System.out.println("##END");
        dump();
        //outputTopology(new File("insert.dot"));
    }
}
