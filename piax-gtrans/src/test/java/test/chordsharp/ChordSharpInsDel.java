/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestAggSG.java 823 2013-08-25 00:41:53Z yos $
 */

package test.chordsharp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChordSharpInsDel extends ChordSharpExpBase {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ChordSharpInsDel.class);

    ChordSharpInsDel(int nodes) {
        super(nodes);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 0 && args.length != 1) {
            System.err.println("usage: <cmd> numNode");
            System.err.println(" ex. <cmd> 100");
            System.exit(-1);
        }

        int nNodes = 2;
        if (args.length == 1) {
            nNodes = Integer.parseInt(args[0]);
        }

        ChordSharpInsDel exp = new ChordSharpInsDel(nNodes);
        exp.locatorType = "emu";
        exp.test1(nNodes);
        //exp.test15(nNodes);
        //exp.test2(nNodes);
        //exp.test3(nNodes);
        //exp.test4(nNodes);
        //exp.insertPara1(nNodes);
        //exp.testInsertMultiKey(nNodes);
        //exp.testRecovery(20);
        exp.finAll();

        System.out.printf("%n** End of Simulation.%n");
    }

    private void test1(int n) throws Exception {
        init(n);
        insertAllSequentially();
        System.out.println("## inserted all nodes");
        dump();
        // T=5secの場合,
        // 30秒で6回更新．2^6=64ノード程度まで大丈夫 ?
        sleep(20000);
        dump();
        System.out.println("##END");
        //outputTopology(new File("insert.dot"));
    }

    private void test15(int n) throws Exception {
        init(n);
        insert(0, n - 1);
        System.out.println("## inserted n-1 nodes"); 
        sleep(15000);
        System.out.println("## inserting last node");
        insert(n - 1, n);
        dump();
        System.out.println("##END");
        //outputTopology(new File("insert.dot"));
    }

    private void test2(int n) throws Exception {
        init(n);
        insertAllSequentiallyRandomIntroducer();
        dump();
        sleep(30000);
        System.out.println("##END");
        //outputTopology(new File("insert.dot"));
    }
    
    private void test3(int n) throws Exception {
        init(n);
        insertAllSequentially();
        System.out.println("## inserted all nodes");
        sleep(20000);
        System.out.println("## deleting half nodes");
        for (int i = n / 2; i < nkeys; i++) {
            nodes.get(i).removeKey(i);
        }
        System.out.println("## deleting half nodes done");
        sleep(15000);
        System.out.println("##END");
    }
    
    private void test4(int n) throws Exception {
        init(n);
        for (int i = 0; i < nkeys - 1; i++) {
            insertNode(seedNo, i);
        }
        dump();
        sleep(10000);
        System.out.println("## insert the last");
        insertNode(nkeys / 2, nkeys - 1);
        dump();
        System.out.println("##END");
        //outputTopology(new File("insert.dot"));
    }

    private void testInsertMultiKey(int n) throws Exception {
        init(n);
        insertAllSequentially(0);
        insertAllSequentially(1);
        System.out.println("## inserted all nodes");
        dump();
        // T=5secの場合,
        // 30秒で6回更新．2^6=64ノード程度まで大丈夫 ?
        sleep(20000);
        dump();
        System.out.println("##END");
        //outputTopology(new File("insert.dot"));
    }

    private void insertPara1(int n) throws Exception {
        init(n);
        insertAllInParallel();
        dump();
        System.out.println("## inserted all nodes");
        System.out.println("##END");
    }
    
    private void testRecovery(int n) throws Exception {
        init(n);
        insertAllSequentially();
        int l = (int)Math.ceil(Math.log(n)/Math.log(2));
        sleep(l * 5000 + 1000);
        System.out.println("##insertion done");
        dump();

        int failNode = 13;
        System.out.println("*** offline node " + failNode);
        nodes.get(failNode).offline();

        sleep(l * 5000 + 1000);

        System.out.println("##END");
    }
}
