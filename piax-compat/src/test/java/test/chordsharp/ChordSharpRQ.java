/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestAggSG.java 823 2013-08-25 00:41:53Z yos $
 */

package test.chordsharp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.ov.ring.rq.MessagePath;
import org.piax.gtrans.ov.ring.rq.RQResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChordSharpRQ extends ChordSharpExpBase {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ChordSharpRQ.class);

    ChordSharpRQ(int nodes) {
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

        ChordSharpRQ exp = new ChordSharpRQ(nNodes);
        exp.locatorType = "emu";
        exp.testRQ(nNodes);
        //exp.testRQdeletion(nNodes);
        //exp.testRQMulti(nNodes);
        //exp.testRQfailure(nNodes);
        exp.finAll();

        System.out.printf("%n** End of Simulation.%n");
    }

    private void testRQ(int n) throws Exception {
        init(n);
        insertAllSequentially();
        System.out.println("##insertion done");

        int l = (int) Math.ceil(Math.log(n) / Math.log(2));
        sleep(l * 5000 + 1000);
        dump();

        rq(0, new Range<Integer>(2, true, 15, true));

        //sleep(30000);

        System.out.println("##END");
    }

    private void testRQMulti(int n) throws Exception {
        init(n);
        insertAllSequentially();
        insertAllSequentially(1);
        System.out.println("##insertion done");

        int l = (int) Math.ceil(Math.log(n) / Math.log(2));
        //sleep(l * 5000 + 1000);
        sleep(5000);
        dump();

        rq(0, new Range<Integer>(2, true, 180, true));

        //sleep(30000);

        System.out.println("##END");
    }

    private void testRQdeletion(int n) throws Exception {
        init(n);
        insertAllSequentially();
        System.out.println("##insertion done");

        int l = (int) Math.ceil(Math.log(n) / Math.log(2));
        sleep(l * 5000 + 1000);
        dump();

        deleteNode(4);
        rq(0, new Range<Integer>(2, true, 15, true));

        //sleep(30000);

        System.out.println("##END");
    }

    private void testRQfailure(int n) throws Exception {
        init(n);
        insertAllSequentially();
        int l = (int) Math.ceil(Math.log(n) / Math.log(2));
        sleep(l * 5000 + 1000);
        System.out.println("##insertion done");
        dump();

        int failNode = 13;
        failNode(failNode);
        rq(0, new Range<Integer>(2, true, 15, true));

        sleep(l * 5000 + 1000);

        System.out.println("##END");
    }

    private void rq(int startNode, Range<?> range) {
        try {
            long start = System.currentTimeMillis();
            List<Range<?>> ranges = new ArrayList<Range<?>>();
            ranges.add(range);
            System.out.println("*** range query: " + ranges);
            TransOptions opts = new TransOptions(10000, ResponseType.DIRECT, true);
            RQResults<?> ret =
                    nodes.get(startNode).scalableRangeQueryPro(ranges, null, opts);
            long end = System.currentTimeMillis();
            System.out
                    .println("*** range query took " + (end - start) + "msec");
            System.out.println("*** range query result: ");

            int i = 0;
            for (RemoteValue<?> rv : ret.getFutureQueue()) {
                System.out.println("*** RQ[" + i + "]: " + rv + ", took "
                        + (System.currentTimeMillis() - start) + "msec");
                i++;
            }

            Collection<MessagePath> paths = ret.getMessagePaths();
            System.out.println("*** paths");
            for (MessagePath path : paths) {
                System.out.println(path);
            }
            System.out.println("*** range query result ends");
            System.out.println("*** took "
                    + (System.currentTimeMillis() - start) + "msec");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void test2(int n) throws Exception {
        init(n);
        insertAllSequentiallyRandomIntroducer();
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

}
