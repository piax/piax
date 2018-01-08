/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestSG1.java 1195 2015-06-08 04:00:15Z teranisi $
 */

package test.sg;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.subspace.Range;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.ov.sg.SkipGraph;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.MersenneTwister;

public class TestSG1 {
    static final int RETRY = 30;

    static Random rand = new MersenneTwister(12);
    static ExecutorService threadPool;
    static int numNode;
    static SkipGraph<PeerLocator>[] nodes;
    static Peer[] peers;
    static int GRAIN;

    static int next() {
        int r = rand.nextInt(numNode);
        return r;
    }

    static int next(int n) {
        return rand.nextInt(n);
    }

    static void sleep(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException ignore) {
        }
    }

    static void waitForKeyin() {
        try {
            System.in.read();
            while (System.in.available() > 0) {
                System.in.read();
            }
        } catch (IOException ignore) {
        }
    }

    static List<?> toVals(List<RemoteValue<?>> rset) {
        List<Object> list = new ArrayList<Object>();
        for (RemoteValue<?> rv : rset) {
            try {
                if (rv == null) {
                    System.out.println("?? null RemoteValue inserted");
                    continue;
                }
                list.add(rv.get());
            } catch (InvocationTargetException e) {
                list.add(e);
            }
        }
        return list;
    }

    static String locatorType;
    static PeerLocator seedLoc;
    static int FACTOR = 1;

    static enum Status {
        OUT, INSERTING, IN, DELETING, FAIL
    }

    static Status[] status;

    static PeerLocator newLocator(String locatorType, int vport) {
        PeerLocator peerLocator;
        if (locatorType.equals("emu")) {
            peerLocator = new EmuLocator(vport);
        } else if (locatorType.equals("udp")) {
            peerLocator = new UdpLocator(new InetSocketAddress("localhost",
                    10000 + vport));
        } else {
            peerLocator = new TcpLocator(new InetSocketAddress("localhost",
                    10000 + vport));
        }
        return peerLocator;
    }

    static int seedNo;
    static int numThread;
    static int nkeys;

    public static void main(String[] args) throws Exception {
        if (args.length != 0 && args.length != 1) {
            System.err.println("usage: <cmd> numNode");
            System.err.println(" ex. <cmd> 100");
            System.exit(-1);
        }

        locatorType = "tcp";

        test1();

        for (int i = 0; i < nodes.length; i++) {
            peers[i].fin();
        }
        System.out.printf("%n** End of Simulation.%n");
//        System.exit(0);
    }

    private static void init(int numNode, int factor) throws Exception {
        TestSG1.numNode = numNode;
        // 安定している唯一のノード
        TestSG1.seedNo = 0;// next();
        seedLoc = newLocator(locatorType, seedNo);

        TestSG1.FACTOR = factor;
        TestSG1.nkeys = numNode * FACTOR;
        TestSG1.numThread = nkeys;
        status = new Status[nkeys];
        Arrays.fill(status, Status.OUT);

        GRAIN = 10; // 出力の表示間隔

        System.out.printf("** Simulation start.%n");
        System.out.printf(" - num of nodes: %d%n", numNode);
        System.out.printf(" - seed node: %d%n", seedNo);
        System.out.printf(" - transport: %s%n", locatorType);
        nodes = new SkipGraph[numNode];
        peers = new Peer[numNode];

        // new
        for (int i = 0; i < nodes.length; i++) {
            peers[i] = Peer.getInstance(PeerId.newId());
            PeerLocator loc = newLocator(locatorType, i);
            nodes[i] = new SkipGraph<PeerLocator>(
                    peers[i].newBaseChannelTransport(loc), null);
        }

        // insert the seed node
        nodes[seedNo].addKey(seedLoc, new Integer(seedNo));
    }

    private static void rangeQuery(int i, int from, int to) {
        Range<Integer> range = new Range<Integer>(
                new Integer(Math.min(from, to)),
                new Integer(Math.max(from, to)));
        FutureQueue<?> fq = nodes[i].scalableRangeQuery(
                Collections.<Range<?>> singletonList(range), null, 
                new TransOptions(3000));
        List<RemoteValue<?>> rset = new ArrayList<RemoteValue<?>>();
        for (RemoteValue<?> rv : fq) {
            rset.add(rv);
        }
        System.out.printf("*** range query %s from node %d: %s%n", range, i,
                toVals(rset));
    }

    private static void less(int i, Range<Integer> range, int num) {
        List<RemoteValue<?>> rset = nodes[i].forwardQuery(false, range, num, null);
        System.out.printf("*** less than %s, %d from node %d: %s%n", range, num,
                i, toVals(rset));
    }

    /**
     * シーケンシャル insert/delete のテスト
     */
    private static void test1() throws Exception {
        init(10, 1);

        // insert sequentially
        System.out.println("*** insert all");
        for (int i = 0; i < nodes.length; i++) {
            if (i != seedNo) {
                System.out.println("* inserting " + i);
                try {
                    nodes[i].addKey(seedLoc, new Integer(i));
                } catch (IOException e) {
                    System.err.println("got " + e);
                }
            }
        }
        System.out.println("*** insert finished");

        // range query
        System.out.println("\n*** range query");
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                rangeQuery(next(), next(), next());
            }
            long end = System.currentTimeMillis();
            System.out
                    .println("*** range query took " + (end - start) + "msec");
        } catch (Exception e) {
            System.err.println("got " + e);
        }

        System.out.println("\n*** range query (buggy case)");
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                rangeQuery(i, 0, 9);
            }
            long end = System.currentTimeMillis();
            System.out
                    .println("*** range query took " + (end - start) + "msec");
        } catch (Exception e) {
            System.err.println("got " + e);
        }

        System.out.println("\n*** less than query (LOWER)");
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                int n1 = next(), n2 = next();
                if (n1 == n2) n1 = n2 -1;
                Range<Integer> range = new Range<Integer>(Math.min(n1, n2), false,
                        Math.max(n1, n2), false);
                less(next(), range, next(5) + 1);
            }
            long end = System.currentTimeMillis();
            System.out.println("*** lessthan query took " + (end - start)
                    + "msec");
        } catch (Exception e) {
            System.err.println("got " + e);
        }

        System.out.println("\n*** less equal query (FLOOR)");
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                int n1 = next(), n2 = next();
                Range<Integer> range = new Range<Integer>(Math.min(n1, n2), Math.max(n1, n2));
                less(next(), range, next(5) + 1);
            }
            long end = System.currentTimeMillis();
            System.out.println("*** lessthan query took " + (end - start)
                    + "msec");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("got " + e);
        }

        // 全部delete
        System.out.println("\n*** delete");
        for (int i = 0; i < nodes.length; i++) {
            try {
                nodes[i].removeKey(new Integer(i));
            } catch (IOException e) {
                System.err.println("got " + e);
            }
        }
        System.out.println("*** delete finished");
    }
}
