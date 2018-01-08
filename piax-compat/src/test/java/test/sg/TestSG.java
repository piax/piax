/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestSG.java 1195 2015-06-08 04:00:15Z teranisi $
 */

package test.sg;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.subspace.Range;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.impl.ReceiverThreadPool;
import org.piax.gtrans.ov.sg.SkipGraph;
import org.piax.gtrans.ov.sg.UnavailableException;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 以下のテストを行う。 1. 整数をkeyとして持つnodeをnumNodeの数だけ立ち上げる。 2. すべてのnodeをinsertする。 3.
 * nodes[seed]を除き、すべてのnodeをdeleteする。 4. 半分の数のnodeをinsertする。 5.
 * 4.でinsertしたnodeをdeleteし、それ以外のnodeをinsertする。 6. 立ち上げたピアを終了させる。
 * 
 * 処理の実行は、Executors.newFixedThreadPool(n) を使って同時に行う。 n
 * は使用するスレッド数で、numNodeと一致させておく。
 * 
 * @author Mikio Yoshida
 * @version 1.0.0
 */
public class TestSG {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(TestSG.class);

    static final int RETRY = 30;

    static Random rand = new MersenneTwister();
    // static Random rand = new MersenneTwister(123);
    static ExecutorService threadPool;
    static int numNode;
    static SkipGraph<PeerLocator>[] nodes;
    static Peer[] peers;
    static int GRAIN;

    static int next() {
        int r = rand.nextInt(numNode);
        return r;
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

    static void waitForExecFin() {
        System.out.println("====================WAITING=====================");
        threadPool.shutdown();
        try {
            // 最大2分待つ
            if (!threadPool.awaitTermination(120000, TimeUnit.MILLISECONDS)) {
                logger.warn("some tasks not terminated");
            }
        } catch (InterruptedException ignore) {
        }
        // setLが完了するまでの待ち時間
        waitForRecFin();
        System.out.println("==============WAITING FINISHED===============");
    }

    static void waitForRecFin() {
        // 最大30秒待つ
        for (int i = 0; i < 300; i++) {
            sleep(100);
            int n = ReceiverThreadPool.getActiveCount();
            if (n == 0) {
                return;
            }
        }
        logger.warn("some receiverss not terminated");
    }

    static enum TType {
        INSERT, DELETE, FIN, ADD, OFF, ON
    }

    static String locatorType;
    static PeerLocator seedLoc;
    static int FACTOR = 1;

    static enum Status {
        OUT, INSERTING, IN, DELETING, FAIL
    }

    static Status[] status;

    static void testFunc(SkipGraph<PeerLocator> sg, TType ttype, int n) {
        switch (ttype) {
        case INSERT:
            try {
                if (!sg.addKey(seedLoc, n)) {
                    System.out.printf(" [%d] insert failed *** %n", n);
                } else {
                    status[n] = Status.IN;
                    System.out.printf(" [%d] insert %n", n);
                }
            } catch (IllegalStateException e) {
            } catch (IOException e) {
                System.out.println("got " + e);
            } catch (UnavailableException e) {
                System.out.println("got " + e);
            }
            break;
        case DELETE:
            try {
                if (!sg.removeKey(n)) {
                    System.out.printf(" [%d] delete failed *** %n", n);
                } else {
                    System.out.printf(" [%d] delete %n", n);
                    status[n] = Status.OUT;
                }
            } catch (IllegalStateException e) {
            } catch (IOException e) {
                System.out.println("got " + e);
            }
            break;
        case FIN:
            sg.fin();
            if (n % GRAIN == 0)
                System.out.printf(" [%d] fin %n", n);
            break;
        case OFF:
            sg.offline();
            System.out.printf(" [%d] offline %n", n);
            break;
        case ON:
            sg.online();
            System.out.printf(" [%d] online %n", n);
            break;
        default:
            break;
        }
    }

    static void execSeq(SkipGraph<PeerLocator> sg, final TType ttype, final int n) {
        testFunc(sg, ttype, n);
    }

    static void exec(final SkipGraph<PeerLocator> sg, final TType ttype, final int n) {
        threadPool.execute(new Runnable() {
            public void run() {
                testFunc(sg, ttype, n);
            }
        });
    }

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

        locatorType = "emu";

        test1();
        // test2();
        // test3();
        // testRQ();
        // testInstantFix();

        for (int i = 0; i < nodes.length; i++) {
            peers[i].fin();
        }

        System.out.printf("%n** End of Simulation.%n");
        // System.exit(0);
    }

    private static void init(int numNode, int factor) throws Exception {
        TestSG.numNode = numNode;
        // 安定している唯一のノード
        TestSG.seedNo = 0;// next();
        seedLoc = newLocator(locatorType, seedNo);

        TestSG.numThread = numNode; // 同時に起動するスレッド数
        TestSG.FACTOR = factor;
        TestSG.nkeys = numNode * FACTOR;
        TestSG.numThread = nkeys;
        status = new Status[nkeys];
        Arrays.fill(status, Status.OUT);

        GRAIN = 10; // 出力の表示間隔

        System.out.printf("** Simulation start.%n");
        System.out.printf(" - num of nodes: %d%n", numNode);
        System.out.printf(" - seed node: %d%n", seedNo);
        System.out.printf(" - transport: %s%n", locatorType);
        nodes = new SkipGraph[numNode];
        peers = new Peer[numNode];

        // NodeArray4Test.nodes = nodes;

        // new
        for (int i = 0; i < nodes.length; i++) {
            peers[i] = Peer.getInstance(PeerId.newId());
            PeerLocator loc = newLocator(locatorType, i);
            nodes[i] = new SkipGraph<PeerLocator>(
                    peers[i].newBaseChannelTransport(loc), null);
        }

        // insert the seed node
        nodes[seedNo].addKey(seedLoc, seedNo);
        // nodes[seedNo].offline();
    }

    /**
     * シーケンシャル insert/delete のテスト
     */
    private static void test1() throws Exception {
        init(30, 1);

        // insert sequentially
        System.out.println("*** insert all");
        for (int i = 0; i < nodes.length; i++) {
            if (i != seedNo) {
                System.out.println("* inserting " + i);
                try {
                    if (i % 2 == 0) {
                        nodes[i].addKey(seedLoc, "STRING"
                                + (i / 3));
                    } else {
                        nodes[i].addKey(seedLoc, i / 3);
                    }
                } catch (IOException e) {
                    System.err.println("got " + e);
                }
            }
        }
        System.out.println("*** insert finished");
        dump();
        // System.exit(0);

        // range query
        System.out.println("*** range query");
        // nodes[1].offline();
        try {
            long start = System.currentTimeMillis();
            Collection<RemoteValue<?>> rset = nodes[0].forwardQuery(Collections
                    .<Range<?>> singletonList(new Range<Integer>(
                            0, 10)), null);
            long end = System.currentTimeMillis();
            System.out
                    .println("*** range query took " + (end - start) + "msec");
            System.out.println("*** range query result: ");
            for (RemoteValue<?> o : rset) {
                System.out.println(">> " + o);
            }
            System.out.println("*** range query result ends");
        } catch (Exception e) {
            e.printStackTrace();
            // System.out.println(e);
        }
        ;
        // System.exit(0);
        System.out.println("*** lessthan query (LOWER)");
        // nodes[7].offline();
        try {
            long start = System.currentTimeMillis();
            Collection<RemoteValue<?>> rset = nodes[29].forwardQuery(
                    false, new Range<Integer>(0, false, 10, false), 50, null);
            long end = System.currentTimeMillis();
            System.out.println("*** lessthan query took " + (end - start)
                    + "msec");
            System.out.println("*** lessthan result: ");
            for (RemoteValue<?> o : rset) {
                System.out.println(">> " + o);
            }
            System.out.println("*** lessthan result ends");
        } catch (Exception e) {
            System.out.println(e);
        }
        ;
        System.out.println("*** lessthan query (FLOOR)");
        try {
            Collection<RemoteValue<?>> rset = nodes[0].forwardQuery(
                    false, new Range<String>("A", "STRING5"), 50, null);
            System.out.println("*** lessthan result: ");
            for (RemoteValue<?> o : rset) {
                System.out.println(">> " + o);
            }
            System.out.println("*** lessthan result ends");
        } catch (Exception e) {
            System.out.println(e);
        }
        ;

        // System.exit(0);

        // System.out.println("*** fail");
        // nodes[1].offline();
        // nodes[2].offline();
        /*
         * for (int i = 0; i < 10; i++) { try { Thread.sleep(2000); } catch
         * (InterruptedException e) {} dump(); }
         */
        /*
         * System.out.println("*** find"); Link ip = nodes[0].find(null, 3, 10);
         * System.out.println(ip);
         * 
         * System.out.println("*** find2"); Link ip2 = nodes[3].find(null, 0,
         * 10); System.out.println(ip2);
         * 
         * System.out.println("*** revive"); nodes[1].online();
         * nodes[2].online(); try { Thread.sleep(10000); } catch
         * (InterruptedException e) {} dump();
         */
        // 全部delete
        System.out.println("*** delete half");
        for (int i = 0; i < nodes.length; i++) {
            try {
                if (i % 2 == 0) {
                    nodes[i].removeKey("STRING" + (i / 5));
                } else {
                    nodes[i].removeKey(i / 5);
                }
            } catch (IOException e) {
                System.err.println("got " + e);
            }
        }
        System.out.println("*** delete finished");
        dump();

        System.out.println("*** fin");
        for (int i = 0; i < nodes.length; i++) {
            nodes[i].getTransport().fin();
            nodes[i].fin();
        }
        System.out.println("*** fin finished");
    }

    /**
     * 同一ピア複数ノードのテスト
     */
    @SuppressWarnings("unused")
    private static void test2() throws Exception {
        init(30, 1);

        // 全部insert
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** insert all **%n");
        for (int i = 0; i < nkeys; i++) {
            if (i == seedNo)
                continue;
            exec(nodes[key2node(i)], TType.INSERT, i);
        }
        waitForExecFin();

        System.out.println("*** insert finished");
        dump();
        // System.exit(0);

        /*
         * System.out.println("*** make node1 offline"); nodes[1].offline();
         * //nodes[2].offline(); for (int i = 0; i < 10; i++) { try {
         * Thread.sleep(2000); } catch (InterruptedException e) {}
         * System.out.println("*** i=" + i); if (i == 3) {
         * System.out.println("*** node1 online"); nodes[1].online(); } dump();
         * }
         */

        // System.exit(0);
        /*
         * System.out.println("*** find"); try { Link ip = nodes[0].find(null,
         * 3, 10); System.out.println("*** find result " + ip); } catch
         * (Exception e) { System.out.println(e); };
         * 
         * System.out.println("*** find2"); try { Link ip2 = nodes[3].find(null,
         * 0, 10); System.out.println("*** find result " + ip2); } catch
         * (Exception e) { System.out.println(e); }
         */

        // range query
        System.out.println("*** range query");
        nodes[1].offline();
        try {
            Collection<RemoteValue<?>> rset = nodes[2].forwardQuery(Collections
                    .<Range<?>> singletonList(new Range<Integer>(
                            1, 7)), null);
            System.out.println("*** range query result: ");
            for (RemoteValue<?> o : rset) {
                System.out.println(">> " + o);
            }
            System.out.println("*** range query result ends");
        } catch (Exception e) {
            e.printStackTrace();
            // System.out.println(e);
        }
        ;
        System.exit(0);
        System.out.println("*** lessthan query");
        try {
            Collection<RemoteValue<?>> rset = nodes[1].forwardQuery(
                    false, new Range<Integer>(0, false, 10, false), 4, null);
            System.out.println("*** lessthan result: ");
            for (RemoteValue<?> o : rset) {
                System.out.println(">> " + o);
            }
            System.out.println("*** lessthan result ends");
        } catch (Exception e) {
            System.out.println(e);
        }
        ;
        // System.exit(0);

        // System.out.println("*** revive");
        // nodes[1].online();
        // nodes[2].online();
        /*
         * try { Thread.sleep(10000); } catch (InterruptedException e) {}
         * dump();
         */

        // 全部delete
        // nodes[1].offline();
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** delete all **%n");
        for (int i = 0; i < nkeys; i++) {
            if (i == seedNo)
                continue;
            exec(nodes[key2node(i)], TType.DELETE, i);
        }
        waitForExecFin();
        dump();
        // System.exit(0);

        /*
         * System.out.println("*** delete half"); for (int i = 0; i <
         * nodes.length; i+= 2) { nodes[i].removeKey(new
         * WrappedComparableKey<Integer>(i)); }
         */

        // 半分insert
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** insert half **%n");
        for (int i = 0; i < nkeys; i++) {
            if (i == seedNo)
                continue;
            if (i % 4 < 2)
                exec(nodes[key2node(i)], TType.INSERT, i);
        }
        waitForExecFin();
        dump();

        System.out.printf("%n** wait for stabilization **%n");
        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
            System.out.println("*** i=" + i);
            dump();
        }

        System.exit(0);

        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** delete half and insert half %n");
        for (int i = 0; i < nkeys; i++) {
            if (i == seedNo)
                continue;
            if (i % 4 < 2)
                exec(nodes[key2node(i)], TType.DELETE, i);
            else
                exec(nodes[key2node(i)], TType.INSERT, i);
        }
        waitForExecFin();

        System.out.println("*** finished");
        dump();
    }

    private static int key2node(int key) {
        // return key % numNode;
        return key / FACTOR;
    }

    /**
     * ランダムに挿入・削除を繰り返すテスト
     */
    @SuppressWarnings("unused")
    private static void test3() throws Exception {
        init(30, 3);

        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** insert/delete randomly **%n");
        for (int k = 0; k < 100; k++) {
            int i = (int) (Math.random() * nkeys);
            if (i == seedNo)
                continue;
            switch (status[i]) {
            case OUT:
                status[i] = Status.INSERTING;
                exec(nodes[key2node(i)], TType.INSERT, i);
                break;
            case IN:
                status[i] = Status.DELETING;
                exec(nodes[key2node(i)], TType.DELETE, i);
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            ;
            dump();
        }
        System.out.println("*** finished");
        dump();
    }

    /**
     * range query test
     */
    private static void testRQ() throws Exception {
        init(10, 1);

        // insert sequentially
        System.out.println("*** insert all");
        for (int i = 0; i < nkeys; i++) {
            if (i != seedNo) {
                System.out.println("* inserting " + i);
                try {
                    nodes[i % numNode].addKey(seedLoc, i);
                } catch (IOException e) {
                    System.err.println("got " + e);
                    e.printStackTrace();
                }
            }
        }
        System.out.println("*** insert finished");
        dump();
        // System.exit(0);

        // range query
        // nodes[3].offline();
        // nodes[5].offline();

        // nodes[7].offline();
        // System.out.println("** offline " + nodes[7].toString());
        try {
            long start = System.currentTimeMillis();
            List<Range<?>> ranges = new ArrayList<Range<?>>();
            // ranges.add(new Range<Comparable<?>>(-1, true, 3, true));
            ranges.add(new Range<Integer>(3, false,
                    20, false));
            System.out.println("*** range query: " + ranges);
            FutureQueue<?> fq = nodes[0].scalableRangeQuery(ranges, null,
                    new TransOptions(30000));
            long end = System.currentTimeMillis();
            System.out
                    .println("*** range query took " + (end - start) + "msec");
            System.out.println("*** range query result: ");

            for (RemoteValue<?> rv : fq) {
                System.out.println(">> " + rv);
                System.out.println("*** took "
                        + (System.currentTimeMillis() - start) + "msec");
            }

            // while (fq.hasNext()) {
            // Object value;
            // try {
            // value = fq.getNext();
            // // } catch (InterruptedException e) {
            // // // 返り値の収拾時にタイムアウトが発生した場合
            // // break;
            // } catch (NoSuchElementException e) {
            // // cancel 等で、登録されていた返り値がなくなった場合
            // System.out.println("+++ NoSuchElementException thrown");
            // System.out.println("*** took " + (System.currentTimeMillis()
            // -start) + "msec");
            // break;
            // } catch (InvocationTargetException e) {
            // // メソッド呼び出し先での例外を取得した場合
            // System.out.println("err>> " + e.getCause());
            // continue;
            // }
            // System.out.printf(">> %s from peer:%s%n", value,
            // fq.getThisPeerId());
            // System.out.println("*** took " + (System.currentTimeMillis()
            // -start) + "msec");
            // }
            System.out.println("*** range query result ends");
            System.out.println("*** took "
                    + (System.currentTimeMillis() - start) + "msec");
        } catch (Exception e) {
            e.printStackTrace();
        }
        ;

        dump();
        // System.exit(0);
        System.out.println("*** lessthan query (LOWER)");

        // nodes[7].offline();
        try {
            long start = System.currentTimeMillis();
            Collection<RemoteValue<?>> rset = nodes[29].forwardQuery(
                    false, new Range<Integer>(0, false, 10, false), 50, null);
            long end = System.currentTimeMillis();
            System.out.println("*** lessthan query took " + (end - start)
                    + "msec");
            System.out.println("*** lessthan result: ");
            for (RemoteValue<?> o : rset) {
                System.out.println(">> " + o);
            }
            System.out.println("*** lessthan result ends");
        } catch (Exception e) {
            System.out.println(e);
        }
        ;
        System.out.println("*** lessthan query (FLOOR)");
        try {
            Collection<RemoteValue<?>> rset = nodes[0].forwardQuery(
                    false, new Range<String>("A", "STRING5"), 50, null);
            System.out.println("*** lessthan result: ");
            for (RemoteValue<?> o : rset) {
                System.out.println(">> " + o);
            }
            System.out.println("*** lessthan result ends");
        } catch (Exception e) {
            System.out.println(e);
        }
        ;

        // System.exit(0);

        // System.out.println("*** fail");
        // nodes[1].offline();
        // nodes[2].offline();
        /*
         * for (int i = 0; i < 10; i++) { try { Thread.sleep(2000); } catch
         * (InterruptedException e) {} dump(); }
         */
        /*
         * System.out.println("*** find"); Link ip = nodes[0].find(null, 3, 10);
         * System.out.println(ip);
         * 
         * System.out.println("*** find2"); Link ip2 = nodes[3].find(null, 0,
         * 10); System.out.println(ip2);
         * 
         * System.out.println("*** revive"); nodes[1].online();
         * nodes[2].online(); try { Thread.sleep(10000); } catch
         * (InterruptedException e) {} dump();
         */
        // 全部delete
        System.out.println("*** delete half");
        for (int i = 0; i < nodes.length; i++) {
            try {
                if (i % 2 == 0) {
                    nodes[i].removeKey("STRING" + (i / 5));
                } else {
                    nodes[i].removeKey(i / 5);
                }
            } catch (IOException e) {
                System.err.println("got " + e);
            }
        }
        System.out.println("*** delete finished");
        dump();
    }

    // private static List<Object> executeRangeQuery(int rootNode,
    // final List<Range<?>> ranges) {
    // final long start = System.currentTimeMillis();
    // final ReturnSet<Object> rset
    // = nodes[rootNode].scalableRangeQuery(null, ranges, null, null,
    // 500, true);
    // long end = System.currentTimeMillis();
    // System.out.println("*** range query took " + (end-start) + "msec");
    // final List<Object> results = new ArrayList<Object>();
    // System.out.println("*** range query result: ");
    // while (rset.hasNext()) {
    // Object value;
    // try {
    // value = rset.getNext();
    // // } catch (InterruptedException e) {
    // // // 返り値の収拾時にタイムアウトが発生した場合
    // // break;
    // } catch (NoSuchElementException e) {
    // // cancel 等で、登録されていた返り値がなくなった場合
    // System.out.println("+++ NoSuchElementException thrown");
    // System.out.println("*** took " + (System.currentTimeMillis() -start) +
    // "msec");
    // break;
    // } catch (InvocationTargetException e) {
    // // メソッド呼び出し先での例外を取得した場合
    // System.out.println("err>> " + e.getCause());
    // continue;
    // }
    // System.out.printf(">> %s from peer:%s%n", value, rset.getThisPeerId());
    // System.out.println("*** took " + (System.currentTimeMillis() -start) +
    // "msec");
    // results.add(value);
    // }
    // System.out.println("*** range query result ends (1): "
    // + ranges + " -> " + results);
    // return results;
    // }

    /**
     * instantFix test
     */
    @SuppressWarnings("unused")
    private static void testInstantFix() throws Exception {
        init(6, 1);

        // insert sequentially
        System.out.println("*** insert all");
        for (int i = 0; i < nkeys; i++) {
            if (i != seedNo) {
                System.out.println("* inserting " + i);
                try {
                    nodes[i % numNode].addKey(seedLoc, i);
                } catch (IOException e) {
                    System.err.println("got " + e);
                }
            }
        }
        System.out.println("*** insert finished");
        dump();

        System.out.println("*** offline");
        nodes[1].offline();
        // nodes[2].offline();

        /*
         * System.out.println("*** instantFix"); int origin = 2; SGNode sgnode =
         * nodes[origin].getSGNode(origin); Tile t = sgnode.getTile(1); Link
         * failed = t.node.getLeft();
         * nodes[(Integer)(failed.key.mainKey)].offline();
         * System.out.println("* node " + t.node.getLeft().key + " failed");
         * nodes[origin].instantFix(origin, t.node.getLeft(), FixMode.ROOT);
         */
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
        }
        dump();

        System.out.println("*** recover");
        nodes[1].online();
        nodes[2].online();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
        }
        System.out.println("*** end");
        dump();
        System.exit(0);
    }

    static void dump() {
        for (int i = 0; i < nodes.length; i++) {
            System.out.println("Node " + i);
            System.out.println(nodes[i]);
        }
    }
}
