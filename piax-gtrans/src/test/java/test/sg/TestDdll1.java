/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestDdll1.java 1256 2015-08-02 13:44:03Z teranisi $
 */

package test.sg;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.ReceiverThreadPool;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ddll.Node.InsertionResult;
import org.piax.gtrans.ov.ddll.NodeArray4Test;
import org.piax.gtrans.ov.ddll.NodeManager;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 以下のテストを行う。
 * 1. 整数をkeyとして持つnodeをnumNodeの数だけ立ち上げる。
 * 2. すべてのnodeをinsertする。
 * 3. nodes[seed]を除き、すべてのnodeをdeleteする。
 * 4. 半分の数のnodeをinsertする。
 * 5. 4.でinsertしたnodeをdeleteし、それ以外のnodeをinsertする。
 * 6. 立ち上げたピアを終了させる。
 * 
 * 処理の実行は、Executors.newFixedThreadPool(n) を使って同時に行う。
 * n は使用するスレッド数で、numNodeと一致させておく。
 * 
 * @author     Mikio Yoshida
 * @version    1.0.0
 */
public class TestDdll1 {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(TestDdll1.class);

    static final int RETRY = 30;

    static Random rand = new MersenneTwister();
//    static Random rand = new MersenneTwister(123);
    static ExecutorService threadPool;
    static int numNode;
    static NodeManager[] managers;
    static Node[] nodes;
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
    static Link seedLoc;
    static void testFunc(TType ttype, int n) {
        switch(ttype) {
        case INSERT:
            try {
                InsertionResult insres = nodes[n].insert(seedLoc, RETRY);
                if (!insres.success) {
                    System.out.printf(" [%d] insert failed *** %n", n);
                } else {
                    System.out.printf(" [%d] insert %n", n);
                    NodeArray4Test.dump(n, false);
                }
            } catch (IllegalStateException e) {
            }
            break;
        case DELETE:
            try {
                if (!nodes[n].delete(RETRY)) {
                    System.out.printf(" [%d] delete failed *** %n", n);
                } else {
                    System.out.printf(" [%d] delete %n", n);
                    NodeArray4Test.dump(n, false);
                }
            } catch (IllegalStateException e) {
            }
            break;
        case FIN:
            Transport<?> trans = managers[n].getTransport();
            managers[n].fin();
            trans.fin();
            if (true || n % GRAIN == 0)
                System.out.printf(" [%d] fin %n", n);
            break;
        case OFF:
            managers[n].offline();
            System.out.printf(" [%d] offline %n", n);
            break;
        case ON:
            managers[n].online();
            System.out.printf(" [%d] online %n", n);
            break;
        default:
            break;
        }
    }

    static void execSeq(final TType ttype, final int n) {
        testFunc(ttype, n);
    }
    
    static void exec(final TType ttype, final int n) {
        threadPool.execute(
                new Runnable() {
                    public void run() {
                        Thread.currentThread().setName("Peer" + n);
                        testFunc(ttype, n);
                    }
                });
    }

    static PeerLocator newLocator(String locatorType, int vport) {
        PeerLocator peerLocator;
        if (locatorType.equals("emu")) {
            peerLocator = new EmuLocator(vport);
        } else if (locatorType.equals("udp")) {
            peerLocator = new UdpLocator(
                    new InetSocketAddress("localhost", 10000 + vport));
        } else {
            peerLocator = new TcpLocator(
                    new InetSocketAddress("localhost", 10000 + vport));
        }
        return peerLocator;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 0 && args.length != 1) {
            System.err.println("usage: <cmd> numNode");
            System.err.println(" ex. <cmd> 100");
            System.exit(-1);
        }
        
        locatorType = "emu";
        numNode = 10;
        if (args.length == 1) {
            numNode = Integer.parseInt(args[0]);
        }
        // 安定している唯一のノード
        int seedNo = next();
        int numThread = numNode;    // 同時に起動するスレッド数
        GRAIN = 10;      // 出力の表示間隔
        
        System.out.printf("** Simulation start.%n");
        System.out.printf(" - num of nodes: %d%n", numNode);
        System.out.printf(" - seed node: %d%n", seedNo);
        System.out.printf(" - transport: %s%n", locatorType);
        managers = new NodeManager[numNode];
        nodes = new Node[numNode];
        peers = new Peer[numNode];

        // new
        for (int i = 0; i < nodes.length; i++) {
            peers[i] = Peer.getInstance(new PeerId("P" + i));
            PeerLocator loc = newLocator(locatorType, i);
            managers[i] = new NodeManager(
                    peers[i].newBaseChannelTransport(loc));
            nodes[i] = managers[i].createNode(i, "");
            nodes[i].setCheckPeriod(1000);
        }
        NodeArray4Test.nodes = nodes;

        // seedNo番目のnodeはinitial nodeとしてinsertし、安定稼働させる
        System.out.println("node " + seedNo + " is inserted as the initial node");
        nodes[seedNo].insertAsInitialNode();
        seedLoc = nodes[seedNo].getMyLink();
        // 全部insert
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.println("** insert all start");
        for (int i = 0; i < nodes.length - 1; i++) {
            if (i == seedNo) continue;
            exec(TType.INSERT, i);
            //nodes[i].insert(seedLoc, 10);
        }
        waitForExecFin();
        System.out.println("** insert all finished");
        NodeArray4Test.dump(false);
        mustBeConsistent();

        System.out.println("** insert the last node");
        threadPool = Executors.newFixedThreadPool(1);
        exec(TType.INSERT, nodes.length - 1);
        waitForExecFin();
        System.out.println("** insert the last node finished");
        NodeArray4Test.dump(false);
        mustBeConsistent();

        if (true) {
            System.out.println("** recovery test start");
            threadPool = Executors.newFixedThreadPool(numThread);
            int fnode = (seedNo + 1) % numNode;
            exec(TType.OFF, fnode);
            //exec(TType.OFF, (seedNo + 2) % numNode);

            sleep(1000);
            // wait for recovery done 
            while (!NodeArray4Test.dump(true)) {
                sleep(1000);
            } 
            System.out.println("** recovery test finished");
            NodeArray4Test.dump(false);

            if (true) {
                // offline にしたノードをオンラインに戻すテスト．
                /*
                 * numNode = 2 の場合，次の sleep 中にオフラインノードは左ノードを
                 * 自ノードに設定する． 
                 */
                System.out.println("** sleep 5000");
                sleep(5000);
                NodeArray4Test.dump(false);
                System.out.println("** re-online node " + (seedNo + 1) % numNode);
                exec(TType.ON, fnode);
                /*
                 * オンラインに戻すとオフラインだったノードは孤立し，連結リストが2つに分裂する．
                 */
                // wait for recovery done 
                while (!NodeArray4Test.dump(true)) {
                    sleep(1000);
                }
            }
            waitForExecFin();
            NodeArray4Test.dump(false);
        }
        
        // 全部delete
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.println("** delete all start");
        for (int i = 0; i < nodes.length; i++) {
            if (i == seedNo) continue;
            exec(TType.DELETE, i);
        }
        waitForExecFin();
        System.out.println("** sleep 10sec");
        sleep(10000);
        System.out.println("** delete all finished");
        mustBeConsistent();
        NodeArray4Test.dump(false);
        
        // 半分insert
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** insert half **%n");
        for (int i = 0; i < nodes.length; i++) {
            if (i == seedNo) continue;
            if (i % 4 < 2)
                exec(TType.INSERT, i);
        }
        waitForExecFin();
        System.out.println("** insert half finished");
        mustBeConsistent();
        NodeArray4Test.dump(false);

        // 半分delete、同時に半分insert
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.println("** delete half and insert half start");
        for (int i = 0; i < nodes.length; i++) {
            if (i == seedNo) continue;
            if (i % 4 < 2)
                exec(TType.DELETE, i);
            else
                exec(TType.INSERT, i);
        }
        waitForExecFin();
        mustBeConsistent();
        NodeArray4Test.dump(false);

        // fin
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** fin **%n");
        for (int i = 0; i < nodes.length; i++) {
            exec(TType.FIN, i);
        }
        waitForExecFin();
        mustBeConsistent();

        for (int i = 0; i < nodes.length; i++) {
            peers[i].fin();
        }
        System.out.printf("%n** End of Simulation.%n");
    }
    
    static void mustBeConsistent() {
        if (!NodeArray4Test.dump(true)) {
            System.out.println("found inconsistency");
            System.exit(1);
        }
    }
}
