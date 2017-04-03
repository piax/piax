/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestDdll2.java 1256 2015-08-02 13:44:03Z teranisi $
 */

package test.sg;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.NeighborSet;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ddll.Node.InsertionResult;
import org.piax.gtrans.ov.ddll.NodeArray4Test;
import org.piax.gtrans.ov.ddll.NodeManager;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 以下のテストを行う。
 * 1. 整数をkeyとして持つnodeをnumNodeの数だけ立ち上げる。
 * 2. すべてのnodeをinsertする。
 * 3. 指定した確率でnode障害を起こし、修復される過程を見る。
 * 4. 指定した確率でnode障害を起こすと同時に障害nodeをrevivalさせ、修復される過程を見る。
 * 6. 立ち上げたピアを終了させる。
 * 
 * 処理の実行は、Executors.newFixedThreadPool(n) を使って同時に行う。
 * n は使用するスレッド数で、numNodeと一致させておく。
 * 
 * @author     Mikio Yoshida
 * @version    1.0.0
 */
public class TestDdll2 extends TestDdll1 {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(TestDdll2.class);

    static final int RETRY = 30;

//  static Random rand = new MersenneTwister(123);
    static Random rand = new MersenneTwister();
    static int numNode;
    static NodeManager[] managers;
    static Node[] nodes;
    static Peer[] peers;
    static int GRAIN;

    static int next() {
        int r = rand.nextInt(numNode);
        return r;
    }

    static ExecutorService threadPool;
    static void exec(final TType ttype, final int n) {
        threadPool.execute(
                new Runnable() {
                    public void run() {
                        testFunc(ttype, n);
                    }
                });
    }

    static void waitForExecFin() {
        threadPool.shutdown();
        try {
            // 最大10分待つ
            if (!threadPool.awaitTermination(600000, TimeUnit.MILLISECONDS)) {
                logger.warn("some tasks not terminated");
            }
        } catch (InterruptedException ignore) {
        }
        // setLが完了するまでの待ち時間 
        waitForRecFin();
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
                }
            } catch (IllegalStateException e) {
            }
            break;
        case FIN:
            managers[n].fin();
            if (n % GRAIN == 0)
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

    public static void main(String[] args) throws Exception {
        
        if (args.length != 0 && args.length != 1) {
            System.err.println("usage: <cmd> numNode");
            System.err.println(" ex. <cmd> 100");
            System.exit(-1);
        }
        
//        Node.GOD_MODE = true;
        Node.GETSTAT_OP_TIMEOUT = 50;
        final int FIX_OP_PERIOD = 500;
        NeighborSet.setDefaultNeighborSetSize(10);
        
        locatorType = "emu";
        numNode = 40;
        int numThread = numNode;    // 同時に起動するスレッド数
        GRAIN = 5;      // 出力の表示間隔
        double failureRate = 0.3;
        double revivalRate = 0.5;
        
        if (args.length == 1) {
            numNode = Integer.parseInt(args[0]);
        }

        System.out.printf("** Simulation start.%n");
        System.out.printf(" - num of nodes: %d%n", numNode);
        System.out.printf(" - transport: %s%n", locatorType);
        managers = new NodeManager[numNode];
        nodes = new Node[numNode];
        peers = new Peer[numNode];
        NodeArray4Test.nodes = nodes;

        // new
        for (int i = 0; i < nodes.length; i++) {
            peers[i] = Peer.getInstance(PeerId.newId());
            PeerLocator loc = newLocator(locatorType, i);
            managers[i] = new NodeManager(
                    peers[i].newBaseChannelTransport(loc));
            nodes[i] = managers[i].createNode(i, "");
            nodes[i].setCheckPeriod(FIX_OP_PERIOD);
        }
        // 0番目のnodeはinitial nodeとしてinsertし、安定稼働させる
        nodes[0].insertAsInitialNode();
        seedLoc = nodes[0].getMyLink();

        // 全部insert
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** insert all **%n");
        for (int i = 1; i < nodes.length; i++) {
            exec(TType.INSERT, i);
        }
        waitForExecFin();
        NodeArray4Test.dump();
        sleep(3000);
        
        // failureを起こして修復状態を見る
        for (int n = 0; n < 2; n++) {
            // failure
            threadPool = Executors.newFixedThreadPool(numThread);
            System.out.printf("%n** failure **%n");
            for (int i = 1; i < nodes.length; i++) {
                if (Math.random() < failureRate) 
                    exec(TType.OFF, i);
            }
            waitForExecFin();
            boolean consis = NodeArray4Test.dump();
            for (int i = 0; i < 20; i++) {
                if (consis) break;
                sleep(2000);
                consis = NodeArray4Test.dump();
            }
        }
        
        // failureを起こして修復状態を見る。revival有り
        for (int n = 0; n < 2; n++) {
            // failure
            threadPool = Executors.newFixedThreadPool(numThread);
            System.out.printf("%n** failure with revival **%n");
            for (int i = 1; i < nodes.length; i++) {
                if (nodes[i].isOnline()) {
                    if (Math.random() < failureRate) 
                        exec(TType.OFF, i);
                } else {
                    if (Math.random() < revivalRate) 
                        exec(TType.ON, i);
                }
            }
            waitForExecFin();
            boolean consis = NodeArray4Test.dump();
            for (int i = 0; i < 20; i++) {
                if (consis) break;
                sleep(2000);
                consis = NodeArray4Test.dump();
            }
        }

        // fin
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** fin **%n");
        for (int i = 0; i < nodes.length; i++) {
            exec(TType.FIN, i);
        }
        waitForExecFin();
        
        for (int i = 0; i < nodes.length; i++) {
            peers[i].fin();
        }
        System.out.printf("%n** End of Simulation.%n");
//        System.exit(0);
    }
}
