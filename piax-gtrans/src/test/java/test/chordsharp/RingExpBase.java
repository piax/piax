/*
 * Revision History:
 * ---
 * 2009/02/13 designed and implemented by M. Yoshida.
 * 
 * $Id: TestAggSG.java 823 2013-08-25 00:41:53Z yos $
 */

package test.chordsharp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.impl.ReceiverThreadPool;
import org.piax.gtrans.ov.ddll.NodeMonitor;
import org.piax.gtrans.ov.ring.RingManager;
import org.piax.gtrans.ov.ring.UnavailableException;
import org.piax.gtrans.ov.ring.rq.FlexibleArray;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RingExpBase<S extends RingManager<PeerLocator>> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RingExpBase.class);

    static enum TType {
        INSERT, DELETE, FIN, ADD, OFF, ON
    }

    static enum Status {
        OUT, INSERTING, IN, DELETING, FAIL
    }

    protected static Random rand = new MersenneTwister();
    //    static Random rand = new MersenneTwister(123);
    protected static ExecutorService threadPool;
    protected int numNode;
    protected FlexibleArray<S> nodes;
    protected Peer[] peers;

    protected String locatorType;
    protected PeerLocator seedLoc;
    protected int seedNo;
    protected int numThread;
    protected int nkeys;

    protected Status[] status;

    protected RingExpBase(int nodes) {
        this.numNode = nodes;
    }

    // override this method if S is not RingManager
    S createNode(ChannelTransport trans, int n) throws IOException,
            IdConflictException {
        S s = (S) new RingManager<PeerLocator>(new TransportId("ring"), trans);
        return s;
    }

    boolean insertNode(PeerLocator introducer, int n) throws IOException,
            UnavailableException {
        return nodes.get(n).addKey(introducer, key(n));
    }

    boolean insertNode(PeerLocator introducer, int n, int nth)
            throws IOException, UnavailableException {
        return nodes.get(n).addKey(introducer, key(n, nth));
    }

    boolean insertNode(int introducer, int n) throws IOException,
            UnavailableException {
        return nodes.get(n).addKey(nodes.get(introducer).getEndpoint(), key(n));
    }

    boolean deleteNode(int n) throws IOException, UnavailableException {
        System.out.println("# delete node " + n);
        boolean rc = nodes.get(n).removeKey(key(n));
        System.out.println("# delete node " + n + " returned " + rc);
        return rc;
    }

    void failNode(int n) {
        System.out.println("# fail node " + n);
        nodes.get(n).offline();
    }

    protected int key(int n) {
        return key(n, 0);
    }

    protected int key(int n, int nth) {
        return n + nth * 100;
    }

    int next() {
        int r = rand.nextInt(numNode);
        return r;
    }

    public static void sleep(int msec) {
        System.out.println("# sleep " + msec + " start");
        try {
            Thread.sleep(msec);
        } catch (InterruptedException ignore) {
        }
        System.out.println("# sleep " + msec + " done");
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

    void waitForExecFin() {
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

    void waitForRecFin() {
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

    @SuppressWarnings("deprecation")
    void testFunc(TType ttype, int n) {
        RingManager<PeerLocator> sg = nodes.get(n);
        switch (ttype) {
        case INSERT:
            try {
                //if (!sg.addPoint(seedLoc, value(n))) {
                if (!insertNode(seedLoc, n)) {
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
                if (!sg.removeKey(new Integer(n))) {
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

    void execSeq(final TType ttype, final int n) {
        testFunc(ttype, n);
    }

    void exec(final TType ttype, final int n) {
        threadPool.execute(new Runnable() {
            public void run() {
                testFunc(ttype, n);
            }
        });
    }

    public static PeerLocator newLocator(String locatorType, int vport) {
        PeerLocator peerLocator;
        if (locatorType.equals("emu")) {
            peerLocator = new EmuLocator(vport);
        } else if (locatorType.equals("udp")) {
            peerLocator =
                    new UdpLocator(new InetSocketAddress("localhost",
                            10000 + vport));
        } else {
            peerLocator =
                    new TcpLocator(new InetSocketAddress("localhost",
                            10000 + vport));
        }
        return peerLocator;
    }

    public void init(int numNode) throws Exception {
        // set the timeout value of DDLL NodeMonitor.
        NodeMonitor.PING_TIMEOUT = 1 * 1000;

        this.numNode = numNode;
        // 安定している唯一のノード
        seedNo = 0;//next();
        seedLoc = newLocator(locatorType, seedNo);

        numThread = numNode; // 同時に起動するスレッド数
        nkeys = numNode;
        status = new Status[nkeys];
        Arrays.fill(status, Status.OUT);

        System.out.printf("** Simulation start.%n");
        System.out.printf(" - num of nodes: %d%n", numNode);
        System.out.printf(" - seed node: %d%n", seedNo);
        System.out.printf(" - transport: %s%n", locatorType);
        nodes = new FlexibleArray<S>();
        peers = new Peer[numNode];

        // new
        for (int i = 0; i < numNode; i++) {
            peers[i] = Peer.getInstance(new PeerId("P" + i));
            PeerLocator loc = newLocator(locatorType, i);
            nodes.set(i,
                    (S) createNode(peers[i].newBaseChannelTransport(loc), i));
        }

        // insert the seed node
        insertNode(seedLoc, seedNo);
    }

    public void finAll() {
        for (int i = 0; i < nodes.size(); i++) {
            peers[i].fin();
        }
    }

    public void dump() {
        for (int i = 0; i < nodes.size(); i++) {
            System.out.println("Node " + i);
            System.out.println(nodes.get(i));
        }
    }

    /*protected void insertNode(int introducer, int i) throws UnavailableException, IOException { 
        nodes.get(i).addKey(nodes.get(introducer).getEndpoint(), i);
    }*/

    protected void insertAllSequentially() throws UnavailableException {
        insert(0, nodes.size());
    }

    protected void insertAllSequentially(int nth) throws UnavailableException {
        insertMulti(0, nodes.size(), nth);
    }

    protected void insert(int from, int to) throws UnavailableException {
        System.out.println("*** insert nodes " + from + " to " + to);
        for (int i = from; i < Math.min(to, nodes.size()); i++) {
            if (i != seedNo) {
                System.out.println("* inserting " + i);
                try {
                    insertNode(seedLoc, i);
                } catch (IOException e) {
                    System.err.println("got " + e);
                }
            }
        }
        System.out.println("*** insert nodes " + from + " to " + to
                + " finished");
    }

    protected void insertMulti(int from, int to, int nth)
            throws UnavailableException {
        System.out.println("*** insert nodes " + from + " to " + to + " (nth="
                + nth + ")");
        for (int i = from; i < Math.min(to, nodes.size()); i++) {
            if (i == seedNo && nth == 0) {
                //
            } else {
                System.out.println("* inserting " + i);
                try {
                    insertNode(seedLoc, i, nth);
                } catch (IOException e) {
                    System.err.println("got " + e);
                }
            }
        }
        System.out.println("*** insert nodes " + from + " to " + to
                + " finished");
    }

    protected void insertAllSequentiallyRandomIntroducer()
            throws UnavailableException {
        // insert sequentially
        System.out.println("*** insert all");
        PeerLocator s;

        for (int i = 0; i < nodes.size(); i++) {
            if (i == seedNo) {
                continue;
            }
            if (i == 0) {
                s = seedLoc;
            } else {
                S tmp = nodes.get((int) (Math.random() * i));
                s = tmp.getEndpoint();
            }
            System.out
                    .println("* inserting " + i + " (introducer = " + s + ")");
            try {
                insertNode(s, i);
            } catch (IOException e) {
                System.err.println("got " + e);
            }
        }
        System.out.println("*** insert finished");
    }

    protected void insertAllInParallel() {
        threadPool = Executors.newFixedThreadPool(numThread);
        System.out.printf("%n** insert all **%n");
        for (int i = 0; i < nkeys; i++) {
            if (i == seedNo) {
                continue;
            }
            exec(TType.INSERT, i);
        }
        waitForExecFin();

    }

    /*protected void outputTopology(File file) {
        try {
            List<Range<?>> range =
                    Collections.<Range<?>> singletonList(new Range<Integer>(0,
                            10));
            List<SGNodeDetails> nodeList =
                    SkipGraphDrawer.gatherSGNodeDetails(nodes.get(0), range,
                            1000);
            SkipGraphDrawer.outputFile(file, nodeList, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
}
