package org.piax.ayame.sim;

import static org.piax.ayame.EventExecutor.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.piax.ayame.Event.Lookup;
import org.piax.ayame.EventException;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.FTEntry;
import org.piax.ayame.FailureCallback;
import org.piax.ayame.LatencyProvider.StarLatencyProvider;
import org.piax.ayame.LocalNode;
import org.piax.ayame.NetworkParams;
import org.piax.ayame.Node;
import org.piax.ayame.Node.NodeMode;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.ov.atomic.AtomicRingStrategy;
import org.piax.ayame.ov.atomic.AtomicRingStrategy.AtomicRingNodeFactory;
import org.piax.ayame.ov.chord.ChordStrategy.ChordNodeFactory;
import org.piax.ayame.ov.cmr.CmrStrategy;
import org.piax.ayame.ov.cmr.CmrStrategy.CmrNodeFactory;
import org.piax.ayame.ov.ddll.DdllKey;
import org.piax.ayame.ov.ddll.DdllStrategy;
import org.piax.ayame.ov.ddll.DdllStrategy.DdllNodeFactory;
import org.piax.ayame.ov.ddll.DdllStrategy.RetryMode;
import org.piax.ayame.ov.sg.SkipGraphStrategy.SkipGraphNodeFactory;
import org.piax.ayame.ov.suzaku.SuzakuStrategy;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.ayame.sim.stats.MultiStatSet;
import org.piax.ayame.sim.stats.Stat;
import org.piax.ayame.sim.stats.StatSet;
import org.piax.common.Option;
import org.piax.common.Option.BooleanOption;
import org.piax.common.Option.DoubleOption;
import org.piax.common.Option.EnumOption;
import org.piax.common.Option.IntegerOption;
import org.piax.common.TransportId;
import org.piax.util.MersenneTwister;
import org.piax.util.UniqId;

public class Sim {
    @FunctionalInterface
    public interface GetFactory {
        NodeFactory getFactory();
    }
    public enum Algorithm {
        AtomicRing(() -> new AtomicRingNodeFactory()), 
        DDLL(() -> new DdllNodeFactory()),
        CHORDSHARP(() -> new SuzakuNodeFactory(0)), 
        SUZAKU(() -> new SuzakuNodeFactory(1)), 
        SUZAKU2(() -> new SuzakuNodeFactory(2)), 
        SUZAKU3(() -> new SuzakuNodeFactory(3)),
        CHORD(() -> new ChordNodeFactory()),
        CMR(() -> new CmrNodeFactory()),
        SKIPGRAPH(() -> new SkipGraphNodeFactory());
        public GetFactory method;
        private Algorithm(GetFactory method) {
            this.method = method;
        }
    }

    @FunctionalInterface
    public interface ExpMethod {
        void run(Sim sim, NodeFactory factory);
    }
    public enum ExpType {
        CONCURRENTJOIN((sim, factory)-> sim.concurrentJoin(factory)), 
        RETRANSTIME((sim, factory) -> sim.retransTest(factory)),
        MIXLATENCY((sim, factory) -> sim.mixedLatencyTest(factory)),
        SIMPLE((sim, factory) -> sim.simpleTest(factory)),
        INSERTSEQ((sim, factory) -> sim.insertSeqTest(factory)),
        INSERTLOOKUP((sim, factory) -> sim.expInsertLookupTest(factory,
                insOrder.value(), false)),
        INSERTFAILLOOKUP((sim, factory) -> sim.expInsertLookupTest(factory,
                insOrder.value(), true)),
        DELETELOOKUP((sim, factory) -> sim.deleteLookupTest(factory)),
        INSERTFAILREPEAT((sim, factory) -> sim.insertFailRepeat(factory,
                insOrder.value(), true)),
        LOOKUPVARYN((sim, factory) -> sim.lookupVaryingN(factory)),
        PERMUTATIONS((sim, factory) -> sim.permutation(factory)),
        SPECIFICORDER((sim, factory) -> sim.specificOrder(factory)),
        INSERTDELETE((sim, factory) -> sim.insertDelete(factory)),
        HOPSBYDIST((sim, factory) -> sim.expHopsByDistance(factory)),
        JOINMSGS((sim, factory) -> sim.msgs4Join(factory)),
        FTDISTANCE((sim, factory) -> sim.expFtDistance(factory)),
        NODESVSHOPS((sim, factory) -> sim.expNodesVsHops(factory));

        public ExpMethod method;
        private ExpType(ExpMethod exp) {
            this.method = exp;
        }
    }
    
    @FunctionalInterface
    public interface InsertMethod {
        void insert(Sim sim, LocalNode[] nodes, int from, int to,
                long delay1, long delay2, Runnable after);
    }
    public enum InsertOrder {
        LTOR((sim, nodes, from, to, delay1, delay2, after)
                -> sim.insertSeqLR(nodes, from, to, delay1, delay2, after)),
        RTOL((sim, nodes, from, to, delay1, delay2, after)
                -> sim.insertSeqRL(nodes, from, to, delay1, delay2, after)),
        RANDOM((sim, nodes, from, to, delay1, delay2, after)
                -> sim.insertRandom(nodes, from, to, delay1, delay2, after));
        public InsertMethod method;
        private InsertOrder(InsertMethod method) {
            this.method = method;
        }
    }

    // Command line Options
    public static BooleanOption help
        = new BooleanOption(false, "-help", (val) -> {
            if (val) {
                System.out.println("Usage: java Sim [options]");
                System.out.println("Options:");
                Option.help();
                System.exit(0);
            }
        });

    public static LocalNode[] nodes;
    public static BooleanOption verbOpt = new BooleanOption(false, "-verbose",
            val -> {Log.verbose = val;});
    public static EnumOption<Algorithm> algorithm
        = new EnumOption<>(Algorithm.class, Algorithm.DDLL, "-algorithm");
    // use PIAX network as the underlying network
    // if you turn this option on, also you must turn on "-realtime" option. 
    public static BooleanOption netOpt = new BooleanOption(false, "-net");
    public static EnumOption<ExpType> exptype
        = new EnumOption<>(ExpType.class, ExpType.CONCURRENTJOIN, "-type");
    private static IntegerOption seedOption = new IntegerOption(-1, "-seed", val -> {
        if (val == -1) {
            EventExecutor.setRandom(new MersenneTwister());
        } else {
            EventExecutor.setRandom(new MersenneTwister(val));
        }
    });
    // used by expNodesVsHops
    private static IntegerOption numNodesOption = new IntegerOption(0, "-nodes");
    // used by expFtDistance
    private static IntegerOption numIteration = new IntegerOption(1, "-iter");
    public static EnumOption<InsertOrder> insOrder
        = new EnumOption<InsertOrder>(InsertOrder.class, InsertOrder.RANDOM,
                "-insorder");
    public static DoubleOption slowNodeRatio
        = new DoubleOption(0.0, "-slowratio");
    // INSERTDELETEにおける，ノードの平均生存時間
    public static DoubleOption aveLifeTime
        = new DoubleOption(convertSecondsToVTime(15*60), "-avelife");
    public static DoubleOption failRate
        = new DoubleOption(0.0, "-failRate");
    StarLatencyProvider latencyProvider = new StarLatencyProvider();

    public static void main(String[] args) {
        Log.init();
        // force load to initialize Options
        EventExecutor.load();
        SuzakuStrategy.load();
        DdllStrategy.load();
        AtomicRingStrategy.load();
        CmrStrategy.load();
        //NetworkParams.load();

        List<String> argList = new ArrayList<>(Arrays.asList(args));
        Option.parseParams(argList);
        args = argList.toArray(new String[0]);
        //System.out.println("args remained: " + Arrays.toString(args));

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
            default:
                System.err.println("Unknown argument: " + args[i]);
                System.exit(1);
            }
        }
        System.out.println("Simulation Configurations:");
        for (Option<?> opt: Option.allOptions()) {
            System.out.println(" " + opt.getArgName() + ": " + opt.value());
        }
        System.out.println();
        // start simulation
        long startTime = System.currentTimeMillis();
        new Sim().sim(algorithm.value(), exptype.value());
        long endTime = System.currentTimeMillis();
        long elapsed = endTime - startTime;
        System.out.println("\n");
        System.out.println("Simulation time: " + elapsed/1000 + " sec");
        System.exit(0);
    }

    private void sim(Algorithm algorithm, ExpType exptype) {
        EventExecutor.setLatencyProvider(latencyProvider);
        NodeFactory factory = algorithm.method.getFactory();
        exptype.method.run(this, factory);
    }

    private static void startSim(LocalNode[] nodes) {
        startSim(nodes, 0);
    }

    private static void startSim(LocalNode[] nodes, long duration) {
        Sim.nodes = nodes;
        EventExecutor.startSimulation(duration);
    }

    public static LocalNode[] getNodes() {
        return nodes;
    }
    
    /**
     * locate the node position and insert
     * @param n
     * @param introducer
     * @param delay
     * @param callback
     */
    public static void joinLater(LocalNode n, LocalNode introducer, long delay,
            Runnable callback) {
        joinLater(n, introducer, delay, callback, (exc) -> {
            throw new Error("joinLater got exception", exc);
        });
    }

    public static void joinLater(LocalNode n, LocalNode introducer, long delay,
            Runnable callback, FailureCallback failure) {
        //n.mode = NodeMode.TO_BE_INSERTED;
        if (delay == 0) {
            joinAsync(n, introducer, callback, failure);
        } else {
            EventExecutor.sched(delay, () -> {
                joinAsync(n, introducer, callback, failure);
            });
        }
    }
    
    public static void joinAsync(LocalNode n, LocalNode introducer,
            Runnable callback) {
        joinAsync(n, introducer, callback, (exc) -> {
            System.out.println(n + ": joinAsync: finished with " + exc);
        });
    }
    public static void joinAsync(LocalNode n, LocalNode introducer,
            Runnable callback, FailureCallback failure) {
        CompletableFuture<Boolean> future = n.joinAsync(introducer);
        future.handle((rc, exc) -> {
            if (exc != null) {
                failure.run((EventException)exc);
            } else if (rc) {
                callback.run();
            } else {
                System.out.println("joinAsync finished with false!");
            }
            return false;
        });
    }
    
    public static void dump(LocalNode start) {
        System.out.println("node dump:");
        LocalNode x = start;
        while (true) {
            System.out.println(x.toStringDetail());
            x = (LocalNode)x.succ;
            if (x == start) {
                break;
            }
        }
    }

    public static void dump(LocalNode[] nodes) {
        System.out.println("node dump:");
        int c = 0;
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].mode == NodeMode.INSERTED) {
                System.out.println(i + ": " + nodes[i].toStringDetail());
                c++;
            }
        }
        System.out.println("# of inserted node: " + c);
    }
    
    public static long convertSecondsToVTime(int sec) {
        if (EventExecutor.realtime.value()) {
            return (long)(sec * 1000);
        } else {
            return (long)(sec * 1000 / NetworkParams.LATENCY_FACTOR);
        }
    }

    private LocalNode createNode(NodeFactory factory, int key) {
        return createNode(factory, key, NetworkParams.HALFWAY_DELAY);
    }

    private LocalNode createNode(NodeFactory factory, int key, long latency) {
        TransportId transId = new TransportId("SimTrans");
        if (netOpt.value()) {
/*            Peer peer = Peer.getInstance(new PeerId("P" + key));
            DdllKey k = new DdllKey(key, new UniqId(peer.getPeerId()), "", null);
            PeerLocator loc = newLocator("emu", key);
            ChannelTransport<?> trans;
            try {
                trans = peer.newBaseChannelTransport(loc);
                LocalNode n = new LocalNode(transId, trans, k);
                factory.setupNode(n);
                latencyProvider.add(n, latency);
                return n;
            } catch (IOException | IdConflictException e) {
                throw new Error("something wrong!", e);
            }
            */
            throw new Error("run as net not supported now.");
        } else {
            UniqId p = new UniqId("P" + key);
            DdllKey k = new DdllKey(key, p, "", null);
            LocalNode n = new LocalNode(k, null);
            factory.setupNode(n);
            latencyProvider.add(n, latency);
            return n;
        }
    }
/*
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
*/    
    /**
     * keyを検索し，統計情報を stat に追加する．
     * 
     * @param key
     * @param stat
     */
    public static void lookup(LocalNode from, DdllKey key, LookupStat stat) {
        // System.out.println(from + " lookup " + key);
        long start = EventExecutor.getVTime();
        Lookup ev = new Lookup(from, key);
        ev.onReply((done, exc) -> {
            if (exc != null) {
                System.out.println("Lookup failed: " + exc);
                return;
            }
            if (done.req.key.compareTo(done.pred.key) != 0) {
                System.out.println("Lookup error: req.key=" + done.req.key
                        + ", " + done.pred.key);
                System.out.println(done.pred.toStringDetail());
                //dispatcher.dump();
                stat.lookupFailure.addSample(1);
            } else {
                stat.lookupFailure.addSample(0);
            }
            // 先頭と末尾は検索開始ノードなので2を減じる．
            // ただし，検索開始ノード＝検索対象ノードの場合 0 ホップ
            int h = Math.max(done.routeWithFailed.size() - 2, 0);
            stat.hops.addSample(h);
            int nfails = done.routeWithFailed.size() - done.route.size();
            //stat.failedNodes.addSample(nfails);
            stat.failedNodes.addSample(nfails > 0 ? 1 : 0);
            long end = EventExecutor.getVTime();
            long elapsed = (int) (end - start);
            stat.time.addSample((double) elapsed);
            if (nfails > 0) {
                System.out.println("lookup done!: " + done.route + " (" + h
                        + " hops, " + elapsed + ", actual route="
                        + done.routeWithFailed + ", evid="
                        + done.req.getEventId() + ")");
            } else {
//                System.out.println("lookup done: " + done.route + " (" + h
//                        + " hops, " + elapsed + ")");
            }
        });
        from.post(ev);
    }

    private void simpleTest(NodeFactory factory) {
        LocalNode a = createNode(factory, 0, NetworkParams.HALFWAY_DELAY);
        a.joinInitialNode();
        LocalNode b = createNode(factory, 10, NetworkParams.HALFWAY_DELAY);
        LocalNode z = createNode(factory, 100, NetworkParams.HALFWAY_DELAY);
        joinAsync(b, a, () -> System.out.println(b + " joined!"),
                exc -> {
                    System.out.println("Node b join failed!");
                });
        /*z.joinAsync(a, () -> System.out.println(z + " joined"),
                exc -> {
                    System.out.println("Node z join failed");
                });*/
        
        /*Node c = createNode(cons, 20);
        c.join0(a);
        Node d = createNode(cons, 30);
        d.join0(a);*/
        //NodeImpl[] nodes = new NodeImpl[]{a, z, b, c, d};
        LocalNode[] nodes = new LocalNode[] { a, z, b };
        Arrays.sort(nodes);
        EventExecutor.sched(2000, () -> {
            b.fail();
            joinAsync(z, a, () -> System.out.println(z + " joined"),
                    exc -> {
                        System.out.println("Node z join failed: " + exc);
                    });
            
        });
        
        startSim(nodes, 100000);
        System.out.println(a.toStringDetail());
        System.out.println(b.toStringDetail());
        System.out.println(z.toStringDetail()); 
        //EventDispatcher.nmsgs = 0;
        //System.out.println("*****************************");
        //LookupStat s = new LookupStat();
        //a.lookup(z.key, s); 
        //a.lookup(b.key, s); 
        //startSim(nodes);
        //s.hops.printBasicStat("hops", 0);
        //dump(nodes);
    }

    /**
     * nodesのfrom番目からto番目を順番に挿入する．
     * 
     * @param nodes
     * @param from
     * @param to
     * @param delay1
     * @param delay2
     * @param after
     */
    private void insertSeqLR(LocalNode[] nodes, int from, int to, 
            long delay1, long delay2, Runnable after) {
        List<Integer> order = new ArrayList<>();
        IntStream.range(from, to).forEachOrdered(order::add);
        insertSeq(nodes, order, 0, delay1, delay2, after); 
    }

    private void insertSeqRL(LocalNode[] nodes, int from, int to,
            long delay1, long delay2, Runnable after) {
        List<Integer> order = new ArrayList<>();
        IntStream.range(from, to).forEachOrdered(order::add);
        Collections.reverse(order);
        insertSeq(nodes, order, 0, delay1, delay2, after); 
    }

    private List<Integer> insertRandom(LocalNode[] nodes, int from, int to,
            long delay1, long delay2, Runnable after) {
        return insertRandom(nodes, from, to, delay1, delay2, after, null);
    }

    private List<Integer> insertRandom(LocalNode[] nodes, int from, int to,
            long delay1, long delay2, Runnable after,
            Consumer<LocalNode> insertCallback) {
        List<Integer> order = new ArrayList<>();
        IntStream.range(from, to).forEach(order::add);
        Collections.shuffle(order, EventExecutor.random());
        insertSeq(nodes, order, 0, delay1, delay2, after, insertCallback);
        return order;
    }

    private void insertSeq(LocalNode[] nodes, List<Integer> order, int index,
            long initialDelay, long afterDelay, Runnable after) {
        insertSeq(nodes, order, index, initialDelay, afterDelay, after, null);
    }

    /**
     * 指定された順序でノードを逐次的に挿入する．
     * <pre>
     * +delay1 N0挿入開始
     * N0挿入完了+delay2: N1挿入開始
     * N1挿入完了+delay2: N2挿入開始
     * ...
     * </pre>
     * @param nodes
     * @param order
     * @param index
     * @param initialDelay
     * @param afterDelay
     * @param after
     * @param insertCallback
     */
    private void insertSeq(LocalNode[] nodes, List<Integer> order, int index,
            long initialDelay, long afterDelay, Runnable after,
            Consumer<LocalNode> insertCallback) {
        LocalNode introducer = nodes[0];
        LocalNode node = nodes[order.get(index)];
        joinLater(node, introducer, initialDelay, () -> {
            cNode++;
            if (insertCallback != null) {
                insertCallback.accept(node);
            }
            if (index + 1 < order.size()) {
                insertSeq(nodes, order, index + 1, afterDelay, afterDelay,
                        after, insertCallback);
            } else {
                System.out.println("** insertion finished: vtime="
                        + EventExecutor.getVTime() + ", rtime="
                        + NetworkParams.toRealTime(EventExecutor.getVTime())
                    );
                if (after != null) {
                    after.run();
                }
            }
        });
    }

    private void insertSeqTest(NodeFactory factory) {
        int N = 16;
        LocalNode[] nodes = new LocalNode[N];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }
        nodes[0].joinInitialNode();
        insOrder.value().method.insert(this, nodes, 1, nodes.length, 0, 0, null);
        Arrays.sort(nodes);
        dump(nodes);
        startSim(nodes, 5000);
        EventExecutor.nmsgs = 0;
        System.out.println("*****************************");
        dump(nodes);
        System.out.println("*****************************");
        EventExecutor.dumpMessageCounters();
        System.out.println("*****************************");
    }

    final static int NQUERY = 2000;
    Runnable lookupTest(LocalNode[] nodes, LookupStat s) {
        return lookupTest(nodes, s, NQUERY);
    }
    Runnable lookupTest(LocalNode[] nodes, LookupStat s, int nquery) {
        return () -> {
            System.out.println("start lookupTest: " + EventExecutor.getVTime());
            for (int i = 0; i < nquery; i++) {
                lookup1(nodes, s, null, null);
            }
        };
    }
    private void distLookupTest(LocalNode[] nodes, LookupStat s, long tFrom,
            int tWidth) {
        distLookupTest(nodes, s, tFrom, tWidth, null);
    }

    /**
     * 時刻tFromから時刻tFrom + random*tWidthまでの範囲で繰り返し検索する．
     *  
     * @param nodes
     * @param s
     * @param tFrom
     * @param tWidth
     */
    private void distLookupTest(LocalNode[] nodes, LookupStat s, long tFrom,
            int tWidth, boolean[] ignore) {
        distLookupTest(nodes, s, tFrom, tWidth, ignore, ignore, NQUERY);
    }

    private void distLookupTest(LocalNode[] nodes, LookupStat s, long tFrom,
            int tWidth, boolean[] ignFrom, boolean[] ignTo, int nquery) {
        for (int i = 0; i < nquery; i++) {
            long t = tFrom + (tWidth > 0 ? random().nextInt(tWidth) : 0);
            EventExecutor.sched(t, () -> {
                lookup1(nodes, s, ignFrom, ignTo);
            });
        }
    }

    private void lookup1(LocalNode[] nodes, LookupStat s,
            boolean[] ignFrom, boolean[] ignTo) {
        int from, dest;
        do {
            from = random().nextInt(nodes.length);
        } while (nodes[from].mode != NodeMode.INSERTED
                || (ignFrom != null && ignFrom[from]));
        do {
            dest = random().nextInt(nodes.length);
        } while (nodes[dest].mode != NodeMode.INSERTED
                || (ignTo != null && ignTo[dest]));
        lookup(nodes[from], nodes[dest].key, s);
    }

    Runnable lookupTestFull(LocalNode[] nodes, int start, int end, LookupStat s) {
        return () -> {
            System.out.println("start lookupTest: " + EventExecutor.getVTime());
            for (int from = start; from < end; from++) {
                if (nodes[from].mode != NodeMode.INSERTED) continue;
                for (int to = start; to < end; to++) {
                    if (nodes[to].mode != NodeMode.INSERTED) continue;
                    lookup(nodes[from], nodes[to].key, s);
                }
            }
        };
    }

    private void expInsertLookupTest(NodeFactory factory,
            InsertOrder insOrder, boolean doFail) {
        DdllStrategy.pingPeriod.set(0);
//        boolean pu2 = SuzakuStrategy.PASSIVE_UPDATE_2.value();
//        boolean pu2bid = SuzakuStrategy.PASSIVE_UPDATE_2_BIDIRECTIONAL.value();
//        String postfix = (pu2 ? ("pu2"
//                + (pu2bid ? "-bid" : "")) : "nopu2");
//        postfix += "-" + insOrder.toString().toLowerCase();
        int num = numNodesOption.value();   // 全ノード数
        int initial = num;  // 最初に挿入するノード数
        // 後で挿入するノード数
        int later = num - initial;
        // 後で故障/削除するノード数 (doFail == true のときのみ使用)
        int nFail= 16;
        assert !doFail || nFail <= num;
        LocalNode[] nodes = new LocalNode[num];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }

        // T秒ごとにLOOKUP_TIMES回，lookupTestを実行
        long T = convertSecondsToVTime(60);
        int LOOKUP_TIMES = 36;
        AllLookupStats all = new AllLookupStats();
        StatSet msgset = new StatSet(); // # of messages for finger table update
        StatSet numStatSet = new StatSet(); // # of nodes

        Runnable doLookup = () -> {
            if (true) for (int i = 0; i < LOOKUP_TIMES; i++) {
                long t = i * T;
                LookupStat s = all.getLookupStat(i);
                distLookupTest(nodes, s, t, 0*(int)T);
                Stat ms = msgset.getStat(i);
                Stat ns = numStatSet.getStat(i);
                final int i0 = i;
                EventExecutor.sched(t, () -> {
                    System.err.println("!!! " + (i0+1) + "/" + LOOKUP_TIMES
                            + ", T=" + EventExecutor.getVTime());
                    collectMessageCounts(nodes, ms);
                    ns.addSample(cNode);
                    System.out.println("T = " + t
                            + " (real=" + NetworkParams.toRealTime(t) +")");
                    dump(nodes);
                });
            }
        };

        // insert the initial node
        nodes[0].joinInitialNode();
        // and others
        insOrder.method.insert(this, nodes, 1, initial, 0, 0, () -> {
            doLookup.run();
        });
        System.out.println("*****************************");

        // 後で挿入するノードを 10 * T 時間後に挿入
        if (later != 0) {
            long start = EventExecutor.getVTime();
            long delay = 40 * T;
            Runnable after = () -> {
                System.out.println("*****************************");
                System.out.println("insertion of the rest nodes done!");
                System.out.println("!!! insertion duration + "
                        + (start + delay) + " to " + EventExecutor.getVTime());
                dump(nodes);
                System.out.println("*****************************");
            };
            insOrder.method.insert(this, nodes, initial, num, delay, 0, after);
        }

        // nFail個のノードを時刻を後で故障させる
        if (doFail) {
            EventExecutor.sched(T * 50, 
                    () -> {
                        System.out.println("FAIL!!! " + EventExecutor.getVTime());
                        boolean[] failed = new boolean[num];
                        for (int i = 0; i < nFail; i++) {
                            int r;
                            do {
                                r = random().nextInt(num);
                            } while (failed[r]);
                            failed[r] = true; 
                            //nodes[r].fail();
                            nodes[r].leaveAsync();
                        }
                    });//);
        }

//        double logNum = Math.log(num) / Math.log(2);
//        long duration = (int)(logNum * logNum * 
//                SuzakuStrategy.UPDATE_FINGER_PERIOD.value());
        // 2*60 だと Chord# で不足
        long duration = T * LOOKUP_TIMES + convertSecondsToVTime(4*60);
        System.err.println("duration=" + duration);
        startSim(nodes, duration);
        System.out.println("*****************************");
        dump(nodes);
        System.out.println("*****************************");
        all.hopSet.printBasicStat("hops");
        all.timeSet.printBasicStat("time");
        all.failSet.printBasicStat("lookupFails");
        all.failedNodeSet.printBasicStat("encounterFailedNodes");
        
        all.hopSet.printCSV("hopsdata");

        all.hopSet.getStat(1).outputFreqDist("dist-1", 1);
        all.hopSet.getStat(all.hopSet.lastKey()).outputFreqDist(
                "dist-last", 1);

        msgset.printBasicStat("ftmsgs");
        numStatSet.printBasicStat("numNodes");
        EventExecutor.dumpMessageCounters();
    }

    private void deleteLookupTest(NodeFactory factory) {
        int num = 256;   // 全ノード数
        int delStart = 32; // 削除開始ノード (inclusive)
        int delEnd = 96; // 削除終了ノード (exclusive)
        LocalNode[] nodes = new LocalNode[num];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }
        // 検索の周期
        long T = convertSecondsToVTime(10);
        AllLookupStats all = new AllLookupStats();
        StatSet msgset = new StatSet();
        StatSet numStatSet = new StatSet();
        // T 毎に LOOKUP_TIMES 回，lookupTest を実行
        int LOOKUP_TIMES = 20*5;
        int NQUERY = 100;
        long DELTIME = convertSecondsToVTime(50 * 60);
        Runnable doLookup = () -> {
            boolean[] ignFrom = new boolean[num];
            boolean[] ignTo = new boolean[num];
            IntStream.range(delStart, num).forEach(i -> {
                ignFrom[i] = true;
            });
            IntStream.range(0, delEnd).forEach(i -> {
                ignTo[i] = true;
            });
            IntStream.range(num / 2, num).forEach(i -> {
                ignTo[i] = true;
            });
            for (int i = 0; i < LOOKUP_TIMES; i++) {
                long t = DELTIME + (i - 1) * T;
                LookupStat s = all.getLookupStat(i);
                System.out.println("!!! sched lookup t=" + t);
                distLookupTest(nodes, s, t, (int)T, ignFrom, ignTo, NQUERY);
                Stat ms = msgset.getStat(i);
                EventExecutor.sched(t, () -> collectMessageCounts(nodes, ms));
                Stat ns = numStatSet.getStat(i);
                int i0 = i;
                EventExecutor.sched(t, () -> {
                    ns.addSample(cNode);
                    System.out.println("=== DUMP === " + i0);
                    dump(nodes);
                });
            }
            // ノード削除
            EventExecutor.sched(DELTIME, () -> {
                System.out.println("leave!!! " + EventExecutor.getVTime());
                int ndel = delEnd - delStart;
                boolean[] failed = new boolean[ndel];
                for (int i = 0; i < ndel; i++) {
                    int r;
                    do {
                        r = random().nextInt(ndel);
                    } while (failed[r]);
                    failed[r] = true; 
                    CompletableFuture<Boolean> future = nodes[delStart + r].leaveAsync();
                    future.handle((rc, exc) -> {
                        assert rc;
                        cNode--;
                        return false;
                    });
                }
            });
        };

        // insert the initial node
        nodes[0].joinInitialNode();
        // and others
        insOrder.value().method.insert(this, nodes, 1, num, 0, 0, () -> {
            doLookup.run();
        });
        System.out.println("*****************************");
        startSim(nodes, DELTIME * 2);
        System.out.println("*****************************");
        dump(nodes);
        System.out.println("*****************************");
        all.hopSet.printBasicStat("hops");
        all.timeSet.printBasicStat("time");
        all.failSet.printBasicStat("lookupFails");
        all.failedNodeSet.printBasicStat("encounterFailedNodes");
        for (int i = 0; i < LOOKUP_TIMES; i++) {
            all.timeSet.getStat(i).outputFreqDist("time-dist-" + i, 10000);
            //all.failedNodeSet.getStat(i).outputFreqDist("failed-dist-" + i, 1, false);
        }
        //msgset.printBasicStat("ftmsgs");
        numStatSet.printBasicStat("numNodes");
        //EventDispatcher.dumpMessageCounters();
    }

    /**
     * joinに必要なメッセージ数を計測する
     * 
     * @param factory
     */
    private void msgs4Join(NodeFactory factory) {
        MultiStatSet msgset = new MultiStatSet();
        // !!! Chord#では大きすぎる
        int ITER = 10;
        for (int i = 0; i < ITER; i++) {
            System.err.println("!!! iteration " + i);
            msgs4Join(factory, msgset);
        }
        msgset.printBasicStatAll();
        EventExecutor.dumpMessageCounters();
    }
    
    /**
     * numノードをランダムな順序で挿入し，その際各ノードのjoinに要したメッセージ数を
     * 計測する．
     * 
     * @param factory
     * @param statset
     */
    private void msgs4Join(NodeFactory factory, MultiStatSet cset) {
        int num = 100; // 挿入するノード数
        EventExecutor.reset();
        LocalNode[] allNodes = new LocalNode[num];
        for (int i = 0; i < allNodes.length; i++) {
            allNodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }

        allNodes[0].joinInitialNode();
        List<Integer> insOrder = insertRandom(allNodes, 1, num, 0, 0, () -> {
            // ノード挿入が終わったら最後の方に挿入したノードのfinger tableの更新のために
            // 2分間待ってから終了する．
            EventExecutor.sched("terminate", convertSecondsToVTime(2*60), () ->{
                EventExecutor.terminate();
            });
        });
        System.out.println("*****************************");
        startSim(allNodes, convertSecondsToVTime(10*60));
        for (int j = 0; j < insOrder.size(); j++) {
            LocalNode node = allNodes[insOrder.get(j)];
            cset.addCounter(j + 1, node.counter);
        }
    }

    /**
     * 以下の実験を行う
     * ・Aノード挿入
     * ・以下を繰り返す
     *   ・Bノード故障
     *   ・Bノード挿入
     * @param factory
     * @param insOrder
     * @param doFail
     */
    private void insertFailRepeat(NodeFactory factory,
            InsertOrder insOrder, boolean doFail) {
        // 全ノード数
        int num = 1000;        // 最初に挿入するノード数
        int initial = 200;
        // 後で挿入するノード数
        int diff = 100;
        LocalNode[] nodes = new LocalNode[num];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }
        LocalNode introducer = nodes[0];
        introducer.joinInitialNode();

        // 乱数で選択した initial 個のノードを挿入
        ArrayList<Integer> rest = new ArrayList<>();
        for (int i = 1; i < num; i++) {
            rest.add(i);
        }
        Collections.shuffle(rest, random());
        ArrayList<Integer> inserted = new ArrayList<>();
        int base = 1;
        for (int i = base; i < initial; i++) {
            int index = rest.get(i);
            inserted.add(index);
            joinAsync(nodes[index], introducer, null);
        }
        base = initial;
        int T = 1000*1000;
        for (int j = 1; j < 10; j += 2) {
            for (int i = 0; i < diff; i++) {
                if (inserted.size() > 0) {
                    int r = random().nextInt(inserted.size());
                    int index = inserted.get(r);
                    inserted.remove(r);
                    long t = j * 10 * T;
                    System.out.println("@remove " + nodes[index] + " at " + t);
                    EventExecutor.sched(t, () -> {
                        //nodes[index].fail();
                        nodes[index].leaveAsync();
                    });
                }
            }
            for (int i = base; i < base + diff; i++) {
                int index = rest.get(i);
                long t = (j + 1) * 10 * T;
                System.out.println("@insert " + nodes[index] + " at " + t);
                inserted.add(index);
                joinLater(nodes[index], introducer, t, null);
            }
            base += diff;
        }

        System.out.println("*****************************");

        AllLookupStats all = new AllLookupStats();
        StatSet symset = new StatSet();
        // T 毎に lookupTest を実行
        int LOOKUP_TIMES = 80;
        if (true) for (int i = 1; i < LOOKUP_TIMES; i++) {
            long t = i * T + 2000;
            System.out.println("!!! " + i + "th loop start (" + t + ")");
            LookupStat s = all.getLookupStat(i);
            EventExecutor.sched(t, lookupTest(nodes, s));
            Stat as = symset.getStat(i);
            EventExecutor.sched(t, () -> symmetricDegree(nodes, as));
        }

        startSim(nodes, T * LOOKUP_TIMES);
        System.out.println("*****************************");
        dump(nodes);
        System.out.println("*****************************");
        all.hopSet.printBasicStat("hops");
        all.failSet.printBasicStat("lookupFails");
        all.failedNodeSet.printBasicStat("encounterFailedNodes");

        //all.hopSet.getStat(1).outputFreqDist("dist-1", 1, false);
        //all.hopSet.getStat(LOOKUP_TIMES - 1).outputFreqDist("dist-last", 1, false);

        symset.printBasicStat("symmetric");
    }

    
    /**
     * ノード数を増やしながら検索ホップ数を測定
     * 
     * @param name
     * @param factory
     */
    private void lookupVaryingN(NodeFactory factory) {
        //final int NSTART = 0; // 最小ノード数
        final int NEND = 5000; // 最大ノード数 +1 
        final long DELTA = 10*1000L;
        //int M = ((NEND - NSTART) / STEP);
        LocalNode[] nodes = new LocalNode[NEND];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }
        nodes[0].joinInitialNode();
        List<Integer> order = new ArrayList<>();
        IntStream.range(1, NEND).forEach(order::add);
        Collections.shuffle(order, random());
        for (int i = 0; i < order.size(); i++) {
            joinLater(nodes[order.get(i)], nodes[0], i * DELTA, null);
        }
        AllLookupStats all = new AllLookupStats();
        // すべてのノードを挿入するのにDELTA*NEND時間かかる
        int DIV = 20; // 分割数
        int STEP = NEND / DIV;
        for (int i = 1; i <= DIV; i++) {
            // T = 1STEP * DELTA, 2STEP * DELTA, ... ごとに
            // x = 1DELTA, 2DELTA, ... として統計を取得
            int n = STEP * i;     // # of nodes
            long t = n * DELTA;
            System.out.println("!!! lookup + " + t);
            LookupStat s = all.getLookupStat(n);
            EventExecutor.sched(t, lookupTest(nodes, s));
        }
        //startSim(nodes, (M + 1) * 10 * T);
        startSim(nodes, DELTA * (NEND + 1));

        // collect msg counts
        StatSet msgs = new StatSet();
        for (int i = 0; i < order.size(); i++) {
            LocalNode node = nodes[order.get(i)];
            int joinmsgs = node.counter.get("join.lookup")
                    + node.counter.get("join.ddll")
                    + node.counter.get("join.ftupdate");
            Stat s = msgs.getStat(((i / STEP) + 1) * STEP);
            s.addSample(joinmsgs);
            System.out.println(nodes[order.get(i)] + ": " + joinmsgs + " msgs");
        }

        all.hopSet.printBasicStat("hops");
        msgs.printBasicStat("msgs");
        //dump(nodes);
    }

    /**
     * ノード数を増やしながら，挿入直後（Finger Table更新なし）の検索ホップ数を測定する．
     * 
     * @param name
     * @param factory
     */
    private void expNodesVsHops(NodeFactory factory) {
        SuzakuStrategy.UPDATE_FINGER_PERIOD.set(0);
        DdllStrategy.pingPeriod.set(0);
        AllLookupStats all = new AllLookupStats();
        Consumer<Integer> exp = (num) -> {
            EventExecutor.reset();
            LocalNode[] nodes = new LocalNode[num];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
            }
            nodes[0].joinInitialNode();
            insOrder.value().method.insert(this, nodes, 1, num, 0, 0, () -> {
                LookupStat s = all.getLookupStat(num);
                lookupTest(nodes, s, 4000).run();
            });
            startSim(nodes);
        };
        /*for (int i = 16; i <= 16384; i *= 4) {
            System.err.println("i=" + i);
            exp.accept(i);
        }*/
        exp.accept(numNodesOption.value());
        all.hopSet.printBasicStat("hops");
        all.hopSet.printCSV("hopsdata");
    }

    int cNode = 0;
    int iNode = 0;
    int dNode = 0;
    private void insertDelete(NodeFactory factory) {
        // 全ノード数
        int num = 256;
        LocalNode[] nodes = new LocalNode[num];
        boolean[] graceful = new boolean[num];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
            graceful[i]= random().nextDouble() > failRate.value();
        }
        nodes[0].joinInitialNode();
        long T = convertSecondsToVTime(60); // 1分
        for (int i = 1; i < nodes.length; i++) {
            long s, e;
            s = (long)(random().nextDouble() * 100 * T);
            double dur = exponentialDist(1.0/(aveLifeTime.value()));
            e = s + (long)dur;
            LocalNode x = nodes[i];
            int j = i;
            System.out.println(x + ": " + s + " to " + e + (
                    graceful[i] ? " graceful" : " ungraceful"));
            EventExecutor.sched(s, () -> {
                joinAsync(x, nodes[0], () -> {
                    cNode++;
                    iNode++;
                    EventExecutor.sched((long)dur, () -> {
                        cNode--;
                        dNode++;
                        if (graceful[j]){
                            x.leaveAsync();
                        } else {
                            x.fail();
                        }
                    });
                });
            });
        }

        int LOOKUP_TIMES = 200;
        AllLookupStats all = new AllLookupStats();
        StatSet msgset = new StatSet();
        StatSet symset = new StatSet();
        StatSet numStatSet = new StatSet();
        StatSet istatset = new StatSet();
        StatSet dstatset = new StatSet();
        for (int i = 1; i < LOOKUP_TIMES; i++) {
            long t = i * T;
            LookupStat s = all.getLookupStat(i);
            //EventDispatcher.sched(t, lookupTest(nodes, s));
            distLookupTest(nodes, s, t, (int)T);

            Stat stat = numStatSet.getStat(i);
            Stat istat = istatset.getStat(i);
            Stat dstat = dstatset.getStat(i);
            Stat as = symset.getStat(i);
            EventExecutor.sched(t, () -> {
                System.out.println("!!! lookup + " + t);
                stat.addSample(cNode);
                istat.addSample(iNode);
                dstat.addSample(dNode);
                symmetricDegree(nodes, as);
                dump(nodes);
            });
        }
        startSim(nodes, T * LOOKUP_TIMES);
        LookupStat unified = new LookupStat();
        for (int i = 1; i < LOOKUP_TIMES; i++) {
            LookupStat s = all.getLookupStat(i);
            unified.merge(s);
        }

        System.out.println("*****************************");
        dump(nodes);
        System.out.println("*****************************");
        all.hopSet.printBasicStat("hops");
        all.timeSet.printBasicStat("time");
        all.failSet.printBasicStat("lookupFails");
        all.failedNodeSet.printBasicStat("encounterFailedNodes");
        symset.printBasicStat("symmetric");
        
        all.timeSet.outputFreqDist("timefreq", 100);
        unified.time.outputFreqDist("unifiedTimeFreq", 100);

        numStatSet.printBasicStat("numNodes");
        istatset.printBasicStat("iNodes");
        dstatset.printBasicStat("dNodes");
    }

    /**
     * ノードN0~N63, N65~N127までを挿入し，その後 N64からすべてのノードを
     * 検索するときの検索時間を求める．
     */
    private void expHopsByDistance(NodeFactory factory) {
        // 全ノード数
        int num = 128;      // 最初に挿入するノード数
        LocalNode[] nodes = new LocalNode[num];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }
        nodes[0].joinInitialNode();
        int CENTER = num / 2;
        LocalNode[] nodes2 = new LocalNode[num];
        for (int i = 0; i < num - 1; i++) {
            int j = i < CENTER ? i : i + 1;
            nodes2[i] = nodes[j];
        }
        // 60秒ごとに検索
        long T = convertSecondsToVTime(60);
        int LOOKUP_TIMES = 41;
        AllLookupStats[] alls = new AllLookupStats[LOOKUP_TIMES];
        insOrder.value().method.insert(this, nodes2, 1, num - 1, 0, 0,
                () -> joinAsync(nodes[CENTER], nodes[0], () -> {
                    for (int i = 0; i < LOOKUP_TIMES; i++) {
                        alls[i] = new AllLookupStats();
                        long t = i * T;
                        int i0 = i;
                        EventExecutor.sched(t, () -> {
                            for (int j = 0; j < num; j++) {
                                LookupStat stat = alls[i0].getLookupStat(j);
                                lookup(nodes[CENTER], nodes[j].key, stat);
                            }
                        });
                    }
                }));

        startSim(nodes, T * (LOOKUP_TIMES + 10));
        System.out.println("*****************************");
        dump(nodes);
        System.out.println("*****************************");
        
        for (int i = 0; i < LOOKUP_TIMES; i++) {
            AllLookupStats all = alls[i];
            all.hopSet.printBasicStat("hops#" + i + "#");
        }
    }

    /**
     * Finger Table の距離の統計を取る
     * 
     * @param factory
     */
    private void expFtDistance(NodeFactory factory) {
        boolean pu2 = SuzakuStrategy.PASSIVE_UPDATE_2.value();
        boolean pu2bid = SuzakuStrategy.PASSIVE_UPDATE_2_BIDIRECTIONAL.value();
        String postfix = (pu2 ? ("pu2"
                + (pu2bid ? "-bid" : "")) : "nopu2");
        if (numIteration.value() > 1) {
            SuzakuStrategy.UPDATE_FINGER_PERIOD.set(60*1000);
        } else {
            SuzakuStrategy.UPDATE_FINGER_PERIOD.set(0);
        }
        DdllStrategy.pingPeriod.set(0);

        StatSet n0stats = new StatSet();
        List<StatSet> allstats = new ArrayList<>();
        Stat ins = new Stat();
        int ITER = 1;
        for (int i = 0; i < ITER; i++) {
            System.err.println("!!! iteration " + (i + 1) + "/" + ITER);
            expFtDistance0(factory, n0stats, allstats, ins);
        }
        
        n0stats.printBasicStat("fft-dist-n0-" + postfix);
        ins.outputFreqDist("fft-initial-fft1-dist-" + postfix, 1);
        for (int i = 0; i < allstats.size(); i++) {
            allstats.get(i).printCSV("fftdist-" + i);
        }
    }

    private void expFtDistance0(NodeFactory factory, StatSet n0stats,
            List<StatSet> allstats, Stat ins) {
        EventExecutor.reset();
        int num = numNodesOption.value(); // 400

        LocalNode[] nodes = new LocalNode[num];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }
        nodes[0].joinInitialNode();

        Consumer<Integer> run = (ith) -> {
            StatSet s;
            if (ith < allstats.size()) {
                s = allstats.get(ith);
            } else {
                s = new StatSet();
                allstats.add(s);
                assert allstats.size() == ith + 1;
            }
            for (int i = 0; i < num; i++) {
                SuzakuStrategy t = SuzakuStrategy.getSuzakuStrategy(nodes[i]);
                // 各ノードのFFTの，レベルごとの距離の統計
                for (int lv = 1; lv < t.getFingerTableSize(); lv++) {
                    FTEntry ent = t.getFingerTableEntry(lv);
                    if (ent == null) {
                        System.out.println("ent=null!\n" + nodes[i].toStringDetail());
                        continue;
                    }
                    int key = (Integer)ent.getNode().key.getRawKey() / 10;
                    int distance = key - (Integer)nodes[i].key.getRawKey() / 10;
                    if (distance < 0) {
                        distance = (distance + num) % num;
                    }
                    s.getStat(lv).addSample(distance);
                    if (distance == 0) {
                        System.out.println("ZeroDist! " + lv + "\n" + nodes[i].toStringDetail());
                    }
                }
            }
        };
        
        insertRandom(nodes, 1, num, 0, 0, () -> {
            // 最後のノードのFinger Table構築完了を待って終了
            int iter = numIteration.value();
            for (int i = 0; i < iter; i++) {
                int i0 = i;
                EventExecutor.sched((i+1)*60*1000, () -> {
                    run.accept(i0);
                    if (i0 == iter - 1) {
                        EventExecutor.terminate();
                    }
                });
                
            }
        }, (node) -> {
            EventExecutor.sched(NetworkParams.ONEWAY_DELAY * 7, () -> {
                SuzakuStrategy sz = SuzakuStrategy.getSuzakuStrategy(node);
                FTEntry ent = sz.getFingerTableEntry(1);
                if (ent != null) {
                    Node remote = ent.getNode();
                    int distance = ((int)(remote.key.getRawKey())
                            - (int)node.key.getRawKey()) / 10;
                    if (distance < 0) {
                        distance += num;
                    }
                    ins.addSample(distance);
                }
            });
        });
        EventExecutor.startSimulation(0);

        // N0のFFTの，レベルごとの距離の統計
        {
            SuzakuStrategy s = SuzakuStrategy.getSuzakuStrategy(nodes[0]);
            System.out.println(s.toStringDetail());
            for (int i = 1; i < s.getFingerTableSize(); i++) {
                FTEntry ent = s.getFingerTableEntry(i);
                int key = (Integer)ent.getNode().key.getRawKey() / 10;
                Stat stat = n0stats.getStat(i);
                stat.addSample(key);
            }
        }
    }

    
    /*
     * Poisson distribution
     * 
     * http://www.ishikawa-lab.com/montecarlo/4shou.html
     */
    private static int poisson(double lambda)  {
        double xp;
        int k = 0;
        xp = Math.random();
        while (xp >= Math.exp(-lambda)) {
            xp = xp * Math.random();
            k = k + 1;
        }
        return k;
    }

    /**
     * 指数分布に従う乱数を返す
     */
    private static double exponentialDist(double lambda) {
        return -Math.log(1.0 - random().nextDouble()) / lambda;
    }

    /**
     * @param factory
     * @param insOrder
     * @param doFail
     */
    private void permutation(NodeFactory factory) {
        // 全ノード数
        int num = 9;//100;
        // 順列を生成するノード数
        int initial = 9;
        List<Integer> order = new ArrayList<>();
        for (int i = 1; i < initial; i++) {
            order.add(i);
        }
        LookupStat merged = new LookupStat();
        AllLookupStats all = new AllLookupStats();
        long permutations = Permutations.factorial(order.size());
        LongStream.range(0, permutations).forEachOrdered(x -> {
            LookupStat s = all.getLookupStat((int)x);
            List<Integer> p = Permutations.permutation(x, order);
            System.err.println(x + ": ORDER " + p);
            System.out.println(x + ": ORDER " + p);
            specificOrder(factory, num, p, s);
            merged.merge(s);
        });
        System.out.println("*****************************");
        all.hopSet.printBasicStat("hops");
        merged.hops.printBasicStat("mergedHops", 0);
        merged.hops.outputFreqDist("mergedDist", 1);
    }

    /**
     * 順列pで指定された順序でノードを挿入する．
     * 
     * @param factory
     * @param num
     * @param p
     * @param s
     */
    private void specificOrder(NodeFactory factory, int num, List<Integer> p,
            LookupStat s) {
        long T = 1000*1000;
        EventExecutor.reset();
        LocalNode[] nodes = new LocalNode[num];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = createNode(factory, i * 10, NetworkParams.HALFWAY_DELAY);
        }
        nodes[0].joinInitialNode();
        // num = 8
        // nodes: 0  1  2  3  4  5  6  7
        //        ^           r
        // p: a permutation of [1 2 3]
        int r = p.size() + 1;
        //insertSeqLR(nodes, r, num, T, T, null);
        insertSeq(nodes, p, 0, 0, convertSecondsToVTime(1),
                () -> {
                    EventExecutor.sched(convertSecondsToVTime(1),
                            lookupTestFull(nodes, 0, r, s));
                });
        //EventDispatcher.sched(999*T, () -> dump(nodes));
        //EventDispatcher.sched(1000*T, lookupTestFull(nodes, 0, r, s));
        startSim(nodes, 2000*T);
    }

    private void specificOrder(NodeFactory factory) {
        // 全ノード数
        int num = 100;
        // 順列を生成するノード数
        //int [] order = {2, 1, 3, 5, 4, 6};
        // lookup done: [N0, N10, N30, N40, N60, N0] (4 hops)
        //int [] order = {2, 1, 3, 5, 4, 6, 8, 7, 9};
        //lookup done: [N0, N10, N30, N40, N60, N70, N90, N0] (6 hops)
        int [] order = {2, 1, 3, 5, 4, 6, 8, 7, 9, 11, 10, 12};
        //lookup done: [N0, N10, N30, N40, N60, N70, N90, N100, N120, N0] (12 hops)
        List<Integer> p = new ArrayList<>();
        IntStream.of(order).forEach(p::add);
        LookupStat s = new LookupStat();
        specificOrder(factory, num, p, s);
        s.hops.printBasicStat("hops", order.length);
        s.hops.outputFreqDist("hopdist", 1);
    }

    private void concurrentJoin(NodeFactory factory) {
        final int NSTART = 0; // 最小ノード数
        final int NEND = 100; // 最大ノード数
        final int STEP = 5; // ノード数のステップ
        int M = ((NEND - NSTART) / STEP) + 1;
        final int ITER = 50; // 繰り返し数 
        StatSet mstats = new StatSet();
        StatSet tstats = new StatSet();
        StatSet istats = new StatSet();
        int n = NSTART;
        for (int j = 0; j < M; j++, n += STEP) {
            System.err.print("[" + n + "]");
            Stat mstat = mstats.getStat(n);
            Stat tstat = tstats.getStat(n);
            Stat istat = istats.getStat(n);
            EventExecutor.resetMessageCounters();
            for (int i = 0; i < ITER; i++) {
                System.out.println("** Simulation Start: " + n
                        + " nodes ***************************");
                System.err.print(" " + i);
                mixedLatencyExp(factory, n, mstat, tstat, istat);
            }
            System.err.println();
            /*msg.printBasicStat(n);
            msg.outputFreqDist("msg", 10, false);
            time.printBasicStat(n); 
            time.outputFreqDist("time", 100, false);*/
            EventExecutor.dumpMessageCounters();
        }
        String name = factory.toString();
        istats.printBasicStat("insert:" + name);
        mstats.printBasicStat("msg:" + name);
        tstats.printBasicStat("time:" + name);
    }

    /**
     * 
     * @param factory
     * @param n
     * @param msg       メッセージ数を格納するためのStat
     * @param time      全ノードの挿入時間を格納するためのStat
     * @param insert    各ノードの挿入時間を格納するためのStat
     */
    private void mixedLatencyExp(NodeFactory factory, int n, Stat msg, Stat time,
            Stat insert) {
        int MINID = 0;
        int MAXID = 1000;
        EventExecutor.reset();
        Set<Integer> iset = new HashSet<Integer>();
        iset.add(MINID);
        iset.add(MAXID);
        int nSlowNodes = (int) (n * slowNodeRatio.value());
        LocalNode[] nodes = new LocalNode[n];
        for (int i = 0; i < n; i++) {
            int r;
            do {
                r = random().nextInt(MAXID);
            } while (iset.contains(r));
            iset.add(r);
            if (i < nSlowNodes) {
                // narrow band node
                nodes[i] = createNode(factory, r, 200);
            } else {
                // broad band node
                nodes[i] = createNode(factory, r, 50);
            }
        }
        if (n == 0) {
            insert.addSample(0);    // fake
        } else {
            LocalNode introducer = nodes[0];//createNode(factory, MINID);
            introducer.joinInitialNode();
            for (int i = 1; i < n; i++) {
                LocalNode x = nodes[i];
                joinAsync(x, introducer, () -> {
                    insert.addSample(x.getInsertionTime());
                });
            }
        }
        Arrays.sort(nodes);
        dump(nodes);
        long start = EventExecutor.getVTime();
        if (factory instanceof ChordNodeFactory) {
            do {
                startSim(nodes, 1000); // 1sec
            } while (!Sim.isFinished());
        } else {
            startSim(nodes);
        }
        dump(nodes);
        long end = EventExecutor.getVTime();
        System.out.println("start = " + start + ", end = " + end
                + ", elapsed = " + (end - start));
        System.out.println("#msg = " + EventExecutor.nmsgs);
        time.addSample(end - start);
        msg.addSample(EventExecutor.nmsgs);
        if (!isFinished()) {
            System.err.println("Inconsisntent!");
        }
        //dump(a);
        //EventDispatcher.dumpCounter();
    }

    /**
     * retrans timeを変化させ，適切な再送時間を求める．
     *  
     * @param name
     * @param cons
     */
    private void retransTest(NodeFactory factory) {
        if (DdllStrategy.retryMode.value() != RetryMode.RANDOM) {
            System.err.println("specify -retrymode RANDOM");
            System.exit(1);
        }
        final int MAX = 25;
        final int ITER = 300;
        StatSet mstats = new StatSet();
        StatSet tstats = new StatSet();
        StatSet istats = new StatSet();
        int n = 50;
        int delay = 1;
        for (int j = 0; j < MAX; j++, delay += 1) {
            System.err.println("[" + j + "]");
            DdllStrategy.JOIN_RETRY_DELAY = delay;
            //AtomicRingStrategy.JOIN_RETRY_DELAY = delay;
            //CmrStrategy.JOIN_RETRY_DELAY = delay;
            Stat mstat = mstats.getStat(delay);
            Stat tstat = tstats.getStat(delay);
            Stat istat = istats.getStat(delay);
            EventExecutor.resetMessageCounters();
            for (int i = 0; i < ITER; i++) {
                System.out.println("** Simulation Start: " + n
                        + " nodes ***************************");
                mixedLatencyExp(factory, n, mstat, tstat, istat);
            }
            /*msg.printBasicStat(n);
            msg.outputFreqDist("msg", 10, false);
            time.printBasicStat(n); 
            time.outputFreqDist("time", 100, false);*/
            EventExecutor.dumpMessageCounters();
        }
        String name = factory.toString();
        mstats.printBasicStat("msg:" + name);
        tstats.printBasicStat("time:" + name);
    }

    /**
     * Latencyが混在する場合のテスト
     *  
     * @param name
     * @param cons
     */
    private void mixedLatencyTest(NodeFactory factory) {
        final int STEP = 5;
        final int MAX = (100 / STEP) + 1;
        final int ITER = 100;
        StatSet mstats = new StatSet();
        StatSet tstats = new StatSet();
        StatSet istats = new StatSet();
        int n = 50;         // # of nodes
        int r = 0;  // percentage of slow nodes
        for (int j = 0; j < MAX; j++, r += STEP) {
            System.err.println("[" + j + "]");
            slowNodeRatio.set(r / 100.0); 
            Stat mstat = mstats.getStat(r);
            Stat tstat = tstats.getStat(r);
            Stat istat = istats.getStat(r);
            EventExecutor.resetMessageCounters();
            for (int i = 0; i < ITER; i++) {
                System.out.println("** Simulation Start: " + n
                        + " nodes ***************************");
                mixedLatencyExp(factory, n, mstat, tstat, istat);
            }
            /*msg.printBasicStat(n);
            msg.outputFreqDist("msg", 10, false);
            time.printBasicStat(n); 
            time.outputFreqDist("time", 100, false);*/
            EventExecutor.dumpMessageCounters();
        }
        String name = factory.toString();
        istats.printBasicStat("ratio:" + name);
        
        istats.getStat(0).outputFreqDist("dist#0", 0.05);
        istats.getStat(STEP).outputFreqDist("dist#1", 0.05);
        istats.getStat(2*STEP).outputFreqDist("dist#2", 0.05);
        istats.getStat(3*STEP).outputFreqDist("dist#3", 0.05);
        istats.getStat(4*STEP).outputFreqDist("dist#4", 0.05);
        istats.getStat(5*STEP).outputFreqDist("dist#5", 0.05);

        mstats.printBasicStat("msg:" + name);
        tstats.printBasicStat("time:" + name);
    }

    public static boolean isFinished() {
        LocalNode[] sortedNodes = getNodes();
        LocalNode x = null;
        boolean rc = true;
        //System.out.println("start");
        for (int i = 0; i < sortedNodes.length; i++) {
            if (x == null) {
                x = sortedNodes[i];
            } else {
                LocalNode y = sortedNodes[i];
                if (x.succ != y) {
                    rc = false;
                    //System.out.println("  " + i + ": " + y.toStringDetail());
                    break;
                }
                if (y.pred != x) {
                    rc = false;
                    //System.out.println("  " + i + ": " + y.toStringDetail());
                    break;
                }
                x = y;
            }
            //System.out.println("  " + i + ": " + x.toStringDetail());
        }
        //System.out.println("finish: " + rc);
        /*if (!rc) {
            dump(sortedNodes[0]);
        }*/
        return rc;
    }

    /**
     * 経路表の対称性を調べる
     * @param nodes
     * @param s
     */
    public void symmetricDegree(LocalNode[] nodes, Stat s) {
//        for (int i = 0; i < nodes.length; i++) {
//            NodeImpl n = nodes[i];
//            if (n.mode == NodeMode.INSERTED
//                    && n.topStrategy instanceof SuzakuStrategy) {
//                SuzakuStrategy szk = (SuzakuStrategy)n.topStrategy;
//                //double degree = szk.symmetricDegree2(nodes);
//                double degree = szk.livenessDegree(nodes);
//                if (!Double.isNaN(degree)) {
//                    s.addSample(degree);
//                }
//            }
//        }
        //System.out.println("#symmetric-degree");
        //s.printBasicStat(nodes.length);
    }

    /**
     * メッセージ数 (FingerTable更新)
     * @param nodes
     * @param s
     */
    public void collectMessageCounts(LocalNode[] nodes, Stat s) {
        int c = EventExecutor.getCounter("GetFTEntEvent");
        s.addSample(c);
    }

    /**
     * lookup statistics for various number of nodes
     */
    public static class AllLookupStats {
        final StatSet hopSet;
        final StatSet failedNodeSet;
        final StatSet failSet;
        final StatSet timeSet;

        public AllLookupStats() {
            this.hopSet = new StatSet();
            this.failedNodeSet = new StatSet();
            this.failSet = new StatSet();
            this.timeSet = new StatSet();
        }

        public LookupStat getLookupStat(int n) {
            return new LookupStat(this, n);
        }
    }

    /**
     * lookup statistics for a certain number of nodes
     */
    public static class LookupStat {
        public Stat hops;
        public Stat failedNodes;
        public Stat lookupFailure;
        public Stat time;

        public LookupStat() {
            this.hops = new Stat();
            this.failedNodes = new Stat();
            this.lookupFailure = new Stat();
            this.time = new Stat();
        }

        public LookupStat(AllLookupStats all, int n) {
            this.hops = all.hopSet.getStat(n);
            this.failedNodes = all.failedNodeSet.getStat(n);
            this.lookupFailure = all.failSet.getStat(n);
            this.time = all.timeSet.getStat(n);
        }

        public void merge(LookupStat another) {
            this.hops.addStat(another.hops);
            this.failedNodes.addStat(another.failedNodes);
            this.lookupFailure.addStat(another.lookupFailure);
            this.time.addStat(another.time);
        }
    }
}
