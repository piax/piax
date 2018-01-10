package org.piax.ayame.ov.suzaku;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.piax.ayame.Event.Lookup;
import org.piax.ayame.Event.LookupDone;
import org.piax.ayame.Event.TimerEvent;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.FTEntry;
import org.piax.ayame.LocalNode;
import org.piax.ayame.NetworkParams;
import org.piax.ayame.Node;
import org.piax.ayame.Node.NodeMode;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.NodeStrategy;
import org.piax.ayame.ov.ddll.DdllEvent.SetRJob;
import org.piax.ayame.ov.ddll.DdllStrategy;
import org.piax.ayame.ov.suzaku.SuzakuEvent.FTEntRemoveEvent;
import org.piax.ayame.ov.suzaku.SuzakuEvent.FTEntUpdateEvent;
import org.piax.ayame.ov.suzaku.SuzakuEvent.GetEntReply;
import org.piax.ayame.ov.suzaku.SuzakuEvent.GetEntRequest;
import org.piax.ayame.ov.suzaku.SuzakuEvent.GetFTAllRequest;
import org.piax.ayame.ov.suzaku.SuzakuEvent.RemoveReversePointerEvent;
import org.piax.common.Option.BooleanOption;
import org.piax.common.Option.IntegerOption;
import org.piax.util.FlexibleArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Suzaku, a churn resilient and lookup-efficient
 * key-order preserving structured overlay network. 
 * This implementation also supports Chord#.
 * 
 * <p>The details of the algorithm is described in the following paper.
 * (English paper is in preparation)
 * 
 * <blockquote>
 * 安倍広多，寺西裕一: "高いChurn耐性と検索性能を持つキー順序保存型構造化オーバレイ
 * ネットワークSuzakuの提案と評価"，信学技報 Vol. 116, No. 362 (IA2016-65)，
 * pp. 11--16，(2016-12)．(in Japanese)
 * </blockquote>
 */
public class SuzakuStrategy extends NodeStrategy {
    static final Logger logger = LoggerFactory.getLogger(SuzakuStrategy.class);
    public static class SuzakuNodeFactory extends NodeFactory {
        public SuzakuNodeFactory(int type) {
            switch (type) {
            case 0: // Chord#
                SuzakuStrategy.USE_BFT = false;
                SuzakuStrategy.COPY_FINGERTABLES = true;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = false;
                SuzakuStrategy.PASSIVE_UPDATE_2.set(false);
                SuzakuStrategy.DELAYED_UPDATE = false;
                SuzakuStrategy.ZIGZAG_UPDATE = false;
                break;
            case 1: // Suzaku1: finger table双方向化
                SuzakuStrategy.USE_BFT = true;
                SuzakuStrategy.COPY_FINGERTABLES = true;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = false;
                SuzakuStrategy.PASSIVE_UPDATE_2.set(false);
                SuzakuStrategy.DELAYED_UPDATE = false;
                SuzakuStrategy.ZIGZAG_UPDATE = false;
                break;
            case 2: // Suzaku2: 挿入時に両側をアクティブに更新
                SuzakuStrategy.USE_BFT = true;
                SuzakuStrategy.COPY_FINGERTABLES = false;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = true;
                SuzakuStrategy.PASSIVE_UPDATE_2.set(false);
                SuzakuStrategy.DELAYED_UPDATE = false;
                SuzakuStrategy.ZIGZAG_UPDATE = false;
                break;
            case 3: // Suzaku3: ジグザグ，パッシブな更新2，遅延更新
                SuzakuStrategy.USE_BFT = true;
                SuzakuStrategy.COPY_FINGERTABLES = false;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = true;
                // SuzakuStrategy.PASSIVE_UPDATE_2.set(true);
                SuzakuStrategy.DELAYED_UPDATE = true;
                SuzakuStrategy.ZIGZAG_UPDATE = true;
                break;
            default:
                throw new Error("internal error");
            }
        }
        @Override
        public void setupNode(LocalNode node) {
            node.pushStrategy(new DdllStrategy());
            node.pushStrategy(new SuzakuStrategy());
        }
        @Override
        public String toString() {
            return "Suzaku";
        }
    }
    
    public static boolean USE_BFT = false;
    public static boolean DEBUG_REVPTR = false;
    // trueならば一周期ですべてのFinger Tableエントリを更新する
    public static BooleanOption UPDATE_ONCE
        = new BooleanOption(false, "-updateonce");
    public static IntegerOption UPDATE_FINGER_PERIOD
        = new IntegerOption(60 * 1000, "-ftperiod");
    public static BooleanOption NOTIFY_WITH_REVERSE_POINTER 
        = new BooleanOption(true, "-notify-rev", (val) -> {
            sanityCheck();
        });
    final static boolean P2U_EXPERIMENTAL = false; // currently buggy

    // parameter to compute the base of log
    public static final int BASE_DEFAULT = 1;
    /** the base of log. K = 2<sup>B</sup> */
    public static int K = 1 << BASE_DEFAULT;
    public static IntegerOption B = new IntegerOption(BASE_DEFAULT,
            "-base", val -> { K = 1 << val; });

    // Each FTEntry has backup nodes.  In default, Suzaku uses DDLL's left
    // neighbor set as the backup nodes.  Alternatively, you may use
    // FFT[0]..FFT[SUCCESSOR_LIST_SIZE-1] as the backup nodes.
    public static int SUCCESSOR_LIST_SIZE = 4;
    public static BooleanOption USE_SUCCESSOR_LIST
        = new BooleanOption(false, "-use-succlist", val -> {
            assert !val || SUCCESSOR_LIST_SIZE <= K;
        });

    private int forwardUpdateCount = 0;
    private int backwardUpdateCount = 0;

    /** 挿入時にpredecessorからfinger tableをコピーする */
    public static boolean COPY_FINGERTABLES = false;
    /** 挿入時にfinger tableをアクティブに更新 */
    public static boolean ACTIVE_UPDATE_ON_JOIN = false;
    /** パッシブ更新2 */
    public static BooleanOption PASSIVE_UPDATE_2
        = new BooleanOption(true, "-pu2");
    /** BFT方向でもパッシブ更新2を行う */
    public static BooleanOption PASSIVE_UPDATE_2_BIDIRECTIONAL
        = new BooleanOption(false, "-pu2-bid");
    /** 取得したエントリを生存が確認できるまで格納しない */
    public static boolean DELAYED_UPDATE = false;
    /** ジグザグに更新 */
    public static boolean ZIGZAG_UPDATE = false;
    /** あるFTEを更新する際に，取得してきたり後でパッシブ更新されていたら後者を優先する */
    public static boolean PREFER_NEWER_ENTRY_THAN_FETCHED_ONE = true;

    /** finger tables */
    FingerTables table;

    // 次にfinger tableを更新するレベル (デバッグ用)
    int nextLevel = 0;
    TimerEvent updateSchedEvent;

    DdllStrategy ddll;

    public static void load() {
        // dummy
    }

    private static void sanityCheck() {
    }

    public static SuzakuStrategy getSuzakuStrategy(LocalNode node) {
        return (SuzakuStrategy)node.getStrategy(SuzakuStrategy.class);
    }

    @Override
    public void activate(LocalNode node) {
        super.activate(node);
        ddll = (DdllStrategy)node.getLowerStrategy(this);
        table = new FingerTables(n);
    }

    @Override
    public void initInitialNode() {
        ddll.initInitialNode();
        // UPDATE_FINGER_PERIOD後に最初のFinger Table更新を行う際に
        // zigzag updateを行わないようにするため，forwardUpdateCount = 1 とする．
        forwardUpdateCount = 1;
        schedFFT1Update();
    }

    @Override
    public void join(LookupDone lookupDone, 
            CompletableFuture<Boolean> joinFuture) {
        logger.trace("Suzaku#join: {}, {} hops", lookupDone.route,
                lookupDone.hops()); 
        logger.trace("Suzaku#join: {} joins between {} and {}", n.key
                , lookupDone.pred, lookupDone.succ);
        assert Node.isOrdered(lookupDone.pred.key, n.key, lookupDone.succ.key);
        n.counter.add("join.lookup", lookupDone.hops());
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        ddll.join(lookupDone, future);
        future.whenComplete((rc, exc) -> {
            if (exc != null) {
                joinFuture.completeExceptionally(exc);
            } else if (rc) {
                // 右ノードが変更された契機でリバースポインタの不要なエントリを削除
                SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(n);
                szk.table.sanitizeRevPtrs();
                nodeInserted();
                joinFuture.complete(rc);
            } else {
                joinFuture.complete(rc);
            }
        });
    }

    public static class SuzakuSetRJob implements SetRJob {
        private final Set<Node> reversePointers;
        private final Node n;

        public SuzakuSetRJob(Node n, Set<Node> revPtrs) {
            this.n = n;
            this.reversePointers = revPtrs;
        }

        // この関数はSetRを受信したノードで動作する
        @Override
        public void run(LocalNode node) {
            // node: SetR受信ノード, n: SetR送信ノード

            // 右ノードが変更された契機でリバースポインタの不要なエントリを削除
            SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(node);
            szk.table.sanitizeRevPtrs();

            Set<Node> s = reversePointers;
            logger.debug("{}: receives revptr ({}) from {} : {}", node, s.size(), n, s);
            s.remove(n); // ノードnは既に削除済
            // nodeのfinger tableで，nをnodeに置き換える
            List<Node> neighbors = szk.getNeighbors();
            neighbors.add(0, node);
            szk.removeFromFingerTable(n, neighbors);
            s.forEach(x -> {
                // x が区間 [node, n] に含まれている場合，不要なエントリなので無視．
                // (そのような x は削除済みであるため)
                if (!Node.isOrdered(node.key, x.key, n.key)) {
                    // change entry: n -> neighbors
                    node.post(new FTEntRemoveEvent(x, n, neighbors));
                    szk.table.addReversePointer(x);
                } else {
                    if (DEBUG_REVPTR) {
                        logger.debug("{}: filtered {}", node, x);
                    }
                }
            });
        }
    }

    @Override
    public void leave(CompletableFuture<Boolean> leaveComplete) {
        logger.trace("leave {}", n);
        // jobはSetRが成功した場合に左ノード上で実行される
        SetRJob job;
        if (NOTIFY_WITH_REVERSE_POINTER.value()) {
            job = new SuzakuSetRJob(n, table.reversePointers);
        } else {
            job = null;
        }
        logger.debug("{}: start DDLL deletion", n);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        ddll.leave(future, job);
        future.whenComplete((rc, exc) -> {
            assert rc;
            // SetRAckを受信した場合の処理
            n.mode = NodeMode.GRACE;
            logger.debug("{}: mode=grace", n);
            if (updateSchedEvent != null) {
                logger.debug("{}: remove schedEvent: {}", n, updateSchedEvent.getEventId());
                EventExecutor.cancelEvent(updateSchedEvent);
                updateSchedEvent = null;
            }
            EventExecutor.sched(NetworkParams.ONEWAY_DELAY, () -> {
                n.mode = NodeMode.DELETED;
                logger.debug("{}: mode=deleted", n);
                leaveComplete.complete(true);
            });
        });
    }

    /**
     * リモートノードが削除された場合に送信されるFTEntRemoveEventメッセージの処理
     * 
     * @param r 削除ノード
     * @param neighbor 削除ノードの代替ノード
     */
    void removeFromFingerTable(Node r, List<Node> neighbors) {
        assert r != n;
        table.removeReversePointer(r);
        FTEntry repl;
        if (neighbors.get(0) == n) {
            logger.debug("{}: removeFromFingerTable: self!", n);
            // XXX: 論文に入っていない修正 
            // 代替ノードが自分自身ならば，successorに付け替える．
            repl = getFingerTableEntry(0);
        } else {
            repl = new FTEntry(neighbors);
        }
        table.replace(r, repl);
    }

    @Override
    public void handleLookup(Lookup l) {
        // isInserted() check is for the case where this node has not
        // yet been inserted.
        if (ddll.isInserted()) {
            handleLookup(l, 0);
        }
    }

    public void handleLookup(Lookup l, int nRetry) {
        if (l.fill) {
            FTEntry ent = getFTEntryToSendTop(0, 1);
            logger.debug("handleLookup: {} sends FTEntUpdateEvent: ", n, ent);
            n.post(new FTEntUpdateEvent(l.sender, ent));
        }
        if (isResponsible(l.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            Node next = n.getClosestPredecessor(l.key);
            // successorがfailしていると，next.node == n になる
            if (next == null || next == n) {
                logger.debug("{}: handleLookup! key={}, evid={}, next={}, {}",
                        n, l.key, l.getEventId(), next, toStringDetail());
                //System.out.println(n.toStringDetail());
                n.post(new LookupDone(l, n, n.succ));
                return;
            }
            assert next != n;
            l.delay = Node.NETWORK_LATENCY;
            {
                FTEntry ent = table.getFTEntry(next);
                // multi-keyの場合，上のn.getClosestPredecessor(l.key)は他のkeyの
                // FTEを用いた結果を返す可能性があり，その場合 ent == null となる．
                if (ent != null) {
                    l.fill = ent.needUpdate();
                }
            }
            //logger.debug("T={}: {}: handleLookup evid={} next={}", EventExecutor.getVTime(), n, l.getEventId(), next);
            n.forward(next, l, (exc) -> {
                /* 
                 * 相手ノード障害時は，handleLookupを再実行する．．
                 * getClosestPredecessorは障害ノード集合を取り除いて再送する．
                 */
                FTEntry ent = table.getFTEntry(next);
                logger.debug("TIMEOUT: {} sent a query to {}, ftent = {}\n{}\n{}",
                        n, next.key,  ent,
                        n.toStringDetail(), 
                        next.toStringDetail());
                if (next == n.pred) {
                    CompletableFuture<Boolean> future = ddll.checkAndFix();
                    future.thenRun(() -> {
                        handleLookup(l, nRetry + 1);
                    });
                } else {
                    handleLookup(l, nRetry + 1);
                }
            });
        }
    }

    // handles FTEntUpdateEvent
    public void updateFTEntry(FTEntUpdateEvent event) {
        table.replace(event.sender, event.ent);
    }

    @Override
    public String toStringDetail() {
        StringBuilder buf = new StringBuilder();
        buf.append("|mode: " + n.mode).append("\n");
        buf.append("|ddll: ");
        buf.append(ddll.toStringDetail()).append("\n");
        buf.append("|FFT update: " + forwardUpdateCount
                + ", BFT update: " + backwardUpdateCount
                + ", nextLevel: " + nextLevel + "\n");
        if (NOTIFY_WITH_REVERSE_POINTER.value()) {
            buf.append("|reverse: " + table.reversePointers + "\n");
        }
        //buf.append("predecessors: " + getPredecessorList());
        //buf.append("|successors: " + getSuccessorList()).append("\n");
        int fftsize = getFingerTableSize();
        FlexibleArray<String> left = new FlexibleArray<String>(-1);
        int MIN = FingerTable.LOCALINDEX;
        int lmax = 1;
        for (int i = MIN; i < fftsize; i++) {
            FTEntry ent = getFingerTableEntry(i);
            left.set(i, ent == null ? "" : ent.toString());
            lmax = Math.max(lmax, left.get(i).length());
        }
        int bftsize = USE_BFT ? getBackwardFingerTableSize() : 0;
        FlexibleArray<String> right = new FlexibleArray<String>(-1);
        right.set(-1, "");
        for (int i = 0; i < bftsize; i++) {
            FTEntry ent = getBackwardFingerTableEntry(i);
            right.set(i, ent == null ? "" : ent.toString());
        }
        String fmt0 = "|  |%-" + lmax + "s|%s\n";
        String fmt1 = "|%2d|%-" + lmax + "s|%s\n";
        buf.append(String.format(fmt0, "Forward", "Backward"));
        for (int i = MIN; i < Math.max(fftsize, bftsize); i++) {
            String l = i < fftsize ? left.get(i) : "";
            String r = i < bftsize ? right.get(i) : "";
            buf.append(String.format(fmt1, i, l, r));
        }
        //List<NodeWithEntry> tmp = routingEntryStream().collect(Collectors.toList());
        //buf.append("STREAM=").append(tmp).append("\n");
        return buf.toString();
    }

    private void nodeInserted() {
        if (COPY_FINGERTABLES) {
            // copy predecessor's finger table
            GetFTAllRequest ev = new GetFTAllRequest(n.pred);
            ev.onReply((rep, exc) -> {
                if (exc != null) {
                    logger.debug("getFTAll failed: {}", exc);
                } else {
                    n.counter.add("join.ftupdate", 2); // GetFTAllEvent/Reply
                    FTEntry[][] fts = rep.ents;
                    for (int i = 1; i < fts[0].length; i++) {
                        table.forward.set(i, fts[0][i]);
                    }
                    for (int i = 1; USE_BFT && i < fts[1].length; i++) {
                        table.backward.set(i, fts[1][i]);
                    }
                    initialFTUpdate();
                }
            });
            n.post(ev);
        } else {
            initialFTUpdate();
        }
    }

    @Override
    public List<FTEntry> getRoutingEntries() {
        return getValidFTEntries();
    }

    /**
     * get valid finger table entries from all inserted nodes
     * 
     * @return list of finger table entries
     */
    private List<FTEntry> getValidFTEntries() {
        //logger.debug("getValid: {}", this);
        List<FTEntry> rc = new ArrayList<>();
        List<SuzakuStrategy> vnodes = n.getSiblings().stream()
                .map(ln -> getSuzakuStrategy(ln))
                .collect(Collectors.toList());
        if (vnodes.isEmpty()) {
            return Collections.emptyList();
        }
        SuzakuStrategy v1 = vnodes.get(0);
        SuzakuStrategy v2;
        for (int k = 1; k <= vnodes.size(); k++, v1 = v2) {
            v2 = vnodes.get(k % vnodes.size());
            List<FTEntry> flist = new ArrayList<>();
            List<FTEntry> blist = new ArrayList<>();
            FTEntry me = v1.getFingerTableEntry(FingerTable.LOCALINDEX);
            flist.add(me);
            FTEntry fent = null;
            FTEntry bent = null;
            int fsz = v1.getFingerTableSize();
            int bsz = v2.getBackwardFingerTableSize();
            // forward ft と backward ft の両方を，0 番目のエントリから順番にスキャンし，
            // 両者が出会うところまで　flist と blist に登録していく．
            for (int i = 0; i < Math.max(fsz, bsz); i++) {
                boolean f = false, b = false;
                if (i < fsz) {
                    fent = v1.getFingerTableEntry(i);
                    f = true;
                }
                if (i < bsz) {
                    bent = v2.getBackwardFingerTableEntry(i);
                    b = true;
                }
                if ((fent != null)
                        && (bent != null)
                        && Node.isOrdered(v1.getLocalNode().key,
                                bent.getNode().key, fent.getNode().key)) {
                    if (f) {
                        FTEntry bprev;
                        // fentがbprevとbentの間に挟まれるならば，fentを採用
                        // FFT:         F8...F7...F6
                        // BFT: B6...B7....B8 
                        // さもなくば，fentは採用しない
                        // FFT:   F8.........F7...F6
                        // BFT: B6...B7....B8 
                        if (blist.size() > 0) {
                            bprev = blist.get(blist.size() - 1);
                        } else {
                            bprev = me;
                        }
                        if (Node.isOrdered(bent.getNode().key, fent.getNode().key,
                                bprev.getNode().key)) {
                            flist.add(fent);
                        }
                    }
                    break;
                }
                if (f && (fent != null)) {
                    flist.add(fent);
                }
                if (b && (bent != null)) {
                    blist.add(bent);
                }
            }
            Collections.reverse(blist);
            //            logger.debug("getValid: v1={}, v2={}, flist={}, blist={}",
            //                    v1.getKey(), v2.getKey(), flist, blist);
            rc.addAll(flist);
            rc.addAll(blist);
        }
        return rc;
    }

    private List<Node> getNeighbors() {
        List<Node> neighbors;
        if (USE_SUCCESSOR_LIST.value()) {
            neighbors = getSuccessorList();
        } else {
            neighbors = getPredecessorList();
        }
        return neighbors;
    }

    public List<Node> getSuccessorList() {
        return table.forward.stream()
                .filter(Objects::nonNull)
                .map(ent -> ent.getNode())  // extract Node from FTEntry 
                .distinct()
                .limit(SUCCESSOR_LIST_SIZE)
                .collect(Collectors.toList());
    }

    public List<Node> getPredecessorList() {
        List<Node> list = new ArrayList<Node>();
        List<Node> nbrs = ((DdllStrategy)ddll).leftNbrs.getNeighbors();
        list.addAll(nbrs);
        return list;
    }

    public int getFingerTableSize() {
        return table.forward.getFingerTableSize();
    }

    public int getBackwardFingerTableSize() {
        if (!USE_BFT) {
            return 0;
        }
        return table.backward.getFingerTableSize();
    }
    
    /**
     * handle getEntRequest
     * 
     * @param req   GetEntRequest
     * @return GetEntReply
     */
    GetEntReply getEnts(GetEntRequest req) {
        // finger table 上で，距離が tk^x (where 0 <= t <= 2^y)
        // 離れたエントリを取得する
        boolean isBackward = req.isBackward;
        int x = req.x;
        int y = req.y;
        FTEntrySet passive1 = req.passive1;
        FTEntrySet passive2 = req.passive2;
        FTEntrySet returnSet = new FTEntrySet();
        GetEntReply reply = new GetEntReply(req, returnSet);

        // {tk^x | 0 < t <= 2^y}
        // x = floor(p/b), y = p-bx = p % b
        // ---> p = y + bx 
        returnSet.ents = new FTEntry[(1 << y) + 1];
        // t = 0 represents the local node and
        // t > 0 represents finger table entries
        for (int t = 0; t <= (1 << y); t++) {
            int delta = (1 << (B.value() * x));
            int d = t * delta * (isBackward ? -1 : 1); // t*2^(Bx)
            int d2 = d + delta;
            FTEntry l = getFTEntryToSendTop(d, d2);
            returnSet.ents[t] = l;
        }
        if (USE_BFT) {
            int p = y + B.value() * x;
            int distance;
            if (p == 0) {
                distance = 1;
            } else {
                int delta = 1 << ((p - 1) / B.value() * B.value());
                distance = (1 << (p - 1)) + delta; 
            }
            FingerTable opTable = isBackward ? table.forward: table.backward;
            if (passive1.ents.length > 0) {
                // Passive Update 1
                int index2 = FingerTable.getFTIndex(distance);
                for (int i = 0; i < passive1.ents.length; i++) {
                    logger.trace("pasv1 index={}: {}", index2 + i, passive1.ents[i]);
                    boolean rc = opTable.change(index2 + i, passive1.ents[i], true);
                    if (rc) {
                        reply.msgCount++;
                    }
                }
            }
            if (passive2 != null && passive2.ents.length > 0) {
                assert PASSIVE_UPDATE_2.value();
                // Passive Update 2
                if (P2U_EXPERIMENTAL) {
                    int index2 = p == 0 ? 1 : FingerTable.getFTIndex(distance);
                    assert index2 != FingerTable.LOCALINDEX;
                    for (int i = 0; i < passive2.ents.length + (isBackward ? 0 : -1); i++) {
                        if (passive2.ents[i] != null) {
                            // logger.trace("pasv2 index={}: {}", index2 + i, passive2.ents[i]);
                            boolean rc = opTable.change(index2 + i, passive2.ents[i], true);
                            if (rc) {
                                reply.msgCount++;
                            }
                        }
                    }
                    if (!isBackward && p > 0) {
                        FTEntry last = passive2.ents[passive2.ents.length - 1];
                        if (last != null) {
                            table.addReversePointer(last.getNode());
                        }
                    }
                } else {
                    assert passive2.ents.length == 1;
                    FTEntry last = passive2.ents[0];
                    if (last != null) {
                        if (PASSIVE_UPDATE_2_BIDIRECTIONAL.value() || isBackward) {
                            int index = FingerTable.getFTIndex(1 << (p + 1));
                            // logger.debug("pasv2 index={}, {}", index, last);
                            boolean rc = opTable.change(index, last, true);
                            if (rc) {
                                reply.msgCount++;
                            }
                        } else {
                            table.addReversePointer(last.getNode());
                        }
                    }
                }
            }
        }
        return reply;
    }

    public FTEntry[][] getFingerTable() {
        FTEntry[][] rc = new FTEntry[2][];
        rc[0] = new FTEntry[getFingerTableSize()];
        for (int i = 0; i < getFingerTableSize(); i++) {
            int d = FingerTable.indexToDistance(i);
            FTEntry ent = getFTEntryToSendTop(d, d);
            rc[0][i] = ent;
        }
        if (USE_BFT) {
            rc[1] = new FTEntry[getBackwardFingerTableSize()];
            for (int i = 0; i < getBackwardFingerTableSize(); i++) {
                int d = FingerTable.indexToDistance(i);
                FTEntry ent = getFTEntryToSendTop(-d, -d);
                rc[1][i] = ent;
            }
        }
        return rc;
    }

    @Override
    public final FTEntry getFingerTableEntry(boolean isBackward, int index) {
        if (isBackward) {
            return getBackwardFingerTableEntry(index);
        } else {
            return getFingerTableEntry(index);
        }
    }

    public final FTEntry getFingerTableEntry(int index) {
        return table.forward.getFTEntry(index);
    }

    protected FTEntry getBackwardFingerTableEntry(int index) {
        return table.backward.getFTEntry(index);
    }

    public Stream<FTEntry> getFTEntryStream() {
        // XXX: no implementation
        return null;
    }

    private FTEntry getFTEntryToSendTop(int fromDist, int toDist) {
        return n.getTopStrategy().getFTEntryToSend(fromDist, toDist);
    }

    /**
     * get a specified FTEntry for giving to a remote node.
     * in aggregation chord#, the range [distance1, distance2) is used as the 
     * aggregation range.
     * 
     * @param distance1     distance to the entry
     * @param distance2     distance to the entry
     * @return the FTEntry
     */
    @Override
    public FTEntry getFTEntryToSend(int distance1, int distance2) {
        boolean isBackward = distance1 < 0;
        int index = FingerTable.getFTIndex(Math.abs(distance1));
        FTEntry ent = getFingerTableEntry(isBackward, index);
        if (ent == null) {
            return null;
        }
        if (index == FingerTable.LOCALINDEX) {
            List<Node> nbrs = new ArrayList<>();
            nbrs.add(ent.getNode());
            nbrs.addAll(getNeighbors());
            ent = new FTEntry(nbrs);
        } else {
            // clone it because the returned FTEntry will not be copied
            // in simulations.
            ent = ent.clone();
        }
        return ent;
    }

    /**
     * update the finger table immediately after insertion to level 0 ring is
     * finished.
     */
    private void initialFTUpdate() {
        if (!ACTIVE_UPDATE_ON_JOIN) {
            schedFFT1Update();
            return;
        }
        assert ZIGZAG_UPDATE;
        if (ZIGZAG_UPDATE) {
            // FFT->BFT->FFT...の順に更新
            updateFingerTable(false);
        } else {
            // FFTを更新
            updateFingerTable(false);
            if (USE_BFT) {
                // BFTを更新 (FFTの更新と並行して行う)
                updateFingerTable(true);
            }
        }
    }

    /**
     * schedule next update of FFT[1].
     */
    private void schedFFT1Update() {
        if (UPDATE_FINGER_PERIOD.value() == 0) {
            return;
        }
        long delay = UPDATE_FINGER_PERIOD.value();
        updateSchedEvent = EventExecutor.sched(delay, () -> {
            updateFingerTable(false);
        });
        logger.trace("{}: add schedEvent: {}", n, updateSchedEvent.getEventId());
    }

    /**
     * update xFT[1].
     *
     * @param isBackward
     */
    private void updateFingerTable(boolean isBackward) {
        updateFingerTable0(0, isBackward, null, null);
    }

    /**
     * update the finger table.
     * 
     * <p>nextEnt1が指すノードに問い合わせることで，
     * 自ノードNから時計回り方向(!isBackwardの場合)あるいは
     * 反時計回り方向(isBackwardの場合)に，2<sup>p</sup>個離れたノードQへのポインタを
     * 取得する．
     * <p>nextEnt1は同一方向で2<sup>p-1</sup>離れたエントリ
     * 
     * <pre>
     * isBackward = false:
     * 
     *                 |            |-----------2^p------->|
     *                 |<--2^(p-1)--|--2^(p-1)-->|         |
     *             nextEnt2<--------N-------->nextEnt1---->Q
     *
     * isBackward = true:
     * 
     *       |<-------2^p-----------|----------2^p-------->|
     *       |         |<--2^(p-1)--|                      |
     *       Q<-----nextEnt<--------N------------------>nextEnt2
     *
     * </pre>
     *
     * @param p      distance parameter
     * @param isBackward represents the direction of Q from N
     * @param next1  the previously fetched FTEntry
     * @param next2  次の更新でベースとなる反対方向のFTEntry．
     *               isBackward = false ならば，反時計回り方向に 2^<sup>p-1<sup>
     *               離れている．isBackward = true ならば，時計回り方向に
     *               2^<sup>p</sup>離れている．
     */
    private void updateFingerTable0(final int p, boolean isBackward,
            FTEntry next1, FTEntry next2) {
        nextLevel = p;
        boolean isFirst = isFirst(isBackward);
        int B = SuzakuStrategy.B.value();
        logger.trace("{}: updateFingerTable0 {}, p={}, {}, fcount={}, bcount={}, next1={}, next2={}", 
                EventExecutor.getVTime(), n.key, p, isBackward, forwardUpdateCount, 
                backwardUpdateCount, next1, next2);
        if (n.mode == NodeMode.OUT || n.mode == NodeMode.DELETED) {
            return;
        }
        int distQ = 1 << p;
        int indQ = FingerTable.getFTIndex(distQ);
        FTEntry baseEnt;
        FTEntry current = getFingerTableEntry(isBackward, indQ);
        if (next1 == null) {
            baseEnt = current;
        } else {
            if (current == null
                    || !DELAYED_UPDATE
                    || (PREFER_NEWER_ENTRY_THAN_FETCHED_ONE
                            && current.time < next1.time)) {
                baseEnt = next1;
            } else {
                logger.debug("use current: next1={} (T={}), cur={} (T={})",
                        next1, next1.time, current, current.time);
                baseEnt = current;
            }
        }
        if (baseEnt == null) {
            logger.debug("{}: null-entry-1, index = {}, {}", n, indQ, toStringDetail());
            schedNextLevel(p, isBackward, next2, null);
            return;
        }
        if (baseEnt.getNode() == null) {
            logger.debug("{}: null-entry-2, index = {}, {}", n, indQ, toStringDetail());
            schedNextLevel(p, isBackward, next2, null);
            return;
        }
        if (baseEnt.getNode() == n) {
            // FTEntryの先頭ノードが削除された場合に発生する可能性がある
            logger.debug("{}: self-pointing-entry, index ={}, {} ", n, 
                    indQ, toStringDetail());
            schedNextLevel(p, isBackward, next2, null);
            return;
        }
        // Passive Update 1 で送信するエントリを収集
        /* 
         * |-------------------------------> distQ = 2^p
         * |---------------> 2^(p - 1)
         * |--> delta
         * N==A==B==...====P===============Q
         * 
         * NがQに対してgetFingersを呼ぶ時，[A, B, ... P) を送る．
         * delta = N-A間の距離
         *       = K ^ ⌊(p - 1) / B⌋
         *       = 2 ^ (B * ⌊(p - 1) / B⌋)
         *       = 2 ^ (⌊(p - 1) / B⌋ * B)
         *
         * K = 4 (B = 2) の場合のN0の経路表:
         *                        (p-1)/B  delta=(K^⌊(p-1)/B⌋)
         *  N1  N2  N3  (p=0, 1)   N/A, 0      N/A, 1
         *  N4  N8 N12  (p=2, 3)     0, 1        1, 4
         * N16 N32 N48  (p=4, 5)     1, 2        4, 16
         */
        FTEntrySet passive1 = new FTEntrySet();
        int delta = 1, max = 1;
        if (p > 0) {
            delta = 1 << ((p - 1) / B * B);
            max = 1 << (p - 1);
        }
        {
            ArrayList<FTEntry> p1ents = new ArrayList<>();
            for (int d = 0; d < max; d += delta) {
                int dist1 = d * (isBackward ? -1 : 1);
                int dist2 = dist1 + delta;
                FTEntry e = getFTEntryToSendTop(dist1, dist2);
                p1ents.add(0, e);  // 逆順に格納
            }
            passive1.ents = p1ents.toArray(new FTEntry[p1ents.size()]);
        }
        // Passive Update 2で送信するエントリを収集
        FTEntrySet passive2 = null;
        if (PASSIVE_UPDATE_2.value() && isFirst) {
            List<FTEntry> p2ents = new ArrayList<>();
            if (P2U_EXPERIMENTAL) {
                // K > 2 の場合にバグがある
                for (int d = delta; isBackward ? d <= distQ : d < distQ; d += delta) {
                    int d0 = d * (isBackward ? 1 : -1);
                    int d1 = d0 + delta;
                    FTEntry e = getFTEntryToSendTop(d0, d1);
                    int dis = distQ - d;  // 当該エントリから Q までの距離
                    if (dis < K) {
                        // XXX: THINK!: remove neighbors part
                        if (e != null) {
                            e = new FTEntry(e.getNode());
                        }
                    }
                    p2ents.add(e);
                }
                if (p > 0 && !isBackward) {
                    // FFT側ノードのReverse Pointer更新用
                    p2ents.add(next2 != null ? next2.clone() : null);
                }
            } else {
                if (isBackward) {
                    int delta2 = 1 << (p / B * B);
                    int d0 = distQ;
                    int d1 = d0 + delta2;
                    FTEntry e = getFTEntryToSendTop(d0, d1);
                    // baseEntからみて一周以上回るエントリは送らない
                    if (e != null && e.getNode() != null
                            && baseEnt.getNode() != e.getNode()
                            && Node.isOrdered(baseEnt.getNode().key, false,
                            n.key, e.getNode().key, false)) {
                        p2ents.add(e);
                    } else {
                        //System.out.println("drop baseEnt=" + baseEnt);
                        //System.out.println("drop n=" + n.key);
                        //System.out.println("drop e=" + e);
                        p2ents.add(null);
                    }
                } else {
                    FTEntry e = next2;
                    // baseEntからみて一周以上回るエントリは送らない
                    if (e != null && e.getNode() != null
                            && baseEnt.getNode() != e.getNode()
                            && Node.isOrdered(e.getNode().key, false,
                            n.key, baseEnt.getNode().key, false)) {
                        p2ents.add(e);
                    } else {
                        p2ents.add(null);
                    }
                }
            }
            passive2 = new FTEntrySet();
            passive2.ents = p2ents.toArray(new FTEntry[p2ents.size()]);
        }
        // リモートノードからfinger tableエントリを取得
        // 取得するエントリを指定するパラメータ (論文参照)
        int x = p / B;
        int y = p - B * x; // = p % B
        // Q から，距離が t*k^x (0 <= t <= 2^y)離れたエントリを取得する．
        // y = p - B * (p / B)
        //   = (p - p以下のBの倍数で最大の数)
        //   = p % B
        // 例: B = 1, p = 0 -> x = 0, y = 0, ents=[0, 1] 
        //     B = 1, p = 1 -> x = 1, y = 0, ents=[0, 2]
        //     B = 1, p = 2 -> x = 2, y = 0, ents=[0, 4]
        //     B = 2, p = 0 -> x = 0, y = 0, ents=[0, 1]
        //     B = 2, p = 1 -> x = 0, y = 1, ents=[0, 1*4^0=1, 2*4^0=2] 
        //     B = 2, p = 2 -> x = 1, y = 0, ents=[0, 1*4^1=4]
        //     B = 2, p = 3 -> x = 1, y = 1, ents=[0, 1*4^1=4, 2*4^1=8]
        Node q = baseEnt.getNode();
        logger.trace("update: {}, {}, ent = {}, p ={}", n.key, isBackward,
                q.key, p);
        GetEntRequest ev = new GetEntRequest(q, isBackward, x, y, K, passive1, passive2);
        ev.onReply((repl, exc) -> {
            FingerTable tab = isBackward ? table.backward : table.forward;
            FingerTable opTab = isBackward ? table.forward : table.backward;
            if (exc != null) {
                logger.debug("{}: getFingerTable0: got exception on {}", n, q);
                n.addMaybeFailedNode(q);
                Runnable job = () -> {
                    List<Node> nodes = baseEnt.allNodes();
                    List<Node> altNodes = nodes.stream()
                        .filter(e -> !n.maybeFailedNodes.contains(e))
                        .collect(Collectors.toList());
                    if (!altNodes.isEmpty()) {
                        // we have a backup node
                        FTEntry altEnt = new FTEntry(altNodes);
                        logger.debug("{}: we have backup nodes, p={}, altEnt={}, continue", n, altEnt, p);
                        updateFingerTable0(p, isBackward, altEnt, next2);
                    } else {
                        // we have no backup node
                        if (p + 1 < tab.getFingerTableSize()) {
                            logger.debug("{}: No backup node, p={}, continue", n, p);
                            updateFingerTable0(p + 1, isBackward, null, null);
                        } else {
                            logger.debug("{}: No backup node, p={}, no continue", n, p);
                            schedNextLevel(p, isBackward, next2, null);
                        }
                    }
                };
                if (q == n.pred) {
                    // fix and retry
                    CompletableFuture<Boolean> future = ddll.checkAndFix();
                    future.thenRun(job);
                } else {
                    job.run();
                }
            } else {
                if (ACTIVE_UPDATE_ON_JOIN && isFirst) {
                    n.counter.add("join.ftupdate", 2 + repl.msgCount);
                    // getFTEntEvent/Reply + RemoveReversePointerEvent
                }
                FTEntry[] replEnts = repl.ent.ents;
                // 取得したエントリをfinger tableにセットする
                {
                    // the first entry (replEnts[0]) represents the sender of
                    // the reply.  we have confirmed the aliveness of the node.
                    FTEntry e = replEnts[0];
                    assert e.getNode() == q;
                    tab.change(indQ, e, indQ > 0);
                }
                // process other entries...
                FTEntry nextX = null;
                for (int m = 1; m < replEnts.length; m++) {
                    FTEntry e = replEnts[m];
                    if (e != null && e.getNode() == null) {
                        // 取得したエントリが null の場合，以前のエントリを使う
                        e = getFingerTableEntry(isBackward, indQ + 1);
                        logger.debug("{}: fetched null entry from {}, use old ent: ", n, q, e);
                        if (e == null || e.getNode() == null) {
                            break;
                        }
                    }
                    if (isCirculated(isBackward, q, e)) {
                        tab.shrink(indQ + m);
                        if (opTab != null) {
                            opTab.shrink(indQ + m);
                        }
                        break;
                    }
                    // System.out.println("m=" + m + ": e=" + e);
                    if (DELAYED_UPDATE) {
                        if (m != replEnts.length - 1) {
                            tab.change(indQ + m, e, true);
                        } else {
                            // 最後のエントリの格納は生存が確認できてから行う
                            // (次の更新でアクセスするので)
                            nextX = e; 
                        }
                    } else { // Chord# way
                        // 取得したエントリの格納は今行う
                        tab.change(indQ + m, e, true);
                        nextX = e;
                    }
                }
                // used when PREFER_NEWER_ENTRY_THAN_FETCHED_ONE
                if (nextX != null) {
                    nextX.time = EventExecutor.getVTime();
                }
                schedNextLevel(p, isBackward, next2, nextX);
            }
        });
        n.post(ev);
    }

    /**
     * schedule the next level finger table update
     *
     * @param p
     * @param isBackward
     * @param nextEnt2
     * @param nextEntX
     */
    private void schedNextLevel(int p, boolean isBackward,
            FTEntry nextEnt2, FTEntry nextEntX) {
        if (nextEnt2 == null && nextEntX == null) {
            logger.trace(
                    "{}: finger table update done: isBackward={}, p={}\n{}",
                    n, isBackward, p, n.toStringDetail());
            if (ZIGZAG_UPDATE || !isBackward) {
                forwardUpdateCount++;
                schedFFT1Update();
            } else {
                backwardUpdateCount++;
            }
            return;
        }
        boolean isFirst = isFirst(isBackward);
        if (ZIGZAG_UPDATE) {
            if (isFirst) {
                if (!isBackward) {
                    updateFingerTable0(p, true, nextEnt2, nextEntX);
                } else {
                    updateFingerTable0(p + 1, false, nextEnt2, nextEntX);
                }
            } else {
                // XXX: UPDATE_ONCE is ignored
                nextLevel = p + 1;
                logger.trace("nextLevel={}, nextEntX={}", nextLevel, nextEntX);
                EventExecutor.sched(UPDATE_FINGER_PERIOD.value(),
                        () -> updateFingerTable0(p + 1, isBackward, nextEntX, null));
            }
        } else {
            assert nextEnt2 == null;
            if (UPDATE_ONCE.value()) {
                updateFingerTable0(p + 1, isBackward, nextEntX, null);
            } else {
                EventExecutor.sched(UPDATE_FINGER_PERIOD.value(),
                        () -> updateFingerTable0(p + 1, isBackward, nextEntX, null));
            }
        }
    }

    /**
     * 自ノードのFinger Tableにpがなければ，pのreverse pointerからnを削除する．
     * @param p
     */
    void cleanRemoteRevPtr(Node p) {
        if (!USE_BFT) {
            return;
        }
        // check if we still have a link to p
        boolean found = false;
        for (int i = getFingerTableSize() - 1; i >= 0; i--) {
            FTEntry e = getFingerTableEntry(false, i);
            if (e != null && e.getNode() != null && e.getNode().key.compareTo(p.key) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            for (int i = getBackwardFingerTableSize() - 1; i >= 0; i--) {
                FTEntry e = getFingerTableEntry(true, i);
                if (e != null && e.getNode() != null && e.getNode().key.compareTo(p.key) == 0) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            n.post(new RemoveReversePointerEvent(p));
            if (DEBUG_REVPTR) {
                logger.debug("{}: removes {} from revPtr of {}", n, n, p);
            }
        }
    }

    private boolean isFirst(boolean isBackward) {
        boolean isFirst;
        if (ZIGZAG_UPDATE) {
            isFirst = forwardUpdateCount == 0;
        } else {
            isFirst = ((!isBackward && forwardUpdateCount == 0) 
                    || (isBackward && backwardUpdateCount == 0));
        }
        return isFirst;
    }

    /**
     * 一周したかどうかを判定
     * 
     * @param isBackward
     * @param origin 自ノード
     * @param fetchFrom FTE取得元ノード
     * @param fetched 取得したFTE
     * @return
     */
    private boolean isCirculated(boolean isBackward, Node fetchFrom, FTEntry fetched) {
        return (fetched == null
                || n.key.compareTo(fetched.getNode().key) == 0
                || isBackward && Node.isOrdered(fetched.getNode().key, true, n.key, fetchFrom.key, true))
                || (!isBackward && Node.isOrdered(fetchFrom.key, true, n.key, fetched.getNode().key, true));
    }

    /**
     * a class for sending/receiving finger table entries between nodes.
     */
    public static class FTEntrySet implements Serializable {
        private static final long serialVersionUID = 1L;
        FTEntry[] ents;

        @Override
        public String toString() {
            return "[ents=" + Arrays.deepToString(ents) + "]";
        }
    }
}
