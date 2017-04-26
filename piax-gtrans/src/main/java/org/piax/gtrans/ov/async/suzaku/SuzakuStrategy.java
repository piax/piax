package org.piax.gtrans.ov.async.suzaku;

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

import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.Event.TimerEvent;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.FTEntry;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.Node.NodeMode;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.async.Option.BooleanOption;
import org.piax.gtrans.async.Option.IntegerOption;
import org.piax.gtrans.ov.async.ddll.DdllEvent.SetRJob;
import org.piax.gtrans.ov.async.ddll.DdllStrategy;
import org.piax.gtrans.ov.async.suzaku.SuzakuEvent.FTEntRemoveEvent;
import org.piax.gtrans.ov.async.suzaku.SuzakuEvent.FTEntUpdateEvent;
import org.piax.gtrans.ov.async.suzaku.SuzakuEvent.GetFTAllEvent;
import org.piax.gtrans.ov.async.suzaku.SuzakuEvent.GetFTEntEvent;
import org.piax.gtrans.ov.async.suzaku.SuzakuEvent.RemoveReversePointerEvent;
import org.piax.gtrans.ov.ring.rq.FlexibleArray;

public class SuzakuStrategy extends NodeStrategy {
    public static class SuzakuNodeFactory extends NodeFactory {
        public SuzakuNodeFactory(int type) {
            switch (type) {
            case 0: // Chord#
                SuzakuStrategy.USE_BFT = false;
                SuzakuStrategy.COPY_FINGERTABLES = true;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = false;
                SuzakuStrategy.PASSIVE_UPDATE_2 = false;
                SuzakuStrategy.DELAY_ENTRY_UPDATE = false;
                SuzakuStrategy.ZIGZAG_UPDATE = false;
                break;
            case 1: // Suzaku1: finger table双方向化
                SuzakuStrategy.USE_BFT = true;
                SuzakuStrategy.COPY_FINGERTABLES = true;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = false;
                SuzakuStrategy.PASSIVE_UPDATE_2 = false;
                SuzakuStrategy.DELAY_ENTRY_UPDATE = false;
                SuzakuStrategy.ZIGZAG_UPDATE = false;
                break;
            case 2: // Suzaku2: 挿入時に両側をアクティブに更新
                SuzakuStrategy.USE_BFT = true;
                SuzakuStrategy.COPY_FINGERTABLES = false;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = true;
                SuzakuStrategy.PASSIVE_UPDATE_2 = false;
                SuzakuStrategy.DELAY_ENTRY_UPDATE = false;
                SuzakuStrategy.ZIGZAG_UPDATE = false;
                break;
            case 3: // Suzaku3: ジグザグ，パッシブな更新2，遅延更新
                SuzakuStrategy.USE_BFT = true;
                SuzakuStrategy.COPY_FINGERTABLES = false;
                SuzakuStrategy.ACTIVE_UPDATE_ON_JOIN = true;
                SuzakuStrategy.PASSIVE_UPDATE_2 = true;
                SuzakuStrategy.DELAY_ENTRY_UPDATE = true;
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
    public static boolean DBEUG_FT_UPDATES = false;
    public static boolean DEBUG_REVPTR = false;
    // trueならば一周期ですべてのFinger Tableエントリを更新する
    public static BooleanOption UPDATE_ONCE
        = new BooleanOption(false, "-updateonce");
    public static IntegerOption UPDATE_FINGER_PERIOD
        = new IntegerOption(1000 * 1000, "-ftperiod");
    public static BooleanOption NOTIFY_WITH_REVERSE_POINTER 
        = new BooleanOption(false, "-notify-rev", (val) -> {
            sanityCheck();
        });

    // parameter to compute the base of log
    /** the base of log. K = 2<sup>B</sup> */
    public static int K = 1 << 1;
    public static IntegerOption B = new IntegerOption(1, "-base", val -> {
        K = 1 << val;
    });
    
    // FTEntry内のバックアップノードの数
    public static int SUCCESSOR_LIST_SIZE = 0;
    public static BooleanOption USE_SUCCESSOR_LIST
        = new BooleanOption(false, "-use-succlist", val -> {
            assert !val || SUCCESSOR_LIST_SIZE <= K;
        });

    private boolean updatingFT = false;
    private int forwardUpdateCount = 0;
    private int backwardUpdateCount = 0;
    
    /** 挿入時にpredecessorからfinger tableをコピーする */
    public static boolean COPY_FINGERTABLES = false;
    /** 挿入時にfinger tableをアクティブに更新 */
    public static boolean ACTIVE_UPDATE_ON_JOIN = false;
    /** パッシブ更新2 */
    public static boolean PASSIVE_UPDATE_2 = false;
    /** 取得したエントリを生存が確認できるまで格納しない */
    public static boolean DELAY_ENTRY_UPDATE = false;
    /** ジグザグに更新 */
    public static boolean ZIGZAG_UPDATE = false;

    /** finger tables */
    FingerTables table;

    // 次にfinger tableを更新するレベル (デバッグ用)
    int nextLevel = 0;
    TimerEvent updateSchedEvent;

    DdllStrategy ddll;

    int joinMsgs = 0;
    
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
    
    public void setupLinkChangeListener(Node n) {
//        n.setLinkChangeEventHandler((prev, cur) -> {
//        }, (prev, cur) -> {
//        });
    }
    
    @Override
    public void initInitialNode() {
        ddll.initInitialNode();
        // FINGER_UPDATE_PERIOD後に最初のFinger Table更新を行う際に
        // zigzag updateを行わないようにするため，forwardUpdateCount = 1 とする．
        forwardUpdateCount = 1;
        scheduleFTUpdate(true);
    }

    @Override
    public void join(LookupDone lookupDone, 
            CompletableFuture<Boolean> joinFuture) {
        Log.verbose(() -> "Suzaku#join: " + lookupDone.route
                + ", " + lookupDone.hops() + " hops"); 
        Log.verbose(() -> "Suzaku#join: " + n.key + " joins between "
                + lookupDone.pred + " and " + lookupDone.succ);
        assert Node.isOrdered(lookupDone.pred.key, n.key, lookupDone.succ.key);
        joinMsgs += lookupDone.hops();
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

        @Override
        public void run(LocalNode node) {
            // このラムダ式は，SetRを受信したノードで動作することに注意!
            // node: SetR受信ノード, n: SetR送信ノード

            // 右ノードが変更された契機でリバースポインタの不要なエントリを削除
            SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(node);
            szk.table.sanitizeRevPtrs();

            Set<Node> s = reversePointers;
            System.out.println(node + ": receives revptr ("
                    + s.size() + ") from " + n + ": " + s);
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
                        System.out.println(node + ": filtered " + x);
                    }
                }
            });
        }
    }

    @Override
    public void leave(CompletableFuture<Boolean> leaveComplete) {
        Log.verbose(()-> "leave " + n);
        // jobはSetRが成功した場合に左ノード上で実行される
        SetRJob job;
        if (NOTIFY_WITH_REVERSE_POINTER.value()) {
            job = new SuzakuSetRJob(n, table.reversePointers);
        } else {
            job = null;
        }
        System.out.println(n + ": start DDLL deletion");
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        ddll.leave(future, job);
        future.whenComplete((rc, exc) -> {
            assert rc;
            // SetRAckを受信した場合の処理
            n.mode = NodeMode.GRACE;
            System.out.println(n + ": mode=grace");
            if (updateSchedEvent != null) {
                System.out.println(n + ": remove schedEvent: " + updateSchedEvent.getEventId());
                EventExecutor.cancelEvent(updateSchedEvent);
                updateSchedEvent = null;
            }
            EventExecutor.sched(NetworkParams.ONEWAY_DELAY, () -> {
                n.mode = NodeMode.DELETED;
                System.out.println(n + ": mode=deleted");
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
            System.out.println(n + ": removeFromFingerTable: self!");
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
        // for debugging!
        if (false) {
            if (l.route.size() > 50) {
                if (l.trace == null) {
                    l.trace = new StringBuilder();
                }
                l.trace.append("trace\n" + n.toStringDetail() + "\n");
            }
            if (l.route.size() > 80) {
                System.out.println("too many hops!");
                System.out.println(l);
                System.out.println(l.route);
                System.out.println(l.trace);
                System.exit(1);
            }
        }
        if (l.fill) {
            FTEntry ent = getFTEntryToSendTop(0, 1);
            System.out.println("handleLookup: "
                    + n + " sends FTEntUpdateEvent: " + ent);
            n.post(new FTEntUpdateEvent(l.sender, ent));
        }
        if (isResponsible(l.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            Node next = n.getClosestPredecessor(l.key);
            // successorがfailしていると，next.node == n になる
            if (next == null || next == n) {
                System.out.println(n + ": handleLookup! key=" + l.key + ", evid=" + l.getEventId() + ", next=" + next + ", " + toStringDetail());
                //System.out.println(n.toStringDetail());
                n.post(new LookupDone(l, n, n.succ));
                return;
            }
            assert next != n;
            l.delay = Node.NETWORK_LATENCY;
            {
                FTEntry ent = table.getFTEntry(next);
                if (ent == null) {
                    System.out.println("ent == null, next=" + next);
                    System.out.println(toStringDetail());
                    assert false;
                }
                l.fill = ent.needUpdate();
            }
            System.out.println("T=" + EventExecutor.getVTime() + ": " + n + ": handleLookup " + l.getEventId() + " " + next);
            n.forward(next, l, (exc) -> {
                /* 
                 * 相手ノード障害時は，handleLookupを再実行する．．
                 * getClosestPredecessorは障害ノード集合を取り除いて再送する．
                 */
                FTEntry ent = table.getFTEntry(next);
                System.out.println("TIMEOUT: " + n + " sent a query to "
                        + next.key
                        + ", ftent = " + ent + "\n"
                        + n.toStringDetail() 
                        + "\n" + next.toStringDetail());
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
        //FTEntry ent = table.getFTEntry(event.sender);
        //table.change(index.ftIndex, event.ent, true);
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

    public void nodeInserted() {
        joinMsgs += ddll.getMessages4Join();    // add messages consumed in DDLL 
        if (COPY_FINGERTABLES) {
            // copy predecessor's finger table
            GetFTAllEvent ev = new GetFTAllEvent(n.pred);
            ev.onReply((rep, exc) -> {
                if (exc != null) {
                    System.out.println("getFTAll failed: " + exc);
                } else {
                    joinMsgs += 2;
                    FTEntry[][] fts = rep.ents;
                    for (int i = 1; i < fts[0].length; i++) {
                        table.forward.set(i, fts[0][i]);
                    }
                    for (int i = 1; USE_BFT && i < fts[1].length; i++) {
                        table.backward.set(i, fts[1][i]);
                    }
                    //System.out.println(n + ": FT copied\n" + n.toStringDetail());
                    if (ACTIVE_UPDATE_ON_JOIN) {
                        initialFTUpdate();
                    } else {
                        scheduleFTUpdate(true);
                    }
                }
            });
            n.post(ev);
        } else {
            initialFTUpdate();
        }
    }

    /**
     * 定期的なfinger table更新
     * @param isFirst
     */
    public void scheduleFTUpdate(boolean isFirst) {
        if (UPDATE_FINGER_PERIOD.value() == 0) {
            return;
        }
        long delay;
        if (isFirst) {
            delay = EventExecutor.random().nextInt(UPDATE_FINGER_PERIOD.value());
        } else {
            delay = UPDATE_FINGER_PERIOD.value();
        }
        updateSchedEvent = EventExecutor.sched(delay, () -> {
            updateFingerTable(false);
        });
        Log.verbose(() -> n + ": add schedEvent: " + updateSchedEvent.getEventId());
    }
    
    /**
     * レベル0リング挿入直後のアクティブなfinger table更新処理
     */
    private void initialFTUpdate() {
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

    /*@SuppressWarnings("unused")
    private List<NodeAndIndex> sorted(int key) {
        Comparator<NodeAndIndex> comp = (NodeAndIndex a, NodeAndIndex b) -> {
            // aの方がkeyに近ければ正の数，bの方がkeyに近ければ負の数を返す
            int ax = a.node.key - key;
            int bx = b.node.key - key;
            if (ax == bx) {
                return 0;
            } else if (ax == 0) {
                return +1;
            } else if (bx == 0) {
                return -1;
            } else if (Integer.signum(ax) == Integer.signum(bx)) {
                return ax - bx;
            } else if (ax > 0) {
                return -1;
            } else {
                return +1;
            }
        };
        Comparator<NodeAndIndex> compx = (NodeAndIndex a, NodeAndIndex b) -> {
            int rc = comp.compare(a, b);
            System.out.println("compare " + a.node.key + " with " + b.node.key + " -> " + rc);
            return rc;
        };
        List<NodeAndIndex> nodes = getAllLinks2();
        Collections.sort(nodes, compx);
        return nodes;
    }*/
    
    public List<Node> getNeighbors() {
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

    // to be overridden
//    protected FTEntry getLocalFTEnetry() {
//        return new FTEntry(getLocalNode());
//    }

    public int getFingerTableSize() {
        return table.forward.getFingerTableSize();
    }

    public int getBackwardFingerTableSize() {
        return table.backward.getFingerTableSize();
    }
    
    /**
     * finger table 上で，距離が tk<sup>x</sup> (0 &lt;= t &lt;= 2<sup>y</sup>)
     * 離れたエントリを取得する．結果は 2<sup>y</sup>+1 要素の配列として返す． 
     * 
     * @param isBackward BFTを取得する場合はtrue
     * @param x     parameter
     * @param y     parameter
     * @param k     parameter
     * @param passive1 finger table entries used for passive update-1
     * @param passive2 finger table entries used for passive update 2
     * @return list of finger table entries
     */
    public FTEntrySet getFingers(boolean isBackward, int x, int y, int k,
            FTEntrySet passive1, FTEntrySet passive2) {
        FTEntrySet returnSet = new FTEntrySet();
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
            int delta = 1 << ((p - 1) / B.value() * B.value());
            if (p == 0) {
                delta = 1;
            }
            FingerTable opTable = isBackward ? table.forward: table.backward;
            if (passive1.ents.length > 0) {
                // Passive Update 1
                assert p >= 0;
                //int index = FingerTable.getFTIndex(1 << p);
                //opTable.change(index, passive1.ents[0], true);
                int index2 = FingerTable.getFTIndex(delta);
                for (int i = 0; i < passive1.ents.length; i++) {
                    System.out.println("pasv1 " + index2 + ", " + i + ": " + passive1.ents[i]);
                    opTable.change(index2 + i, passive1.ents[i], true);
                }
            }
            if (passive2 != null) {
                assert PASSIVE_UPDATE_2;
                // Passive Update 2
                int index2 = p == 0 ? 1 : FingerTable.getFTIndex((1 << (p - 1)) + delta);
                assert index2 != FingerTable.LOCALINDEX;
                for (int i = 0; i < passive2.ents.length + (isBackward ? 0 : -1); i++) {
                    if (passive2.ents[i] != null) {
                        System.out.println("pasv2 " + index2 + ", " + i + ": " + passive2.ents[i]);
                        opTable.change(index2 + i, passive2.ents[i], true);
                    }
                }
                if (!isBackward && p > 0 && passive2.ents.length > 0) {
                    // opTable = BFT
                    table.addReversePointer(passive2.ents[passive2.ents.length - 1].getNode());
                }
//                int index = FingerTable.getFTIndex(1 << (p + 1));
//                if (!NOTIFY_WITH_REVERSE_POINTER.value() || isBackward) {
//                    // opTable = FFT
//                    opTable.change(index, passive2, !isBackward);
//                    if (false) System.out.println/*Node.verbose*/(n.key
//                            + ": use passive2 (" + passive2 + "), index="
//                            + index + ", " + n.toStringDetail());
//                } else {
//                    table.addReversePointer(passive2.getNode());
//                }
            }
            /*System.out.printf(n.id + ": getFingers(x=%d, y=%d, k=%d, given=%s) returns %s\n",
                    x, y, k, given, set);
            System.out.printf(n.toStringDetail());*/
        }
        return returnSet;
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

    void updateFingerTable(boolean isBackward) {
        if (n.mode == NodeMode.OUT || n.mode == NodeMode.DELETED || updatingFT) {
            return;
        }
        updatingFT = true;
        Log.verbose(() -> "start finger table update: " + n.key
                + ", " + EventExecutor.getVTime());
        updateFingerTable0(0, isBackward, null, null);
        updatingFT = false;
    }

    /**
     * update the finger table.
     * 
     * 自ノードNから時計回り方向(!isBackwardの場合)あるいは反時計回り方向(isBackwardの場合)
     * に，2<sup>p</sup>個離れたノード Q に問い合わせる．
     * 
     * nextEnt1は同一方向で2<sup>p-1</sup>離れたエントリ．．
     * 
     * <pre>
     * isBackward = false:
     * 
     *                 |            |-----------2^p------->|
     *                 |<--2^(p-1)--|--2^(p-1)-->|         |
     *             nextEnt2<--------N-------->nextEnt1----->Q
     *
     * isBackward = true:
     * 
     *       |<-------2^p-----------|----------2^p-------->|
     *       |         |<--2^(p-1)--|                      |
     *       Q<-----nextEnt<--------N------------------>nextEnt2
     *
     * </pre>
     *
     * SUZAKU1: 最初の更新かどうかは特に関係ない．
     * SUZAKU2: 最初のForward側の更新が終わってからforwardUpdateCount == 1となる
     * 同様にBackward側の更新が終わってからbackwardUpdateCount == 1となる
     * SUZAKU3: 最初の更新が終わってからforwardUpdateCount == 1となる．
     *
     * @param p         distance parameter
     * @param isBackward direction of node N
     * @param nextEnt1  ベースとなるFTEntry
     * @param nextEnt2  次の更新でベースとなる反対方向のFTEntry．
     *                  isBackward = false ならば，反時計回り方向に 2^<sup>p-1<sup>
     *                  離れている．isBackward = true ならば，時計回り方向に
     *                  2^<sup>p</sup>離れている．
     */
    private void updateFingerTable0(final int p, boolean isBackward,
            FTEntry nextEnt1, FTEntry nextEnt2) {
        nextLevel = p;
        boolean isFirst = isFirst(isBackward);
        int B = SuzakuStrategy.B.value();
        Log.verbose(() -> EventExecutor.getVTime() + ": " 
                + "updateFingerTable0 " + n.key + ", p=" + p + ", " + isBackward
                + ", fcount=" + forwardUpdateCount
                + ", bcount=" + backwardUpdateCount
                + ", nextEnt1=" + nextEnt1
                + ", nextEnt2=" + nextEnt2);
        if (n.mode == NodeMode.OUT || n.mode == NodeMode.DELETED) {
            return;
        }
        int distQ = 1 << p;
        int indQ = FingerTable.getFTIndex(distQ);
        FTEntry baseEnt = (nextEnt1 != null
                ? nextEnt1 : getFingerTableEntry(isBackward, indQ)); 
        if (baseEnt == null) {
            System.out.println(n + ": null-entry-1, index = "
                    + indQ + ", " + toStringDetail());
            updateNext(p, isBackward, nextEnt2, null);
            return;
        }
        if (baseEnt.getNode() == null) {
            System.out.println(n + ": null-entry-2, index = "
                    + indQ + ", " + toStringDetail());
            updateNext(p, isBackward, nextEnt2, null);
            return;
        }
        if (baseEnt.getNode() == n) {
            // FTEntryの先頭ノードが削除された場合に発生する可能性がある
            System.out.println(n + ": self-pointing-entry, index = "
                    + indQ + ", " + toStringDetail());
            updateNext(p, isBackward, nextEnt2, null);
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
         * K = 4 の場合のN0の経路表:
         *                          (p-1)/B  delta=(K^⌊(p-1)/B⌋)
         *  N1  N2  N3      (p=0, 1)     0          1
         *  N4  N8 N12      (p=2, 3)     1          4
         * N16 N32 N48      (p=4, 5)     2          8
         */
        FTEntrySet passive1 = new FTEntrySet();
        int delta = 1, max = 1;
        if (p > 0) {
            delta = 1 << ((p - 1) / B * B);
            max = 1 << p;//(p - 1);
        }
        {
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
            ArrayList<FTEntry> p1ents = new ArrayList<>();
            for (int d = 0; d < max; d += delta) {
                int dist1 = d * (isBackward ? -1 : 1);
                int dist2 = dist1 + delta;
                FTEntry e = getFTEntryToSendTop(dist1, dist2);
                int dis = distQ - d;  // 当該エントリから Q までの距離
                if (false && dis < K) {
                    // XXX: THINK!: remove neighbors part
                    // note that this part is never executed when K=1.
                    FTEntry e0 = e;
                    e = new FTEntry(e.getNode());
                }
                p1ents.add(0, e);  // 逆順に格納
            }
            passive1.ents = p1ents.toArray(new FTEntry[p1ents.size()]);
        }
        // Passive Update 2で送信するエントリを収集
        FTEntrySet passive2 = null;
        if (PASSIVE_UPDATE_2 && isFirst) {
            List<FTEntry> p2ents = new ArrayList<>();
            for (int d = delta; isBackward ? d <= distQ : d < distQ; d += delta) {
                int d0 = d * (isBackward ? 1 : -1);
                int d1 = d0 + delta;
                FTEntry e = getFTEntryToSendTop(d0, d1);
                int dis = distQ - d;  // 当該エントリから Q までの距離
                if (dis < K) {
                    // XXX: THINK!: remove neighbors part
                    FTEntry e0 = e;
                    if (e != null) {
                        e = new FTEntry(e.getNode());
                    }
                }
                p2ents.add(e);
            }
            if (p > 0 && !isBackward) {
                // FFT側ノードのReverse Pointer更新用
                if (nextEnt2 != null) {
                    p2ents.add(nextEnt2.clone());
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
        Log.verbose(() -> "update: " + n.key + ", " + isBackward
                + ", ent = " + q.key + ", p =" + p);
        GetFTEntEvent ev = new GetFTEntEvent(q, isBackward, x, y, K, passive1, passive2);
        ev.onReply((repl, exc) -> {
            FingerTable tab = isBackward ? table.backward : table.forward;
            FingerTable opTab = isBackward ? table.forward : table.backward;
            if (exc != null) {
                System.out.println(n + ": getFingerTable0: TIMEOUT on " + q);
                Runnable job = () -> {
                    // XXX: ここで，getNode() は suspectedNode を考慮していない! 
                    if (baseEnt.getNode() != null) {
                        // we have a backup node
                        updateFingerTable0(p, isBackward, baseEnt, nextEnt2);
                    } else {
                        // we have no backup node
                        if (p + 1 < tab.getFingerTableSize()) {
                            System.out.println(n + ": No backup node: " + n + ", p =" + p + ", continue");
                            updateFingerTable0(p + 1, isBackward, null, null);
                        } else {
                            System.out.println(n + ": No backup node: " + n + ", p =" + p + ", no continue");
                            updateNext(p, isBackward, nextEnt2, null);
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
                    joinMsgs += 2;
                }
                FTEntry[] replEnts = repl.ent.ents;
                /* 取得したエントリをfinger tableにセットする */
                // the first entry (ents[0]) represents the sender of this 
                // RQReply.  we have confirmed the aliveness of the node.
                {
                    FTEntry e = replEnts[0];
                    assert e.getNode() == q;
                    tab.change(indQ, e, indQ > 0);
                }
                // process other entries...
                FTEntry nextEntX = null;
                for (int m = 1; m < replEnts.length; m++) {
                    FTEntry e = replEnts[m];
                    if (e != null && e.getNode() == null) {
                        // 取得したエントリが null の場合，以前のエントリを使う
                        e = getFingerTableEntry(isBackward, indQ + 1);
                        System.out.println(n + ": fetched null entry from "
                                + q + ", use old ent: " + e);
                        if (e == null || e.getNode() == null) {
                            break;
                        }
                    }
                    if (isCirculated(isBackward, q, e)) {
                        tab.shrink(indQ + m);
                        opTab.shrink(indQ + m);
                        break;
                    }
                    // System.out.println("m=" + m + ": e=" + e);
                    if (DELAY_ENTRY_UPDATE) {
                        if (m != replEnts.length - 1) {
                            tab.change(indQ + m, e, true);
                        } else {
                            // 最後のエントリの格納は生存が確認できてから行う
                            // (次の更新でアクセスするので)
                            nextEntX = e; 
                        }
                    } else { // Chord# way
                        // 取得したエントリの格納は今行う
                        tab.change(indQ + m, e, true);
                        nextEntX = e;
                    }
                }
                updateNext(p, isBackward, nextEnt2, nextEntX);
            }
        });
        n.post(ev);
    }

    /**
     * Finger Tableにpがなければ，pのreverse pointerからpを削除する．
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
                System.out.println(n + ": removes " + n + " from revPtr of " + p);
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
     * 次のFinger Table更新をスケジュールする．
     * @param p
     * @param isBackward
     */
    private void updateNext(int p, boolean isBackward,
            FTEntry nextEnt2, FTEntry nextEntX) {
        if (nextEnt2 == null && nextEntX == null) {
            finish(isBackward, p);
            return;
        }
        boolean isFirst = isFirst(isBackward);
        if (ZIGZAG_UPDATE) {
            // XXX: UPDATE_ONCE is ignored
            if (isFirst) {
                //assert p == 0 || nextEnt2 != null;
                if (!isBackward) {
                    updateFingerTable0(p, true, nextEnt2, nextEntX);
                } else {
                    updateFingerTable0(p + 1, false, nextEnt2, nextEntX);
                }
            } else {
                nextLevel = p + 1;
                Log.verbose(() -> "nextLevel=" + nextLevel +" , nextEntX=" + nextEntX);
                EventExecutor.sched(UPDATE_FINGER_PERIOD.value(),
                        () -> updateFingerTable0(p + 1, isBackward, nextEntX, null));
            }
        } else {
            assert nextEnt2 == null;
            if (isFirst || UPDATE_ONCE.value()) {
                updateFingerTable0(p + 1, isBackward, nextEntX, null);
            } else {
                EventExecutor.sched(UPDATE_FINGER_PERIOD.value(),
                        () -> updateFingerTable0(p + 1, isBackward, nextEntX, null));
            }
        }
    }
    
    /**
     * 最高位レベルのFinger Table Entry更新が終了した場合の処理．
     * 
     * @param isBackward
     * @param lastIndexPlus1
     */
    private void finish(boolean isBackward, int level) {
        boolean isFirst = isFirst(isBackward);
        Log.verbose(() -> 
            n + ": finger table update done: "
            + isBackward
            + ", level=" + level
            + ", " + (isBackward ? backwardUpdateCount : forwardUpdateCount) + "th" 
            + ", " + EventExecutor.getVTime()
            + "\n" + n.toStringDetail());
//        if (!isFirst) {
//            // truncate the finger tables
//            table.forward.shrink(level + 1);
//            if (USE_BFT) {
//                table.backward.shrink(level + 1);
//            }
//        }
        if (ZIGZAG_UPDATE || !isBackward) {
            forwardUpdateCount++;
            scheduleFTUpdate(isFirst);
        } else {
            backwardUpdateCount++;
        }
        return;
    }

    @Override
    public int getMessages4Join() {
        return joinMsgs;
    }

    /**
     * 自ノードが指しているすべてのノードのうち，生存しているノードの率を求める．
     * 
     * @param nodes すべてのノードの配列
     * @return 対称率
     */
    public double livenessDegree(LocalNode[] nodes) {
        Set<Node> a0 = this.gatherRemoteLinks();
        Set<LocalNode> a = (Set)a0;
        long nAlive = a.stream()
                .filter(node -> node.isInserted())
                .count();
        if (nAlive != a.size()) {
            List<Node> list = a.stream()
                    .filter(node -> !node.isInserted())
                    .collect(Collectors.toList());
            System.out.println("Node " + n.key + ": dead pointers = " + list);
        }
        return (double)nAlive / a.size();
    }

    private Set<Node> gatherRemoteLinks() {
        return table.stream()
            .map(ent -> ent.getNode())
            .filter(node -> node != n)
            .collect(Collectors.toSet());
    }

    private boolean doesPointTo(Node x) {
        for (int i = 0; i < getFingerTableSize(); i++) {
            FTEntry ent = getFingerTableEntry(i);
            if (ent != null && ent.getNode() == x) {
                return true;
            }
        }
        for (int i = 0; i < getBackwardFingerTableSize(); i++) {
            FTEntry ent = getBackwardFingerTableEntry(i);
            if (ent != null && ent.getNode() == x) {
                return true;
            }
        }
        return false;
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

    public static boolean isAllConverged(LocalNode[] nodes) {
        int count = 0;
        for (int i = 0; i < nodes.length; i++) {
            LocalNode n = nodes[i];
            SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(n);
            boolean rc = szk.isConverged(nodes);
            if (rc) count++;
        }
        System.out.println("# of converged nodes: " + count);
        return count == nodes.length;
    }

    private boolean isConverged(Node[] nodes) {
        int fftsiz = getFingerTableSize();
        for (int i = 0; i < fftsiz; i++) {
            int d = 1 << i;
            FTEntry ent = getFingerTableEntry(i);
            if (ent == null) {
                return false;
            }
            Node lnk = ent.getNode();
            if (lnk == null) {
                return false;
            }
            int key = (int)(n.key.getPrimaryKey());
            int lnkkey = (int)(lnk.key.getPrimaryKey());
            if ((key / 10 + d) % nodes.length != lnkkey / 10) {
                return false;
            }
        }
        //System.out.println(n + " is converged");
        return true;
    }
}
