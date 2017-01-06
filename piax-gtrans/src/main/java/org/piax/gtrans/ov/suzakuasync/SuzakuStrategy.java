package org.piax.gtrans.ov.suzakuasync;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.async.FailureCallback;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.EventDispatcher;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.Node.NodeMode;
import org.piax.gtrans.async.NodeAndIndex;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.async.Option.BooleanOption;
import org.piax.gtrans.async.Option.IntegerOption;
import org.piax.gtrans.async.Sim;
import org.piax.gtrans.async.SuccessCallback;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddllasync.DdllStrategy;
import org.piax.gtrans.ov.ring.rq.FlexibleArray;
import org.piax.gtrans.ov.suzakuasync.SuzakuEvent.FTEntRemoveEvent;
import org.piax.gtrans.ov.suzakuasync.SuzakuEvent.FTEntUpdateEvent;
import org.piax.gtrans.ov.suzakuasync.SuzakuEvent.GetFTAllEvent;
import org.piax.gtrans.ov.suzakuasync.SuzakuEvent.GetFTAllReplyEvent;
import org.piax.gtrans.ov.suzakuasync.SuzakuEvent.GetFTEntEvent;
import org.piax.gtrans.ov.suzakuasync.SuzakuEvent.GetFTEntReplyEvent;
import org.piax.gtrans.ov.suzakuasync.SuzakuEvent.RemoveReversePointerEvent;

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
        public LocalNode createNode(TransportId transId,
                ChannelTransport<?> trans, DdllKey key, int latency)
                        throws IdConflictException, IOException {
            NodeStrategy base = new DdllStrategy();
            SuzakuStrategy szk = new SuzakuStrategy(base);
            LocalNode n = new LocalNode(transId, trans, key, szk, latency);
            base.setupNode(n);
            n.setBaseStrategy(base);
            szk.setupLinkChangeListener(n);
            return n;
        }
        @Override
        public String name() {
            return "Suzaku";
        }
    }

    public static void load() {
        // dummy
    }

    public static boolean USE_BFT = false;
    //public static boolean SUZAKU2 = false;
    //public static boolean SUZAKU3 = false;

    public static boolean DBEUG_FT_UPDATES = true;
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

    private static void sanityCheck() {
    }

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
    FingerTable forwardTable;
    FingerTable backwardTable;

    // 次にfinger tableを更新するレベル (デバッグ用)
    int nextLevel = 0;

    DdllStrategy base;

    Set<Node> failedNodes = new HashSet<>();

    int joinMsgs = 0;
    
    // for notify option
    private Set<Node> reversePointers = new HashSet<>();

    public SuzakuStrategy(NodeStrategy base) {
        this.base = (DdllStrategy)base;
    }

    @Override
    public void setupNode(LocalNode node) {
        super.setupNode(node);
        forwardTable = new FingerTable(n, false);
        if (USE_BFT) {
            backwardTable = new FingerTable(n, true);
        }
    }

    public void setupLinkChangeListener(Node n) {
//        n.setLinkChangeEventHandler((prev, cur) -> {
//        }, (prev, cur) -> {
//        });
    }


    @Override
    public void initInitialNode() {
        base.initInitialNode();
        // FINGER_UPDATE_PERIOD後に最初のFinger Table更新を行う際に
        // zigzag updateを行わないようにするため，forwardUpdateCount = 1 とする．
        forwardUpdateCount = 1;
        scheduleFTUpdate(true);
    }

    @Override
    public void joinAfterLookup(LookupDone lookupDone, SuccessCallback cb,
            FailureCallback eh) {
        System.out.println("JoinAfterLookup: " + lookupDone.route); 
        System.out.println("JoinAfterLookup: " + lookupDone.hops());
        System.out.println("JOIN " + n.key + " between " + lookupDone.pred + " and " + lookupDone.succ);
        assert Node.isOrdered(lookupDone.pred.key, n.key, lookupDone.succ.key);
        joinMsgs += lookupDone.hops();
        base.joinAfterLookup(lookupDone, (node) -> {
            // 右ノードが変更された契機でリバースポインタの不要なエントリを削除
            SuzakuStrategy szk = (SuzakuStrategy)node.topStrategy;
            szk.sanitizeRevPtrs();
            nodeInserted();
            cb.run(node);
        }, eh);
    }

    @Override
    public void leave(SuccessCallback callback) {
        LocalNode.verbose("leave " + n);
        SuccessCallback job;
        if (NOTIFY_WITH_REVERSE_POINTER.value()) {
            job = (node) -> {
                // このラムダ式は，SetRを受信したノードで動作することに注意!
                // node: SetR受信ノード, n: SetR送信ノード

                // 右ノードが変更された契機でリバースポインタの不要なエントリを削除
                SuzakuStrategy szk = (SuzakuStrategy)node.topStrategy;
                szk.sanitizeRevPtrs();

                Set<Node> s = reversePointers;
                System.out.println(node + ": receives revptr ("
                        + s.size() + ") from " + node.succ + ": " + s);
                s.remove(n); // ノードnは既に削除済
                List<Node> neighbors = szk.getNeighbors();
                neighbors.add(0, node);
                {
                    // nodeのfinger tableで，nをnodeに置き換える
                    szk.removeFromFingerTable(n, neighbors);
                }
                s.forEach(x -> {
                    // x が区間 [node, n] に含まれている場合，不要なエントリなので無視．
                    // (そのような x は削除済みであるため)
                    if (!Node.isOrdered(node.key, x.key, n.key)) {
                        // change entry: n -> neighbors
                        node.post(new FTEntRemoveEvent(x, n, neighbors));
                        szk.addReversePointer(x);
                    } else {
                        if (DEBUG_REVPTR) {
                            System.out.println(node + ": filtered " + x);
                        }
                    }
                });
            };
        } else {
            job = null;
        }
        System.out.println(n + ": start DDLL deletion");
        DdllStrategy ddll = (DdllStrategy)base;
        ddll.leave(node -> {
            // SetRAckを受信した場合の処理
            assert node == n;
            n.mode = NodeMode.GRACE;
            System.out.println(n + ": mode=grace");
            EventDispatcher.sched(NetworkParams.ONEWAY_DELAY, () -> {
                n.mode = NodeMode.DELETED;
                System.out.println(n + ": mode=deleted");
                if (callback != null) {
                    callback.run(node);
                }
            });
        }, job /* jobは左ノードでSetRが成功した場合に左ノード上で実行される */);
    }

    /**
     * 自ノードの reverse pointer で，[myKey, successorKey] に含まれるものを削除 
     */
    void sanitizeRevPtrs() {
        Set<Node> set = reversePointers.stream()
                .filter(x -> !Node.isOrdered(n.key, x.key, n.succ.key))
                .collect(Collectors.toSet());
        reversePointers = set;
    }

    /**
     * リモートノードが削除された場合に送信されるFTEntRemoveEventメッセージの処理
     * 
     * @param r 削除ノード
     * @param neighbor 削除ノードの代替ノード
     */
    void removeFromFingerTable(Node r, List<Node> neighbors) {
        assert r != n;
        removeReversePointer(r);
        boolean replaced = false;
        for (int i = getFingerTableSize(); i > 0; i--) {
            FTEntry ent = getFingerTableEntry(false, i);
            if (ent != null && ent.getLink() == r) {
                if (neighbors.get(0) == n) {
                    // XXX: 論文に入っていない修正 
                    // 代替ノードが自分自身ならば，successorに付け替える．
                    System.out.println(n + ": removeFromFingerTable: self!");
                    FTEntry e0 = getFingerTableEntry(0);
                    ent.setLink(e0.getLink());
                    ent.setNbrs(e0.getNbrs());
                } else {
                    ent.replace(neighbors);
                }
                replaced = true;
            }
        }
        for (int i = getBackwardFingerTableSize(); i > 0; i--) {
            FTEntry ent = getFingerTableEntry(true, i);
            if (ent != null && ent.getLink() == r) {
                ent.replace(neighbors);
                replaced = true;
            }
        }
        if (replaced) {
            if (DEBUG_REVPTR) {
                System.out.println(n + ": removeFromFingerTable: replaced: " + toStringDetail());
            }
        } else {
            if (DEBUG_REVPTR) {
                System.out.println(n + ": removeFromFingerTable missed " + r);
            }
        }
    }

    public void addReversePointer(Node node) {
        if (node == n) {
            return;
        }
        if (DEBUG_REVPTR) System.out.println(n + ": add revptr " + node);
        this.reversePointers.add(node);
    }

    public void removeReversePointer(Node node) {
        boolean rc = this.reversePointers.remove(node);
        if (NOTIFY_WITH_REVERSE_POINTER.value() && !rc) {
            System.out.println(n + ": removeRP does not exist: " + node + "\n"
                    + n.toStringDetail()); 
        }
    }

    @Override
    public void handleLookup(Lookup l) {
        if (l.getFTEntry) {
            FTEntry ent = getFingerTableEntryForRemote(false,
                    FingerTable.LOCALINDEX, 0);
            System.out.println("handleLookup: "
                    + n + " sends FTEntUpdateEvent: " + ent);
            n.post(new FTEntUpdateEvent(l.sender, l.index, ent));
        }
        handleLookup(l, 0);
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
        if (isResponsible(l.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            NodeAndIndex next = n.getClosestPredecessor(l.key);
            // successorがfailしていると，next.node == n になる
            if (next == null || next.node == n) {
                System.out.println(n + ": handleLookup! key=" + l.key + ", evid=" + l.getEventId() + ", next=" + next + ", " + toStringDetail());
                //System.out.println(n.toStringDetail());
                if (l.getFTEntry) {
                    FTEntry ent = getFingerTableEntryForRemote(false,
                            FingerTable.LOCALINDEX, 0);
                    n.post(new FTEntUpdateEvent(l.sender, l.index, ent));
                }
                n.post(new LookupDone(l, n, n.succ));
                return;
            }
            assert next.node != n;
            l.index = next.index;
            l.delay = Node.NETWORK_LATENCY;
            //l.getFTEntry = nRetry > 0;
            {
                FTEntry ent = getFTEntry(this, next);
                l.getFTEntry = ent.nbrs == null
                        || ent.nbrs.length < SUCCESSOR_LIST_SIZE;
            }
            System.out.println("T=" + EventDispatcher.getVTime() + ": " + n + ": handleLookup " + l.getEventId() + " " + next);
            n.forward(next.node, l, (exc) -> {
                /* [相手ノード障害時]
                 * - 障害ノード集合に追加
                 * - getClosestPredecessorからやりなおす．
                 *   - getClosestPredecessorでは，障害ノード集合を取り除く
                 * - MessageにはLevelを入れておく
                 * - Level != 0 ならば経路表修復のためのFTEntryを貰う 
                */
                //l.faildNodes.add(next.node);
                FTEntry ent = getFTEntry(this, next);
                System.out.println("TIMEOUT: " + n + " sent a query to "
                        + next.node.key
                        + ", ftent = " + ent
                        + ", failedNodes=" + failedNodes + "\n"
                        + n.toStringDetail() 
                        + "\n" + next.node.toStringDetail());
                if (ent != null && ent.getLink() == next.node) {
                    ent.removeHead();
                }
                /* なんちゃって修復 */
                if (next.node == n.succ || next.node == n.pred) {
                    DdllStrategy.fix(next.node);
                }
                failedNodes.add(next.node);
                handleLookup(l, nRetry + 1);
            });
        }
    }

    // handles FTEntUpdateEvent
    public void updateFTEntry(FTEntUpdateEvent event) {
        FingerTable table = event.index < 0 ? backwardTable : forwardTable;
        table.change(index2ftIndex(event.index), event.ent, true); 
    }

    @Override
    public String toStringDetail() {
        StringBuilder buf = new StringBuilder();
        buf.append("|mode: " + n.mode).append("\n");
        buf.append("|ddll: ");
        buf.append(base.toStringDetail()).append("\n");
        buf.append("|FFT update: " + forwardUpdateCount
                + ", BFT update: " + backwardUpdateCount
                + ", nextLevel: " + nextLevel + "\n");
        if (NOTIFY_WITH_REVERSE_POINTER.value()) {
            buf.append("|reverse: " + reversePointers + "\n");
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
        return buf.toString();
    }

    public void nodeInserted() {
        joinMsgs += base.getMessages4Join();    // add messages consumed in DDLL 
        if (COPY_FINGERTABLES) {
            // copy predecessor's finger table
            GetFTAllEvent ev = new GetFTAllEvent(n.pred,
                    (GetFTAllReplyEvent rep) -> {
                        joinMsgs += 2;
                        FTEntry[][] fts = rep.ents;
                        for (int i = 1; i < fts[0].length; i++) {
                            forwardTable.set(i, fts[0][i]);
                        }
                        for (int i = 1; USE_BFT && i < fts[1].length; i++) {
                            backwardTable.set(i, fts[1][i]);
                        }
                        //System.out.println(n + ": FT copied\n" + n.toStringDetail());
                        if (ACTIVE_UPDATE_ON_JOIN) {
                            initialFTUpdate();
                        } else {
                            scheduleFTUpdate(true);
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
            delay = UPDATE_FINGER_PERIOD.value();
        } else {
            delay = UPDATE_FINGER_PERIOD.value();
            //delay = (int)(UPDATE_FINGER_PERIOD * (0.9+Math.random()*.2));
        }
        EventDispatcher.sched(delay, () -> {
            updateFingerTable(false);
        });
    }
    
    /**
     * レベル0リング挿入直後のアクティブなfinger table更新処理
     */
    private void initialFTUpdate() {
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

    //@Override
//    public Node[] getAllLinks() {
//        List<Node> links = new ArrayList<Node>();
//        if (n.mode != NodeMode.INSERTED && n.mode != NodeMode.DELETING) {
//            return links.toArray(new Node[links.size()]);
//        }
//        links.add(getLocalLink());
//        Node pred = getPredecessor();
//        if (pred != null) {
//            links.add(getPredecessor());
//        }
//        for (int i = 0; i < getFingerTableSize(); i++) {
//            FTEntry ent = getFingerTableEntry(i);
//            if (ent != null) {
//                links.add(ent.link);
//            }
//        }
//        for (int i = 0; USE_BFT && i < getBackwardFingerTableSize(); i++) {
//            FTEntry ent = getBackwardFingerTableEntry(i);
//            if (ent != null) {
//                links.add(ent.link);
//            }
//        }
//        return links.toArray(new Node[links.size()]);
//    }

    public List<NodeAndIndex> getAllLinks2() {
        List<NodeAndIndex> links = new ArrayList<>();
        if (n.mode != NodeMode.INSERTED && n.mode != NodeMode.DELETING) {
            return links;
        }
        links.add(new NodeAndIndex(getLocalLink(), 0));
        for (int i = 0; i < getFingerTableSize(); i++) {
            FTEntry ent = getFingerTableEntry(i);
            if (ent != null && ent.getLink() != null) {
                links.add(new NodeAndIndex(ent.getLink(), ftIndex2Index(i, false)));
            }
        }
        for (int i = 0; USE_BFT && i < getBackwardFingerTableSize(); i++) {
            FTEntry ent = getBackwardFingerTableEntry(i);
            if (ent != null && ent.getLink() != null) {
                links.add(new NodeAndIndex(ent.getLink(), ftIndex2Index(i, true)));
            }
        }
        links = links.stream()
                .filter((n) -> !failedNodes.contains(n.node))
                .collect(Collectors.toList());
        return links;
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
        List<Node> list = new ArrayList<Node>();
        int size = Math.min(SUCCESSOR_LIST_SIZE, getFingerTableSize());
        for (int i = 0; i < size; i++) {
            FTEntry ent = getFingerTableEntry(i);
            if (ent != null) {
                list.add(ent.getLink());
            }
        }
        return list;
    }

    public List<Node> getPredecessorList() {
        List<Node> list = new ArrayList<Node>();
        List<Node> nbrs = ((DdllStrategy)base).leftNbrs.getNeighbors();
        list.addAll(nbrs);
        return list;
    }

    // to be overridden
    protected FTEntry getLocalFTEnetry() {
        return new FTEntry(getLocalLink());
    }

    public int getFingerTableSize() {
        return forwardTable.getFingerTableSize();
    }

    public int getBackwardFingerTableSize() {
        return backwardTable.getFingerTableSize();
    }
    
    public final void rtLockR() {
    }
    
    public final void rtUnlockR() {
    }
    
    public final void rtLockW() {
    }

    public final void rtUnlockW() {
    }

    // RPC service
    /**
     * finger table 上で，距離が tk<sup>x</sup> (0 &lt;= t &lt;= 2<sup>y</sup>)
     * 離れたエントリを取得する．結果は 2<sup>y</sup>+1 要素の配列として返す． 
     * 
     * @param isBackward BFTを取得する場合はtrue
     * @param x     parameter
     * @param y     parameter
     * @param k     parameter
     * @param given finger table entries to give to the remote node
     * @param gift2 another entry used only by SUZAKU3
     * @return list of finger table entries
     */
    public FTEntrySet getFingers(boolean isBackward, int x, int y, int k,
            FTEntrySet given, FTEntry gift2 /* SUZAKU3 */) {
        FTEntrySet set = new FTEntrySet();
        // {tk^x | 0 < t <= 2^y}
        // x = floor(p/b), y = p-bx = p % b
        // ---> p = y + bx 
        set.ents = new FTEntry[(1 << y) + 1];
        // t = 0 represents the local node and
        // t > 0 represents finger table entries
        for (int t = 0; t <= (1 << y); t++) {
            int d = t * (1 << (B.value() * x)); // t*2^(Bx)
            int index = FingerTable.getFTIndex(d);
            //int d2 = (t + 1) * (1 << (B * x));
            int d2 = d + (1 << (B.value() * x)) ;
            int index2 = FingerTable.getFTIndex(d2);
            FTEntry l = getFingerTableEntryForRemote(isBackward, index, index2);
            set.ents[t] = l;
        }
        if (USE_BFT) {
            int p = y + B.value() * x;
            FingerTable opTable = isBackward ? forwardTable : backwardTable;
            if (gift2 != null) {
                assert PASSIVE_UPDATE_2;
                // Passive Update 2
                int index = FingerTable.getFTIndex(1 << (p + 1));
                if (!NOTIFY_WITH_REVERSE_POINTER.value() || isBackward) {
                    // opTable = FFT
                    opTable.change(index, gift2, !isBackward);
                    if (false) System.out.println/*Node.verbose*/(n.key
                            + ": use gift2 (" + gift2 + "), index="
                            + index + ", " + n.toStringDetail());
                } else {
                    // opTable = BFT
                    addReversePointer(gift2.getLink());
                }
            }
            if (given.ents.length > 0) {
                // Passive Update 1
                assert p > 0;
                int index = FingerTable.getFTIndex(1 << p);
                opTable.change(index, given.ents[0], true);
                int index2 = FingerTable.getFTIndex(1 << (p - 1));
                for (int i = 1; i < given.ents.length; i++) {
                    opTable.change(index2 + i, given.ents[i], true);
                }
            }
            /*System.out.printf(n.id + ": getFingers(x=%d, y=%d, k=%d, given=%s) returns %s\n",
                    x, y, k, given, set);
            System.out.printf(n.toStringDetail());*/
        }
        return set;
    }

    // RPC service
    public FTEntry[][] getFingerTable() {
        rtLockR();
        FTEntry[][] rc = new FTEntry[2][];
        rc[0] = new FTEntry[getFingerTableSize()];
        for (int i = 0; i < getFingerTableSize(); i++) {
            FTEntry ent = getFingerTableEntryForRemote(false, i, 0);
            rc[0][i] = ent;
        }
        if (USE_BFT) {
            rc[1] = new FTEntry[getBackwardFingerTableSize()];
            for (int i = 0; i < getBackwardFingerTableSize(); i++) {
                FTEntry ent = getFingerTableEntryForRemote(true, i, 0);
                rc[1][i] = ent;
            }
        }
        rtUnlockR();
        return rc;
    }

    public final FTEntry getFingerTableEntry(boolean isBackward, int index) {
        if (isBackward) {
            return getBackwardFingerTableEntry(index);
        } else {
            return getFingerTableEntry(index);
        }
    }

    public final FTEntry getFingerTableEntry(int index) {
        return forwardTable.getFTEntry(index);
    }

    protected FTEntry getBackwardFingerTableEntry(int index) {
        return backwardTable.getFTEntry(index);
    }

    /**
     * get a specified FTEntry for giving to a remote node.
     * in aggregation chord#, the range [index, index2) is used as the 
     * aggregation range.
     * this method is intended to be overridden by subclasses.
     * 
     * @param index     index of the entry
     * @param index2    index of the next entry. 
     * @return the FTEntry
     */
    protected FTEntry getFingerTableEntryForRemote(boolean isBackward, int index, int index2) {
        FTEntry ent = getFingerTableEntry(isBackward, index);
        //logger.debug("getFTRemote: {}, {}", index, index2);
        if (index == FingerTable.LOCALINDEX) {
            List<Node> neighbors = getNeighbors();
            ent.setNbrs(neighbors.toArray(new Node[neighbors.size()]));
        }
        // should clone!
        return (ent == null ? null : ent.clone());
    }

    void updateFingerTable(boolean isBackward) {
        rtLockW();
        try {
            if (n.mode == NodeMode.OUT || n.mode == NodeMode.DELETED || updatingFT) {
                return;
            }
            updatingFT = true;
        } finally {
            rtUnlockW();
        }
        LocalNode.verbose("start finger table update: " + n.key
                + ", " + EventDispatcher.getVTime());
        try {
            updateFingerTable0(0, isBackward, null, null);
        } finally {
            rtLockW();
            updatingFT = false;
            rtUnlockW();
        }
    }

    /**
     * Finger Tableを更新する．
     * SUZAKU1: 最初の更新かどうかは特に関係ない．
     * SUZAKU2: 最初のForward側の更新が終わってからforwardUpdateCount == 1となる
     * 同様にBackward側の更新が終わってからbackwardUpdateCount == 1となる
     * SUZAKU3: 最初の更新が終わってからforwardUpdateCount == 1となる．
     * 
     * @param p 更新するエントリを表す
     * @param isBackward
     * @param isFirst
     */
    private void updateFingerTable0(final int p, boolean isBackward,
            FTEntry nextEnt1, FTEntry nextEnt2) {
        nextLevel = p;
        boolean isFirst = isFirst(isBackward);
        int B = SuzakuStrategy.B.value();
        if (true)
        System.out.println/*Node.verbose*/(EventDispatcher.getVTime() + ": " 
                + "updateFingerTable0 " + n.key + ", p=" + p + ", " + isBackward
                + ", fcount=" + forwardUpdateCount
                + ", bcount=" + backwardUpdateCount
                + ", nextEnt1=" + nextEnt1
                + ", nextEnt2=" + nextEnt2);
        int distance = 1 << p;
        int index = FingerTable.getFTIndex(distance);
//        System.out.println("updateFingerTable: " + n.id
//                + ", p = " + p + ", index = " + index);
        if (n.mode == NodeMode.OUT || n.mode == NodeMode.DELETED) {
            return;
        }
        FTEntry ent;
        if (nextEnt1 != null) {
            ent = nextEnt1;
        } else {
            ent = getFingerTableEntry(isBackward, index);
        }
        if (ent == null) {
            System.out.println(n + ": null-entry-1, index = "
                    + index + ", " + toStringDetail());
            updateNext(p, isBackward, nextEnt2, null);
            return;
        }
        if (ent.getLink() == null) {
            System.out.println(n + ": null-entry-2, index = "
                    + index + ", " + toStringDetail());
            updateNext(p, isBackward, nextEnt2, null);
            return;
        }
        if (ent.getLink() == n) {
            // FTEntryの先頭ノードが削除された場合に発生する可能性がある
            System.out.println(n + ": self-pointing-entry, index = "
                    + index + ", " + toStringDetail());
            updateNext(p, isBackward, nextEnt2, null);
            return;
        }
        // Passive Update 1 の引数を計算
        FTEntrySet gift = new FTEntrySet();
        {
            ArrayList<FTEntry> gives = new ArrayList<FTEntry>();
            /* 
             * |-------------------------------> distance = 2^p
             * |--------------->                 dist = 2^(p - 1)
             * |--> delta
             * N==A==B==...====P===============Q
             * 
             * NがQに対してgetFingersを呼ぶ時，[A, B, ... P) を送る．
             * delta = N-A間の距離
             *       = K ^ floor((p - 1) / B)
             *       = 2 ^ (B * floor((p - 1) / B))
             *       = 2 ^ (floor((p - 1)) / B * B)
             *
             * K = 4 の場合のN0の経路表:
             *                          (p-1)/B  K^floor((p-1)/B)
             *  N1  N2  N3      (p=0, 1)     0          1
             *  N4  N8 N12      (p=2, 3)     1          4
             * N16 N32 N48      (p=4, 5)     2          8
             */
            int delta, max;
            if (p == 0) {
                delta = 1;
                max = 0; //1;
            } else {
                delta = 1 << ((p - 1) / B * B);
                max = 1 << (p - 1);
            }
            for (int d = 0; d < max; d += delta) {
                int idx = FingerTable.getFTIndex(d);
                int idx2 = FingerTable.getFTIndex(d + delta);
                FTEntry e = getFingerTableEntryForRemote(isBackward, idx, idx2);
                // dis = 当該エントリから Q までの距離
                int dis = distance - d;
                if (dis < K) {
                    e = e.clone();
                    e.setNbrs(null);
                }
                // 先頭が自ノード，以降は逆順になるようにリストに格納する．
                // 上の例の場合，gives = {N, C, B, A} となる．
                // TODO: 自然な順序で格納できるようにgetFingersを修正するべき
                if (gives.size() == 0) {
                    gives.add(e);
                } else {
                    gives.add(1, e);
                }
            }
            //System.out.println(n.id + " gives " + gives + " to " + ent.link.id);
            gift.ents = gives.toArray(new FTEntry[gives.size()]);
        }
        // Passive Update 2のための引数を計算
        FTEntry gift2 = null;
        if (PASSIVE_UPDATE_2 && isFirst) {
            if (p > 0) {
                if (isBackward) {   // BFT側ノードのFFT更新
                    gift2 = getFingerTableEntryForRemote(!isBackward, index, 0);
                } else {            // FFT側ノードのReverse Pointer更新
                    if (nextEnt2 != null) {
                        gift2 = nextEnt2.clone();
                    }
                }
            }
        }

        // リモートノードからfinger tableエントリを取得
        // 取得するエントリを指定するパラメータ (論文参照)
        int x = p / B;
        int y = p - B * x; // = p % B
        LocalNode.verbose("update: " + n.key + ", " + isBackward
                + ", ent = " + ent.getLink().key + ", p =" + p);
        // entから，距離が t*k^x (0 <= t <= 2^y)離れたエントリを取得する．
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
        Node baseNode = ent.getLink();
        FingerTable table = isBackward ? backwardTable : forwardTable;
        n.post(new GetFTEntEvent(baseNode, isBackward, x, y, K, gift, gift2, (GetFTEntReplyEvent repl) -> {
            //System.out.println(n + " receives " + repl + ", index = " + index + "\n" + n.toStringDetail());
            if (ACTIVE_UPDATE_ON_JOIN && isFirst) {
                joinMsgs += 2;
            }
            FTEntrySet set = repl.ent;
            FTEntry[] ents = set.ents;
            // 取得したエントリをfinger tableにセットする
            // the first entry ents[0] represents the remote node itself.
            // update the successor list
            {
                FTEntry e = ents[0];
                assert e.getLink() == baseNode;
                table.change(index, e, true);
            }
            // 先頭以降のエントリの処理
            assert B == 1;
            FTEntry nextEntX = null;
            for (int m = 1; m < ents.length; m++) {
                FTEntry e = ents[m];
                if (e != null && e.getLink() == null) {
                    // 取得したエントリが null の場合
                    e = getFingerTableEntry(isBackward, index + 1);
                    System.out.println(n + ": fetched null entry from "
                            + baseNode + ", use old ent: " + e);
                    if (e == null || e.getLink() == null) {
                        break;
                    }
                }
                if (isCirculated(isBackward, n, baseNode, e)) {
                    //table.shrink(index + m);
                    break;
                }
                if (DELAY_ENTRY_UPDATE) {
                    // 取得したエントリの格納は生存が確認できてから行う
                    if (m != 1) {
                        table.change(index + m, e, true); // XXX: Think!
                    } else {
                        nextEntX = e; 
                    }
                } else { // Chord# way
                    // 取得したエントリの格納は今行う
                    table.change(index + m, e, true);
                    nextEntX = e;
                }
            }
            updateNext(p, isBackward, nextEnt2, nextEntX);
        }), (exc) -> {
            System.out.println(n + ": getFingerTable0: TIMEOUT on " + baseNode);
            /* なんちゃって修復 */
            if (baseNode == n.succ || baseNode == n.pred) {
                DdllStrategy.fix(baseNode);
            }
            failedNodes.add(baseNode);
            if (baseNode == ent.getLink() && ent.removeHead()) {
                // リクエスト送信時からFTEntryが変化していなければ...
                // we have a backup node
                updateFingerTable0(p, isBackward, ent, nextEnt2);
            } else {
                // we have no backup node
                if (p + 1 < table.getFingerTableSize()) {
                    System.out.println(n + ": No backup node: " + n + ", p =" + p + ", continue");
                    updateFingerTable0(p + 1, isBackward, null, null);
                } else {
                    System.out.println(n + ": No backup node: " + n + ", p =" + p + ", no continue");
                    updateNext(p, isBackward, nextEnt2, null);
                }
            }
        });
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
            if (e != null && e.getLink() != null && e.getLink().key.compareTo(p.key) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            for (int i = getBackwardFingerTableSize() - 1; i >= 0; i--) {
                FTEntry e = getFingerTableEntry(true, i);
                if (e != null && e.getLink() != null && e.getLink().key.compareTo(p.key) == 0) {
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
     * @param origin
     * @param fetchFrom FTE取得元ノード
     * @param fetched 取得したFTE
     * @return
     */
    private boolean isCirculated(boolean isBackward, Node origin, Node fetchFrom, FTEntry fetched) {
        return (fetched == null
                || origin.key.compareTo(fetched.getLink().key) == 0
                || isBackward && Node.isOrdered(fetched.getLink().key, true, origin.key, fetchFrom.key, true))
                || (!isBackward && Node.isOrdered(fetchFrom.key, true, origin.key, fetched.getLink().key, true));
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
                EventDispatcher.sched(UPDATE_FINGER_PERIOD.value(),
                        () -> updateFingerTable0(p + 1, isBackward, nextEntX, null));
            }
        } else {
            assert nextEnt2 == null;
            if (isFirst || UPDATE_ONCE.value()) {
                updateFingerTable0(p + 1, isBackward, nextEntX, null);
            } else {
                EventDispatcher.sched(UPDATE_FINGER_PERIOD.value(),
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
        if (true || isFirst || Sim.verbose) {
            System.out.println(n + ": finger table update done: "
                    + isBackward
                    + ", " + (isBackward ? backwardUpdateCount : forwardUpdateCount) + "th" 
                    + ", " + EventDispatcher.getVTime()
                    + ", " + n.toStringDetail());
        }
        if (!isFirst) {
            // truncate the finger tables
            forwardTable.shrink(level + 1);
            if (USE_BFT) {
                backwardTable.shrink(level + 1);
            }
        }
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
     * 自ノードが指しているノードが自ノードを指している率を求める
     * 
     * @return 対称率
     */
    public double symmetricDegree() {
        /*if (!USE_BFT) {
            return 0.0;
        }
        int positive = 0;
        int negative = 0;
        for (int j = 0; j < 2; j++) {
            boolean isBackward = j == 1;
            for (int i = 1; i < getFingerTableSize(); i++) {
                FTEntry ent = getFingerTableEntry(isBackward, i);
                if (ent == null || ent.getLink() == null) {
                    continue;
                }
                Node x = ent.getLink();
                SuzakuStrategy xs = (SuzakuStrategy)x.topStrategy;
                FTEntry ent2 = xs.getFingerTableEntry(!isBackward, i);
                if (ent2 != null && ent2.getLink() == n) {
                    positive++;
                } else {
                    negative++;
                    Node.verbose(n + ": " + (isBackward?"BFT":"FFT") 
                            + " level " + i + " points to " + x
                            + " but it points to "
                            + (ent2 == null ? null : ent2.getLink())); 
                }
            }
        }
        return (double)positive / (positive + negative);*/
        return 0.0;
    }

    /**
     * 自ノードを指しているすべてのノードのうち，自ノードが指している率を求める
     * 
     * @param nodes すべてのノードの配列
     * @return 対称率
     */
    public double symmetricDegree2(Node[] nodes) {
        /*if (!USE_BFT) {
            return 0.0;
        }
        Set<Node> a = this.gatherRemoteLinks();
        Set<Node> b = new HashSet<>();
        for (Node node: nodes) {
            if (node == n || (node.mode != NodeMode.INSERTED && node.mode != NodeMode.DELETING)) {
                continue;
            }
            if (((SuzakuStrategy)node.topStrategy).doesPointTo(n)) {
                b.add(node);
            }
        }
        double denominator = b.size();
        b.removeAll(a);
        double nominator = b.size();
        double degree = 1 - nominator / denominator;
        System.out.println("Node " + n.id + ": symdegree=" + degree + " : " + b);
        if (degree != 0.0 && b.size() == 1) {
            System.out.println(n.toStringDetail());
            System.out.println(b.toArray(new Node[1])[0].toStringDetail());
        }
        return degree;*/
        return 0.0;
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
                .filter(node -> (node.mode == NodeMode.INSERTED
                    || node.mode == NodeMode.DELETING))
                .count();
        if (nAlive != a.size()) {
            List<Node> list = a.stream()
                    .filter(node -> (node.mode != NodeMode.INSERTED
                        && node.mode != NodeMode.DELETING))
                    .collect(Collectors.toList());
            System.out.println("Node " + n.key + ": dead pointers = " + list);
        }
        return (double)nAlive / a.size();
    }

    private Set<Node> gatherRemoteLinks() {
        Set<Node> set = new HashSet<>();
        for (int i = 0; i < getFingerTableSize(); i++) {
            FTEntry ent = getFingerTableEntry(i);
            if (ent != null) {
                set.add(ent.getLink());
            }
        }
        if (USE_BFT) {
            for (int i = 0; i < getBackwardFingerTableSize(); i++) {
                FTEntry ent = getBackwardFingerTableEntry(i);
                if (ent != null) {
                    set.add(ent.getLink());
                }
            }
        }
        set.remove(n);
        return set;
    }

    private boolean doesPointTo(Node x) {
        for (int i = 0; i < getFingerTableSize(); i++) {
            FTEntry ent = getFingerTableEntry(i);
            if (ent != null && ent.getLink() == x) {
                return true;
            }
        }
        for (int i = 0; i < getBackwardFingerTableSize(); i++) {
            FTEntry ent = getBackwardFingerTableEntry(i);
            if (ent != null && ent.getLink() == x) {
                return true;
            }
        }
        return false;
    }

    
    // from old NodeAndIndex class
    public static int ftIndex2Index(int ftIndex, boolean isBackward) {
        if (isBackward) {
            // -index - 1 = x  ->  index = -x - 1
            return -ftIndex - 1;
        } else {
            // index - 1 = x  ->  index = x + 1
            return ftIndex + 1;
        }
    }

    public static int index2ftIndex(int index) {
        if (index > 0) {
            return index - 1;
        }
        if (index < 0) {
            return -index - 1;
        }
        return 0;
    }

    public static FTEntry getFTEntry(SuzakuStrategy strategy,
            NodeAndIndex ni) {
        int ftIndex = index2ftIndex(ni.index);
        FTEntry ent;
        if (ni.index < 0) {
            ent = strategy.getBackwardFingerTableEntry(ftIndex);
        } else {
            ent = strategy.getFingerTableEntry(ftIndex);
        }
        return ent;
    }

    /**
     * a class for sending/receiving finger table entries between nodes.
     */
    public static class FTEntrySet implements Serializable {
        private static final long serialVersionUID = 1L;
        FTEntry[] ents;

        //Link[] predecessors;
        //Link[] successors;

        @Override
        public String toString() {
            //return "[ents=" + Arrays.deepToString(ents) + ", predecessors="
            //        + Arrays.toString(predecessors) + "]";
            //return "[ents=" + Arrays.deepToString(ents) + ", successors="
            //+ Arrays.toString(successors) + "]";
            return "[ents=" + Arrays.deepToString(ents) + "]";
        }
    }

    public static boolean isAllConverged(LocalNode[] nodes) {
        int count = 0;
        for (int i = 0; i < nodes.length; i++) {
            LocalNode n = nodes[i];
            SuzakuStrategy szk = (SuzakuStrategy)n.topStrategy;
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
            Node lnk = ent.getLink();
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
