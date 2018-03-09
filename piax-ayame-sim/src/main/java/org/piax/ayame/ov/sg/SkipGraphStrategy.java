package org.piax.ayame.ov.sg;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.ayame.EventExecutor;
import org.piax.ayame.FTEntry;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.NodeStrategy;
import org.piax.ayame.Event.Lookup;
import org.piax.ayame.Event.LookupDone;
import org.piax.ayame.Node.NodeMode;
import org.piax.ayame.ov.ddll.DdllStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of (fake) Skip Graph.  This implementation uses DDLL for 
 * managing the level 0 ring.  Levels above 1 and higher are not implemented
 * in distributed fashion. This implementation is far from complete and can 
 * be used only for simple simulations.
 * 
 * <blockquote>
 * J. Aspnes and G. Shah, "Skip graphs," ACM Trans. on Algorithms, vol. 3,
 * no. 4, pp. 1–25, 2007.
 * </blockquote>
 */
public class SkipGraphStrategy extends NodeStrategy {
    public static class SkipGraphNodeFactory extends NodeFactory {
        @Override
        public void setupNode(LocalNode node) {
            node.pushStrategy(new DdllStrategy());
            node.pushStrategy(new SkipGraphStrategy());
        }
    }

    static final Logger logger = LoggerFactory.getLogger(SkipGraphStrategy.class);
    public final MembershipVector mv;
    public final RoutingTable table;
    public final static int BASE = 2;
    public final static int NNBRS = 6;
    NodeStrategy base;

    public SkipGraphStrategy() {
        this.mv = new MembershipVector(BASE);
        this.table = new RoutingTable(this);
    }

    @Override
    public void activate(LocalNode node) {
        super.activate(node);
        this.base = (DdllStrategy)node.getLowerStrategy(this);
    }

    public static SkipGraphStrategy getSkipGraphStrategy(LocalNode node) {
        return (SkipGraphStrategy)node.getStrategy(SkipGraphStrategy.class);
    }

    @Override
    public String toStringDetail() {
        StringBuilder buf = new StringBuilder();
        buf.append("|mode: " + n.mode).append("\n");
        buf.append("|ddll: ");
        buf.append(base.toStringDetail()).append("\n");
        buf.append("|mv: ").append(mv).append("\n");
        //buf.append("predecessors: " + getPredecessorList());
        //buf.append("|successors: " + getSuccessorList()).append("\n");
        int size = height();
        int lmax = 2;
//        String fmt0 = "|  |%-" + lmax + "s|%s\n";
        String fmt1 = "|%2d|%-" + lmax + "s|%s\n";
        for (int i = 0; i < size; i++) {
            buf.append(String.format(fmt1, i, table.getLeftNeighbor(i),
                    table.getRightNeighbor(i)));
        }
        return buf.toString();
    }
    @Override
    public void initInitialNode() {
        base.initInitialNode();
    }

    @Override
    public CompletableFuture<Void> join(LookupDone lookupDone) {
        logger.trace("{}: join route: {}", n, lookupDone.route); 
        logger.trace("lookup hops: {}", lookupDone.hops());
        n.counters.add("join.lookup", lookupDone.hops());
        return base.join(lookupDone).thenRun(() -> {
            nodeInserted();
        });
    }

    @Override
    public CompletableFuture<Void> leave() {
        logger.trace("{}: start leave", n);
        return base.leave().thenRun(() -> {
            // SetRAckを受信した場合の処理
            logger.debug("{}: mode=grace", n);
            n.mode = NodeMode.DELETED;
            for (int i = 1; i < height(); i++) {
                LocalNode left = table.getLeftNeighbor(i);
                LocalNode right = table.getRightNeighbor(i);
                getTable(left).setRightNeighbor(i, right);
                getTable(right).setLeftNeighbor(i, left);
            }
            System.out.println("SG:leaved" + n);
        });
    }

    @Override
    public void handleLookup(Lookup l) {
        if (isResponsible(l.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            Node next = n.getClosestPredecessor(l.key);
            // successorがfailしていると，next.node == n になる
            if (next == null || next == n) {
                System.out.println(n.key + ": @@@ successor(" + getSuccessor() + ") fails, target=" + l.key);
                System.out.println(n.toStringDetail());
                /*if (l.getFTEntry) {
                    FTEntry ent = getFingerTableEntryForRemote(false,
                            FingerTable.LOCALINDEX, 0);
                    n.post(new FTEntUpdateEvent(l.sender, l.index, ent));
                }*/
                n.post(new LookupDone(l, n, n.succ));
                return;
            }
            assert next != n;
            l.delay = Node.NETWORK_LATENCY;
            //l.getFTEntry = nRetry > 0;
            /*{
                FTEntry ent = next.getFTEntry(this);
                l.getFTEntry = ent.nbrs == null
                        || ent.nbrs.length < SUCCESSOR_LIST_SIZE;
            }*/
            n.forward(next, l, (exc) -> {
                /* [相手ノード障害時]
                 * - 障害ノード集合に追加
                 * - getClosestPredecessorからやりなおす．
                 *   - getClosestPredecessorでは，障害ノード集合を取り除く
                 * - MessageにはLevelを入れておく
                 * - Level != 0 ならば経路表修復のためのFTEntryを貰う 
                */
                //l.faildNodes.add(next.node);
//                FTEntry ent = next.getFTEntry(this);
//                if (ent.link == next.node) {
//                    ent.removeHead();
//                }
//                /* なんちゃって修復 */
//                if (next.node == n.succ) {
//                    System.out.println("REPAIR!");
//                    Node x = n.succ;
//                    for (; x.mode != NodeMode.INSERTED; x = x.succ) {}
//                    n.succ = x;
//                    x.pred = n;
//                }
                table.failedNodes.add(next);
                if (next == getSuccessor() || next == getPredecessor()) {
                    System.out.println(EventExecutor.getVTime()
                            + ": TIMEOUT(L0) " + next.key + "(from " + n + ")\n"
                            + n.toStringDetail());
                    ((DdllStrategy)base).checkAndFix();
                } else {
                    System.out.println(EventExecutor.getVTime()
                            + ": TIMEOUT(non L0) " + next.key + "(from " + n + ")\n"
                            + n.toStringDetail());
                    table.fix(next);
                }
                handleLookup(l); // retry immediately
            });
        }
    }

    private void nodeInserted() {
        //System.out.println("nodeInserted is called");
        table.constructRoutingTable();
    }
    
    @Override
    public List<FTEntry> getRoutingEntries() {
        return table.getRoutingEntries();
    }

    /**
     * ノードが持つ各レベルの情報を出力する．
     */
    public void dump() {
        System.out.println(this);
        System.out.println(table);
    }

    /**
     * ノードの高さを返す
     * @return height
     */
    public int height() {
        return table.height();
    }

    static MembershipVector getMV(Node x) {
        SkipGraphStrategy s = SkipGraphStrategy.getSkipGraphStrategy((LocalNode)x);
        return s.mv;
    }

    static RoutingTable getTable(Node x) {
        SkipGraphStrategy s = SkipGraphStrategy.getSkipGraphStrategy((LocalNode)x);
        return s.table;
    }
}