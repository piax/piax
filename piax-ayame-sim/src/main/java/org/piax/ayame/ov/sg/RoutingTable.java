package org.piax.ayame.ov.sg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.piax.ayame.FTEntry;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.Node.NodeMode;
import org.piax.ayame.ov.ddll.DdllStrategy;
import org.piax.ayame.ov.ddll.DdllStrategy.DdllStatus;
import org.piax.ayame.sim.Sim;
import org.piax.util.FlexibleArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skip Graphの経路表．
 * 各レベルの左右のノードへのポインタと，近隣ノード集合を含む．
 */
public class RoutingTable {
    static final Logger logger = LoggerFactory.getLogger(RoutingTable.class);

    //public final int MAXLEVEL;//スキップグラフの最大レベル
    protected SkipGraphStrategy sg;
    private FlexibleArray<Tile> tiles;
    Set<Node> failedNodes = new HashSet<>();

    public RoutingTable(SkipGraphStrategy strategy) {
        this.tiles = new FlexibleArray<>();
        this.sg = strategy;
        tiles.set(0, new Tile(0, null, null));  // fake
    }

    private LocalNode getNode() {
        return sg.getLocalNode();
    }

    void constructRoutingTable() {
        //System.out.println("constructRoutingTable: " + getNode());
        //Node.dispatcher.dump();
        logger.trace("{} is constructing routing table {}", getNode(),
                sg.mv);
        for (int i = 1;; i++) {
            constructRoutingTable(i);
            if (getRightNeighbor(i) == null || getRightNeighbor(i) == getNode()) {
                break;
            }
        }
        logger.trace("{}", getNode().toStringDetail());
        logger.trace("msgs for join: {}, height={}", getNode().counters, height());
    }
    
    /**
     * 指定されたレベル(level)のルーティングテーブルを構築する．
     * 全てのノードで，level未満のレベルのルーティングテーブルが設定されている必要がある
     *
     * @param level  レベル
     */
    private void constructRoutingTable(int level) {
        LocalNode me = getNode();
        int count = 0;
        LocalNode r;
        for (r = getRightNeighbor(level - 1); r != me;
                r = SkipGraphStrategy.getTable(r).getRightNeighbor(level - 1), count++) {
            if (r.mode != NodeMode.INSERTED) {
                // ignore inserting node
                continue;
            }
            if (sg.mv.matchLength(SkipGraphStrategy.getMV(r)) >= level) {
                break;      // found!
            }
            //System.out.println("r = " + r + ", " + SkipGraphStrategy.getMV(r));
            if (count > 40) {
                System.out.println("too many hops at level " + level);
                //Node.dispatcher.dump();
                throw new Error(getNode() + ": too many hops at level " + level);
            }
        }
        LocalNode l = SkipGraphStrategy.getTable(r).getLeftNeighbor(level);
        int msgs = 0;
        if (r != me) {
            tiles.set(level, new Tile(level, l, r));
            SkipGraphStrategy.getTable(l).setRightNeighbor(level, me);
            SkipGraphStrategy.getTable(r).setLeftNeighbor(level, me);
            //   level: A     C     E
            // level-1: A  B  C  D  E
            // Cが自ノードとする．EでMVマッチしたとすると，count = 1
            // EからEの左ノードを取得する
            // メッセージの流れは C -> D -> E -> C
            // このとき，count + 2 メッセージ
            // さらに, A-C間に挿入するために3メッセージ
            msgs = count + 2 + 3;
            logger.trace("level={}, l={}, r={}, msgs={}", level, l, r, msgs);
        } else {
            msgs = count + 1;
            logger.trace("level={}, msgs={}", level, msgs);
        }
        getNode().counters.add("join.ftupdate", msgs);
    }

    /**
     * なんちゃってリカバー
     *
     * @param failed 故障ノード
     */
    void fix(Node failed) {
        LocalNode me = getNode();
        int h = height();
        LocalNode[] nodes = Sim.nodes;// Node.dispatcher.getNodes();
        int index = Arrays.binarySearch(nodes, me);
        assert index >= 0;
        //System.out.println(me + ": fixing (failed=" + failed + ")\n" + me.toStringDetail());
        for (int i = 0; i < h; i++) {
            //System.out.println("getRightNeighbor(" + i + ")=" + getRightNeighbor(i));
            if (getRightNeighbor(i) == failed) {
                // find the right candidate node
                int rind = (index + 1) % nodes.length;
                LocalNode r = me;
                for (; rind != index; rind = (rind + 1) % nodes.length) {
                    LocalNode r0 = nodes[rind];
                    if (isInserted(r0, failed) 
                            && sg.mv.matchLength(SkipGraphStrategy.getMV(r0)) >= i) {
                        r = r0;
                        break;
                    }
                }
                //System.out.println("r="+r);
                setRightNeighbor(i, r);
                SkipGraphStrategy rs = SkipGraphStrategy.getSkipGraphStrategy((LocalNode)r);
                rs.table.setLeftNeighbor(i, me);
            }
            if (getLeftNeighbor(i) == failed) {
                // find the left candidate node
                int lind = (index + nodes.length - 1) % nodes.length;
                LocalNode l = me; 
                for (; lind != index; lind = (lind + nodes.length - 1) % nodes.length) {
                    LocalNode l0 = nodes[lind];
                    if (isInserted(l0, failed) 
                            && sg.mv.matchLength(SkipGraphStrategy.getMV(l0)) >= i) {
                        l = l0;
                        break;
                    }
                }
                //System.out.println("l="+l);
                setLeftNeighbor(i, l);
                SkipGraphStrategy rs = SkipGraphStrategy.getSkipGraphStrategy(l);
                rs.table.setRightNeighbor(i, me);
            }
        }
        System.out.println(me + ": fixed (failed="+ failed +")\n" + me.toStringDetail());
    }

    private static boolean isInserted(LocalNode x, Node failed) {
        DdllStrategy ds = DdllStrategy.getDdllStrategy(x);
        return x != failed
                && (x.mode == NodeMode.INSERTED || x.mode == NodeMode.DELETING)
                && (ds.getStatus() == DdllStatus.IN || ds.getStatus() == DdllStatus.DEL); 
    }

    /**
     * ノードの高さを返す
     * @return height
     */
    public int height() {
        int i;
        for (i = 0; i < tiles.maxIndexPlus1(); i++) {
            if (getNode() == getRightNeighbor(i)) {
                return i;
            }
        }
        return i;
    }

    private Tile getTile(int level) {
        Tile t = tiles.get(level);
        if (t == null) {
            tiles.set(level, t = new Tile(level, getNode(), getNode()));
        }
        return t;
    }
    
    /**
     * 指定されたlevelの右ノードを返すs
     * @param level
     * @return 右ノード
     */
    public LocalNode getRightNeighbor(int level) {
        return getTile(level).getRight();
    }

    /**
     * 指定されたlevelの左ノードを返す
     * @param level
     * @return 左ノード
     */
    public LocalNode getLeftNeighbor(int level) {
        return getTile(level).getLeft();
    }

    /**
     * 指定されたlevelの右ノードを更新
     * @param level
     * @param right
     */
    public void setRightNeighbor(int level, LocalNode right) {
        getTile(level).setRight(right);
    }

    /**
     * 指定されたlevelの右ノードを更新
     * @param level
     * @param left
     */
    public void setLeftNeighbor(int level, LocalNode left) {
        getTile(level).setLeft(left);
    }

    List<FTEntry> getRoutingEntries() {
        List<FTEntry> links = new ArrayList<>();
        LocalNode node = getNode();
        if (node.mode != NodeMode.INSERTED && node.mode != NodeMode.DELETING) {
            return links;
        }
        links.add(new FTEntry(getNode(), true));
        for (int i = 0; i < height(); i++) {
            links.add(new FTEntry(getRightNeighbor(i), false));
            links.add(new FTEntry(getLeftNeighbor(i), false));
        }
        links = links.stream()
                .filter(n -> !failedNodes.contains(n))
                .collect(Collectors.toList());
        //links.removeAll(failedNodes);
        return links;
    }

    void setLeftNeighbors(int level, List<Node> nbrs) {
        tiles.get(level).leftNeighbors = nbrs;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
//        for (int lv = 0; lv < MAXLEVEL; lv++) {
        for (int lv = 0; lv < height(); lv++) {
            String line = String.format("%2d: %26s\n", lv, tiles.get(lv));
            result.append(line);
        }
//        result.append(" ALL: ").append(getAllLinks());
        return result.toString();
    }
    

//    public NodeAndLevel searchLeftNeighbor(DdllKey destKey, int level) {
//        for (int lv = level; lv >= 0; lv--) {
//            Node leftNode = getLeftNeighbor(lv);
//            if (leftNode == getNode() && lv > 0) {
//                continue;
//            }
//            if (ord(destKey, leftNode.key, getNode().key)) {
//                logger.debug("{} to {}, dest={}, lv={}",
//                        getNode(), leftNode, destKey, lv);
//                return new NodeAndLevel(leftNode, lv);
//            }
//        }
//        throw new Error("key not found");
//    }
//
//    public NodeAndLevel searchRightNeighbor(DdllKey destKey, int level) {
//        for (int lv = level; lv >= 0; lv--) {
//            Node rightNode = getRightNeighbor(lv);
//            if (rightNode == getNode() && lv > 0) {
//                continue;
//            }
//            if (ord(getNode().key, rightNode.key, destKey)) {
//                logger.debug("{} to {}, dest ={}, lv={}", 
//                        getNode(), rightNode, destKey, lv);
//                return new NodeAndLevel(rightNode, lv);
//            }
//        }
//        throw new Error("key not found");
//    }

    public static boolean ord(int a, int b, int c) {
        return (a == c)
                || (a <= b && b <= c)
                || (b <= c && c <= a)
                || (c <= a && a <= b);
    }

    /**
     * ノードが保持する各レベルのリンク情報を保持する．
     */
    private class Tile {
        private LocalNode _left;
        private LocalNode _right;
        // 近隣ノード集合．leftを含む! 
        private List<Node> leftNeighbors = new ArrayList<>();
        private final int level;
        
        public Tile(int level, LocalNode left, LocalNode right) {
            this.level = level;
            this._left = left;
            this._right = right;
        }
        
        void setRight(LocalNode node) {
            assert level > 0;
            this._right = node;
        }

        void setLeft(LocalNode node) {
            assert level > 0;
            this._left = node;
        }

        LocalNode getLeft() {
            if (level == 0) {
                return (LocalNode)sg.getPredecessor();
            }
            return _left;
        }
        
        LocalNode getRight() {
            if (level == 0) {
                return (LocalNode)sg.getSuccessor();
            }
            return _right;
        }

        @Override
        public String toString() {
            return String.format("L=%s, R=%s, NBRS=%s", getLeft(),
                    getRight(), leftNeighbors);
        }
    }

    /**
     * 次に転送するノードとレベルの検索結果
     */
    protected static class NodeAndLevel {
        public Node node;
        public int level;

        public NodeAndLevel(Node node, int level) {
            this.node = node;
            this.level = level;
        }
    }
}
