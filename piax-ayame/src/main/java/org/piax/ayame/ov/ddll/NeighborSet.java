/*
 * NeighborSet.java - NeighborSet implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: NeighborSet.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.ayame.ov.ddll;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.ov.ddll.DdllEvent.PropagateNeighbors;
import org.piax.common.DdllKey;
import org.piax.common.Option.IntegerOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class for implementing a (left) neighbor set.
 */
public class NeighborSet {
    /*
     * (NEIGHBOR_SET_SIZE = 4 case)
     * 
     * inserting case
     *   0 1 2   4 5 (initially 4 knows {2 1 0 5})
     *   0 1 2 3 4 5 (3 is inserted)
     *               3 recvs {2 1 0 5} via SetRAck
     *               3 sends {3 2 1 0} to node 4
     *               4 sends {4 3 2 1} to node 5
     *
     * deleting case
     *   0 1 2 3 4 5 (initially 4 knows {3 2 1 0})
     *   0 1 2   4 5 (3 is deleted)
     *               when 2 receives SetR, 2 sends {2 1 0 5} to node 4
     *               4 sends {4 2 1 0} to node 5
     *
     * failure-recovery case
     *   0 1 2 3  (initially 3 knows {2 1 0}
     *  [0]1 2 3  (0 is failed)
     *            when 3 receives SetR, 3 sends {3 2} to node 1
     *            (node 1 and 0 should be removed from the forwarding set)
     * 
     * TODO: improve multiple keys on a single node case
     *   0 1 2 3 4 5 6 (0=1=2, 3=4=5, 6)
     *               5, 4 and 3 know {2 6}
     *               2, 1 and 0 know {6 5}
     *               6 knows {5 2}
     *   0 1 2 3 4 5 6 (0=2=4, 1=3=5, 6)
     *               Node0=2=4 knows {Node6 Node1=3=5}
     *               Node1=3=5 knows {Node0=2=4 Node6}
     */
    /*--- logger ---*/
    private static final Logger logger
        = LoggerFactory.getLogger(NeighborSet.class);

    public static IntegerOption NEIGHBOR_SET_SIZE =
            new IntegerOption(4, "-ddll-neighbor-size");
    final int capacity;
    final private LocalNode localNode;
    // 最後に送った右ノード
    private Node prevRight;
    // 最後に prevRight に送った集合
    private Set<Node> prevSet;

    /**
     * 左側近隣ノード集合．
     * 例えばノード4のleftNbrSetは {3, 0, 95, 90, 88, 4}のような順序で並ぶ．
     */
    private final ConcurrentSkipListSet<Node> leftNbrSet;

    NeighborSet(LocalNode localNode) {
        this(localNode, NEIGHBOR_SET_SIZE.value());
    }

    NeighborSet(LocalNode localNode, int capacity) {
        this.localNode = localNode;
        this.capacity = capacity;
        this.leftNbrSet = new ConcurrentSkipListSet<>(
                new NodeComparator(localNode.key));
    }

    @Override
    public String toString() {
        return leftNbrSet.toString();
    }

    /**
     * 指定されたノード集合を近隣ノード集合とする．
     * マージするのではなく，入れ替えることに注意．
     * @param nbrs 新しい近隣ノード集合
     */
    void set(Set<Node> nbrs) {
        leftNbrSet.clear();
        leftNbrSet.addAll(nbrs);
    }

    void setPrevRightSet(Node prevRight, Set<Node> nset) {
        this.prevRight = prevRight;
        this.prevSet = nset;
    }

    /**
     * 近隣ノード集合に n を追加する．
     *
     * @param n 追加するノード
     */
    void add(Node n) {
        addAll(Collections.singleton(n));
    }

    void addAll(Collection<Node> nodes) {
        leftNbrSet.addAll(nodes);
        while (leftNbrSet.size() > capacity) {
            leftNbrSet.remove(leftNbrSet.last());
        }
    }

    public List<Node> getNeighbors() {
        return new ArrayList<>(leftNbrSet);
    }

    /**
     **/
    void remove(Node removed) {
        leftNbrSet.remove(removed);
        //logger.debug("removeNode: {} is removed", removed);
    }

    /**
     * 左ノードから新しい近隣ノード集合を受信した場合に呼ばれる．
     *
     * @param newset    左ノードから受信した近隣ノード集合
     * @param right     次に転送する右ノード
     * @param limit     限界
     */
    void receiveNeighbors(Set<Node> newset, Node right, DdllKey limit) {
        set(newset);
        sendRight(right, limit);
    }

    /**
     * 右ノードに対して近隣ノード集合を送信する．
     * 右ノードが limit と等しいか，limit を超える場合は送信しない．
     * 送信する近隣ノード集合は，自ノードの近隣ノード集合に自分自身を加えたものである．
     * 
     * @param right     右ノード
     * @param limit     送信する限界キー．
     */
    void sendRight(Node right, DdllKey limit) {
        if (Node.isOrdered(localNode.key, limit, right.key)) {
            logger.debug("right node {} reached to the limit {}", right, limit);
            return;
        }
        Set<Node> set = computeNeighborSetForRightNode(right);
        if (right == prevRight && set.equals(prevSet)) {
            return;
        }
        // return if neighbor set size == 0 (for experiments)
        if (set.size() == 0) {
            return;
        }
        // propagate to the immediate right node
        localNode.post(new PropagateNeighbors(right, set, limit));
        prevRight = right;
        prevSet = set;
    }

    /**
     * 右ノードに送信するノード集合を計算する．
     *
     * @param right 右ノード
     * @return 右ノードに送信するノード集合．
     */
    Set<Node> computeNeighborSetForRightNode(Node right) {
        SortedSet<Node> set
            = new ConcurrentSkipListSet<>(new NodeComparator(right.key));
        // copy my neighbor set except nodes between me and my right node
        leftNbrSet.stream()
            .filter(node -> !Node.isOrdered(localNode.key, node.key, right.key))
            .forEach(set::add);
        if (localNode != right) {
            set.add(localNode);
        }
        while (set.size() > capacity) {
            set.remove(set.last());
        }
        // create a copy of set because set has a reference to NodeComparator
        return new HashSet<>(set);
    }

    static class NodeComparator implements Comparator<Node> {
        private final DdllKey key;
        NodeComparator(DdllKey key) {
            this.key = key;
        }
        // o1 の方が前に来るならば負の数を返す．
        public int compare(Node o1, Node o2) {
            int c = o2.key.compareTo(o1.key);
            if (c == 0) {
                return 0;
            }
            if (o1.key.compareTo(key) < 0) {
                // o1 < key
                if (o2.key.compareTo(key) < 0) {
                    // o1 < key && o2 < key
                    return c;
                }
                // o1 < key < o2
                return -1;  // o1の方が前
            }
            if (o2.key.compareTo(key) < 0) {
                // o2 < key < o1
                return +1;  // o2の方が前
            }
            return c;
        }
    }
}
