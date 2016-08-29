/*
 * NodeArray4Test.java - NodeArray4Test implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: NodeArray4Test.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ddll;

import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ddll.Node.Mode;

/**
 * NodeArray4Test.java - NodeArray4Test implementation of DDLL.
 * 配列のi番目が key i を持つnode配列を作る前提で、
 * nodeの状態ダンプ機能を実装
 */
public class NodeArray4Test {
    /*
     * あらかじめ、
     * nodes[i] には、key i を持つ前提でnode配列を作る必要がある。
     */
    public static Node[] nodes;
    
    static Node left(int n) {
        Node node = nodes[n];
        int l = (Integer) node.left.key.primaryKey;
        return nodes[l];
    }
    
    static Node right(int n) {
        Node node = nodes[n];
        int r = (Integer) node.right.key.primaryKey;
        return nodes[r];
    }
    
    static Node leftRight(int n) {
        int l = (Integer) nodes[n].left.key.primaryKey;
        int r = (Integer) nodes[l].right.key.primaryKey;
        return nodes[r];
    }
    
    static Node rightLeft(int n) {
        int r = (Integer) nodes[n].right.key.primaryKey;
        int l = (Integer) nodes[r].left.key.primaryKey;
        return nodes[l];
    }

    static boolean isOnline(int n) {
        return nodes[n].isOnline();
    }
    
    public static boolean dump(int n, boolean erroneousOnly) {
        String x = "";
        Node node = nodes[n];

        if (!node.isOnline()) {
            if (!erroneousOnly) 
                System.out.println("| " + n + ": FAILED");
            return true;
        }
        if (node.getMode() == Node.Mode.OUT) {
            if (!erroneousOnly) 
                System.out.println("| " + n + ": NOT_INSERTED");
            return true;
        }
        if (!left(n).isOnline() || left(n).getMode() == Node.Mode.OUT) {
            x += ", left failed or deleted";
        } else if (leftRight(n) != node) {
            x += ", left inconsistent (left's right = "
                    + leftRight(n).key.primaryKey + ")";
        } else if (left(n).rNum == null || !left(n).rNum.equals(node.lNum)) {
            x += ", num inconsistent with left";
        }
        if (!right(n).isOnline() || right(n).getMode() == Node.Mode.OUT) {
            x += ", right failed or deleted";
        } else if (rightLeft(n) != node) {
            x += ", right inconsistent (right's left = "
                    + rightLeft(n).key.primaryKey + ")";
        } else if (! right(n).lNum.equals(node.rNum)) {
            x += ", num inconsistent with right";
        }
        if (!erroneousOnly || !x.equals("")) {
            System.out.println("| " + n + ": " + node + x);
        }
        if (!x.equals("")) {
            return false;
        }
        return true;
    }
    
//    public static boolean traverse() {
//        List<Integer> ring = new ArrayList<Integer>();
//        
//        int ix = 0;
//        String err = "";
//        ring.add(ix);
//        while (true) {
//            int next = (Integer) nodes[ix].right.key;
//            if (!nodes[next].isOnline() || nodes[next].mode == Node.Mode.OUT) {
//                err = "\nfailed or deleted at p:" + next;
//                break;
//            }
//            if ((Integer) nodes[next].left.key != ix) {
//                err = "\nleft wrong at p:" + next;
//                break;
//            }
//            // TODO
//            if (nodes[ix].rNum == null || !nodes[ix].rNum.equals(nodes[next].lNum)) {
//                err = "\ngeneration inconsistent with right at p:" + ix;
//                break;
//            }
//            if (next <= ix) break;
//            ix = next;
//            ring.add(ix);
//        }
//        System.out.println("current list size:" + ring.size());
//        System.out.println(ring + err);
//        return err.equals("");
//    }
    public static boolean dump() {
        return dump(false);
    }
    
    public static boolean dump(boolean erroneousOnly) {
        if (nodes == null) {
            return true;
        }
        int off = 0;
        int inserted = 0;
        int notInserted = 0;
        int others = 0;
        for (int i = 0; i < nodes.length; i++) {
            Node node = nodes[i];
            if (!node.isOnline()) off++;
            else {
                if (node.getMode() == Node.Mode.IN) inserted++;
                else if (node.getMode() == Node.Mode.OUT) notInserted++;
                else others++;
            }
        }
        
        System.out.println("-Nodes-------------------------------");
        System.out.printf("inserted:%d, deleted:%d, offline:%d, others:%d%n",
                inserted, notInserted, off, others);
        boolean consis = true;
        for (int i = 0; i < nodes.length; i++) {
            if (!dump(i, erroneousOnly)) {
                consis = false;
            }
        }
        System.out.println("-------------------------------------");
        return consis;
    }

    private static boolean isAlive(Node n) {
        return n.isOnline() &&
            (n.getMode() == Mode.IN || n.getMode() == Mode.INS || n.getMode() == Mode.DEL 
                    || n.getMode() == Mode.DELWAIT);
    }
    
    public static Stat getLiveLeftStat(Node me) {
        int kk = (Integer) me.key.primaryKey;
        int lIx = (kk - 1 + nodes.length) % nodes.length;
        while (!isAlive(nodes[lIx])) {
            lIx = (lIx - 1 + nodes.length) % nodes.length;
        }
        Node n = nodes[lIx];
        if (!n.isOnline()) {
            System.out.println("*********** oi **************");
            return null;
        }
        return new Stat(n.getMode(), n.me, n.left, n.right, n.rNum);
    }

    public static InsertPoint findLiveLeft2(Node me) {
        Node n = me;
        for (int i = 0; i < NeighborSet.getDefaultNeighborSetSize(); i++) {
            int ix = (Integer) n.left.key.primaryKey;
            n = nodes[ix];
            if (n.isOnline() && n.getMode() != Mode.OUT) {
                return new InsertPoint(n.me, n.right);
            }
        }
        return null;
    }

    public static InsertPoint findInsertPoint(Comparable<?> k) {
        int kk = (Integer) k;
        int lIx = (kk - 1 + nodes.length) % nodes.length;
        while (!isAlive(nodes[lIx])) {
            lIx = (lIx - 1 + nodes.length) % nodes.length;
        }
        int rIx = (kk + 1 + nodes.length) % nodes.length;
        while (!isAlive(nodes[rIx])) {
            rIx = (rIx + 1 + nodes.length) % nodes.length;
        }
        return new InsertPoint(nodes[lIx].me, nodes[rIx].me);
    }
}
