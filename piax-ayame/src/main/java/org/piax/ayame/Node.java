/*
 * Node.java - The Node in Ayame  
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import org.piax.common.DdllKey;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.util.ConcurrentReferenceHashMap;

public class Node implements Comparable<Node>, Serializable {
    public static enum NodeMode {
        /** not inserted */
        OUT, INSERTING,
        /** inserted */
        INSERTED,
        /** deleting */
        DELETING, GRACE, DELETED,
    };

    // postでlatencyとして指定すると，ネットワーク遅延時間後にイベントが実行される
    public final static long NETWORK_LATENCY = -1L;

    public final DdllKey key;
    public final Endpoint addr;
    public final PeerId peerId;

    // we have to guard `instances' with synchronized block
    private static Map<DdllKey, Node> instances
        = new ConcurrentReferenceHashMap<>(16,
                ConcurrentReferenceHashMap.ReferenceType.WEAK,
                ConcurrentReferenceHashMap.ReferenceType.WEAK);

    public static synchronized void resetInstances() {
        instances.clear();
    }

    public static synchronized Node getInstance(DdllKey ddllkey, Endpoint ep) {
        Node n = instances.get(ddllkey);
        if (n == null) {
            n = new Node(ddllkey, ep);
        }
        return n;
    }

    public static synchronized Node getInstance(DdllKey ddllkey) {
        return instances.get(ddllkey);
    }

    public static Node getWildcardInstance(Endpoint ep) {
        return new Node(null, ep);
    }

    public static synchronized LocalNode getAnyLocalNode(Endpoint addr) {
        Optional<Node> anyNode = instances.values().stream()
                .filter(v -> (v instanceof LocalNode)
                        && ((LocalNode)v).mode == NodeMode.INSERTED
                        && ((LocalNode)v).addr.equals(addr))
                .findFirst();
        return (LocalNode) anyNode.orElse(null);
    }

    protected Node(DdllKey ddllkey, Endpoint ep) {
        this.key = ddllkey;
        this.addr = ep;
        if (key != null) {
            this.peerId = ddllkey.getPeerId();
        } else {
            this.peerId = null;
        }
        synchronized (Node.class) {
            if (ddllkey != null) {
                if (instances.containsKey(ddllkey)) {
                    assert !(this instanceof LocalNode);
                } else {
                    instances.put(ddllkey, this);
                }
            }
        }
    }
    
    public PeerId getPeerId() {
        return peerId;
    }

    @Override
    public String toString() {
        if (key == null) {
            return "N*(" + addr + ")";
        }
        return "N" + key;
    }

    public String toStringDetail() {
        return toString();
        //topStrategy.toStringDetail();
    }

    @Override
    public int compareTo(Node o) {
        return key.compareTo(o.key);
    }
    
    /**
     * replace this instance with corresponding Node object on deserialization.
     * 
     * @return Node instance
     * @throws ObjectStreamException deserialization exception
     */
    private Object readResolve() throws ObjectStreamException {
        Node repl = Node.getInstance(this.key, this.addr);
        return repl;
    }

    // x in (y, z]
    public static boolean isIn(DdllKey x, DdllKey y, DdllKey z) {
        return isOrdered(y, false, x, z, true);
    }

    // x in [y, z)
    public static boolean isIn2(DdllKey x, DdllKey y, DdllKey z) {
        return isOrdered(y, true, x, z, false);
    }

    // x in (y, z)
    public static boolean isIn3(DdllKey x, DdllKey y, DdllKey z) {
        return isOrdered(y, false, x, z, false);
    }

    public static boolean isOrdered(DdllKey a, DdllKey b, DdllKey c) {
        if (a.compareTo(b) <= 0 && b.compareTo(c) <= 0) {
            return true;
        }
        if (b.compareTo(c) <= 0 && c.compareTo(a) <= 0) {
            return true;
        }
        if (c.compareTo(a) <= 0 && a.compareTo(b) <= 0) {
            return true;
        }
        return false;
    }

    public static boolean isOrdered(DdllKey from, boolean fromInclusive,
            DdllKey val, DdllKey to, boolean toInclusive) {
        if (from.compareTo(to) == 0 && (fromInclusive ^ toInclusive)) {
            return true;
        }
        boolean rc = isOrdered(from, val, to);
        if (rc) {
            if (from.compareTo(val) == 0) {
                rc = fromInclusive;
            }
        }
        if (rc) {
            if (val.compareTo(to) == 0) {
                rc = toInclusive;
            }
        }
        return rc;
    }

    @FunctionalInterface
    public static interface LinkChangeEventCallback {
        public void run(Node prev, Node now);
    }

    /*public static class Test {
        Integer key;
        public Test(Integer key) {
            this.key = key;
        }
        @Override
        public String toString() {
            return "key=" + key;
        }
    }*/
    
    /*public static void main(String[] args) {
        ConcurrentReferenceHashMap<Integer, Test> map =
                new ConcurrentReferenceHashMap<>(16,
                        ConcurrentReferenceHashMap.ReferenceType.WEAK, 
                        ConcurrentReferenceHashMap.ReferenceType.WEAK);
        Integer k = 100001;
        Test t = new Test(k);
        map.put(k, t);
        System.out.println("map=" + map);
        System.out.println(map.get(k));
        t = null;
        k = null;
        System.gc(); 
        System.out.println("map=" + map);
    }*/
    
    /*public static void main(String[] args) {
        CompletableFuture<Integer> f = new CompletableFuture<>();
        f.thenAccept((x) -> {System.out.println("Hey!" + x);}).thenRun(() -> {System.out.println("Fin");});
        f.exceptionally(exc -> {System.out.println("x"); return 0;});
        //f.<Integer>handle((x, y) -> {System.out.println("x=" + x + ", y=" + y);return 0;});
        f.completeExceptionally(new NullPointerException("n!"));
        System.out.println("hoge");
    }*/
}
