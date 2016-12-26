package org.piax.gtrans.async;

public class Node implements Comparable<Node> {
    public static enum NodeMode {
        /** not inserted */
        OUT, TO_BE_INSERTED, INSERTING,
        /** inserted */
        INSERTED,
        /** deleting */
        DELETING, GRACE, DELETED,
        /** failure */
        FAILED
    };

    // postでlatencyとして指定すると，ネットワーク遅延時間後にイベントが実行される
    public final static long NETWORK_LATENCY = -1L;

    public final int id;
    public final int latency;

    public Node(int latency, int id) {
        this.id = id;
        this.latency = latency;
    }
    
    public String toStringDetail() {
        return "[" + id + "]"; 
        //topStrategy.toStringDetail();
    }

    @Override
    public int compareTo(Node o) {
        return id - o.id;
    }

    public int latency(Node receiver) {
        if (this == receiver) {
            return 0;
        }
        long l = latency + receiver.latency;
        double jitter = 1.0 + (Sim.rand.nextDouble()
                * 2 * NetworkParams.JITTER.value()) - NetworkParams.JITTER.value();
        return (int)(l * jitter);
    }

    // x in (y, z]
    public static boolean isIn(int x, int y, int z) {
        return isOrdered(y, false, x, z, true);
        /*if (y == z) {
            return true;
        } else if (y < z) {
            return y < x && x <= z;
        } else {
            return y < x || x <= z;
        }*/
    }

    // x in [y, z)
    public static boolean isIn2(int x, int y, int z) {
        return isOrdered(y, true, x, z, false);
        /*if (y == z) {
            return true;
        } else if (y <= z) {
            return y <= x && x < z;
        } else {
            return y <= x || x < z;
        }*/
    }

    // x in (y, z)
    public static boolean isIn3(int x, int y, int z) {
        return isOrdered(y, false, x, z, false);
        /*if (y == z) {
            return true;
        } else if (y <= z) {
            return y < x && x < z;
        } else {
            return y < x || x < z;
        }*/
    }

    public static boolean isOrdered(int a, int b, int c) {
        if (a <= b && b <= c) {
            return true;
        }
        if (b <= c && c <= a) {
            return true;
        }
        if (c <= a && a <= b) {
            return true;
        }
        return false;
    }

    public static boolean isOrdered(int from, boolean fromInclusive,
            int val, int to, boolean toInclusive) {
        if (from == to && (fromInclusive ^ toInclusive)) {
            return true;
        }
        boolean rc = isOrdered(from, val, to);
        if (rc) {
            if (from == val) {
                rc = fromInclusive;
            }
        }
        if (rc) {
            if (val == to) {
                rc = toInclusive;
            }
        }
        return rc;
    }

    @FunctionalInterface
    public static interface NodeEventCallback {
        public void run(NodeImpl n);
    }
    @FunctionalInterface
    public static interface LinkChangeEventCallback {
        public void run(Node prev, Node now);
    }
}
