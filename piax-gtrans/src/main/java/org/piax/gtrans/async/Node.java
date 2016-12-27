package org.piax.gtrans.async;

import org.piax.gtrans.ov.ddll.DdllKey;

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

    public DdllKey key;
    public final int latency;

    public Node(DdllKey ddllkey, int latency) {
        this.key = ddllkey;
        this.latency = latency;
    }

    @Override
    public String toString() {
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
