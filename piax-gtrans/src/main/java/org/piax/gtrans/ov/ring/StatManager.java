/*
 * StatManager.java - A status manager
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.piax.common.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatManager {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(StatManager.class);

    public final static int REMOVE_SUSPICIOUS_NODE = 60 * 1000;
    public final static int NUMBER_OF_RTTS = 15;
    public final static double RTT_INFINITE = Double.MAX_VALUE;
    public final static double RTT_VOID = 0.0;
    public final static double RTT_UNKNOWN = 0.0;
    public final static int RTT_ROTATE_PERIOD = 10 * 1000;

    Map<Endpoint, NodeStatus> stats = new HashMap<Endpoint, NodeStatus>();
    final protected RingManager<?> manager;

    public StatManager(RingManager<?> manager) {
        this.manager = manager;
    }

    /**
     * Node 
     * @param p
     */
    public void nodeTimeout(final Endpoint p) {
        nodeAlive(p, RTT_INFINITE);
    }

    public synchronized void nodeAlive(Endpoint p, double rtt) {
        NodeStatus status = stats.get(p);
        if (status == null) {
            status = new NodeStatus(p);
            stats.put(p, status);
        }
        status.addRTT(rtt);
        logger.debug("node rtt {}, {}", rttString(rtt), status);
    }

    public boolean isPossiblyFailed(Endpoint p) {
        double score = getScore(p);
        return score == RTT_INFINITE;
    }

    public double getScore(Endpoint p) {
        NodeStatus status = stats.get(p);
        if (status == null) {
            return RTT_UNKNOWN;
        }
        double[] rtts = status.getRTTs();
        double s = 0.0;
        int n = 0;
        for (int i = 0; i < rtts.length; i++) {
            if (rtts[i] == RTT_VOID) {
                continue;
            }
            if (rtts[i] == RTT_INFINITE) {
                return RTT_INFINITE;
            }
            s += rtts[i];
            n++;
        }
        if (n > 0) {
            return s / n;
        } else {
            // RTT_VOID only!
            return RTT_UNKNOWN;
        }
    }

    private static String rttString(double rtt) {
        if (rtt == RTT_INFINITE) {
            return "T.O";
        } else if (rtt == 0.0) {
            return "N/A";
        } else {
            return new Double(rtt).toString();
        }
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (Map.Entry<Endpoint, NodeStatus> ent : stats.entrySet()) {
            buf.append(ent.getKey() + ": " + ent.getValue());
            buf.append("\n");
        }
        return buf.toString();
    }

    /**
     * Endpointのネットワーク統計情報を格納するクラス．
     * 
     * 過去一定期間の
     * ・平均RTTと最大RTT
     * ・タイムアウト数
     * 最初に経路表に現れてからの経過時間
     */
    public class NodeStatus {
        final Endpoint endpoint;
        ScheduledFuture<?> future;
        double[] rtts = new double[NUMBER_OF_RTTS - 1];
        // the last one is here
        double rtt_acc;
        int rtt_num;

        public NodeStatus(Endpoint p) {
            this.endpoint = p;
            manager.schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized (NodeStatus.this) {
                        System.arraycopy(rtts, 0, rtts, 1, rtts.length - 1);
                        rtts[0] = getRecentAverageRTT();
                        rtt_acc = 0.0;
                        rtt_num = 0;
                    }
                    logger.debug("update NodeStatus {} {}:", endpoint,
                            NodeStatus.this.toString());
                }
            }, RTT_ROTATE_PERIOD, RTT_ROTATE_PERIOD);
        }

        synchronized void addRTT(double rtt) {
            if (rtt == RTT_INFINITE || this.rtt_acc == RTT_INFINITE) {
                this.rtt_acc = RTT_INFINITE;
            } else {
                this.rtt_acc += rtt;
            }
            this.rtt_num++;
            logger.debug("addRTT: rtt={}, rtt_acc={}, rtt_num={}",
                    rttString(rtt), rtt_acc, rtt_num);
        }

        synchronized double getRecentAverageRTT() {
            if (rtt_num == 0) {
                return RTT_VOID;
            }
            if (rtt_acc == RTT_INFINITE) {
                return RTT_INFINITE;
            }
            return rtt_acc / rtt_num;
        }

        synchronized double[] getRTTs() {
            double[] ret = new double[NUMBER_OF_RTTS + 1];
            System.arraycopy(this.rtts, 0, ret, 1, this.rtts.length - 1);
            ret[0] = getRecentAverageRTT();
            return ret;
        }

        @Override
        public String toString() {
            double[] r = getRTTs();
            StringBuilder buf = new StringBuilder("[");
            for (int i = 0; i < r.length; i++) {
                buf.append(rttString(r[i]));
                if (i != r.length - 1) {
                    buf.append(", ");
                }
            }
            buf.append("]");
            return buf.toString();
        }
    }
}
