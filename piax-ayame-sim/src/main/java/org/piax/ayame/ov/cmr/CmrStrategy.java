/*
 * CmrStrategy.java - An Implementation of "Concurrent Maintenance of Rings"
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame.ov.cmr;

import java.util.concurrent.CompletableFuture;

import org.piax.ayame.Event.Lookup;
import org.piax.ayame.Event.LookupDone;
import org.piax.ayame.LocalNode;
import org.piax.ayame.NetworkParams;
import org.piax.ayame.Node;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.NodeStrategy;
import org.piax.ayame.ov.cmr.CmrEvent.CmrAck;
import org.piax.ayame.ov.cmr.CmrEvent.CmrDone;
import org.piax.ayame.ov.cmr.CmrEvent.CmrGrant;
import org.piax.ayame.ov.cmr.CmrEvent.CmrJoin;
import org.piax.ayame.ov.cmr.CmrEvent.CmrJoinLater;
import org.piax.ayame.ov.cmr.CmrEvent.CmrRetry;
import org.piax.ayame.ov.ddll.DdllStrategy;
import org.piax.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of "Concurrent Maintenance of Rings", based on the following
 * paper.
 * 
 * <blockquote>
 * X. Li, J. Misra, and C. G. Plaxton, "Concurrent maintenance of rings."
 * Distributed Comp., vol. 19, no. 2, pp. 126–148, 2006.
 * </blockquote>
 */
public class CmrStrategy extends NodeStrategy {
    public static class CmrNodeFactory extends NodeFactory {
        @Override
        public void setupNode(LocalNode node) {
            node.pushStrategy(new CmrStrategy());
        }
        @Override
        public String toString() {
            return "CMR";
        }
    }
    private static final Logger logger
        = LoggerFactory.getLogger(CmrStrategy.class);
    public static int JOIN_RETRY_DELAY = 2;
    public static enum Status {
        IN, OUT, JNG, LVG, BUSY
    }
    Status status = Status.OUT;
    Node t;
    CompletableFuture<Void> joinFuture;

    public static void load() {
    }

    @Override
    public String toStringDetail() {
        return "N" + n.key + "(succ=" + n.succ + ", pred=" + n.pred + ", status="
                + status + ")";
    }

    @Override
    public void handleLookup(Lookup l) {
        if (Node.isIn2(l.key, n.key, n.succ.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            n.forward(n.succ, l);
        }
    }

    @Override
    public void initInitialNode() {
        n.succ = n;
        n.pred = n;
        status = Status.IN;
    }

    @Override
    public CompletableFuture<Void> join(LookupDone l) {
        this.joinFuture = new CompletableFuture<>();
        join(l.pred, l.succ);
        return this.joinFuture;
    }

    // T_1^j
    public void join(Node predecessor, Node successor) {
        n.pred = predecessor;    // XXX: 元論文では null
        n.succ = successor;  // XXX: 元論文では null
        status = Status.JNG;
        n.post(new CmrJoin(predecessor, n));
    }

    void cmrjoin(CmrJoin j) {
        if (!Node.isIn(j.a.key, n.key, n.succ.key)) {
            n.post(new CmrRetry(j.a));
        } else if (status == Status.IN) {
            n.post(new CmrGrant(n.succ, j.a));
            t = n.succ;
            n.succ = j.a;
            status = Status.BUSY;
        } else {
            n.post(new CmrRetry(j.a));
        }
    }

    public void cmrgrant(CmrGrant msg) {
        if (n.pred == msg.origin) {
            n.post(new CmrAck(msg.a, n.pred));
            n.pred = msg.a;
        } else {
            n.post(new CmrAck(msg.a, null));
            n.pred = msg.origin;
        }
    }

    public void cmrack(CmrAck msg) {
        if (status == Status.JNG) {
            n.succ = msg.origin;
            n.pred = msg.a;
            status = Status.IN;
            n.post(new CmrDone(n.pred));
            joinFuture.complete(null);
        } else if (status == Status.LVG) {
            // not implemented
        }
    }

    public void cmrdone(CmrDone cmrDone) {
        status = Status.IN;
        t = null;
    }

    public void cmrretry(CmrRetry cmrRetry) {
        if (status == Status.JNG) {
            status = Status.OUT;
            // retry!
            logger.trace("receive Retry: join retry");
            //joinUsingIntroducer(introducer);
            
            long delay = 0;
            switch (DdllStrategy.retryMode.value()) {
            case IMMED:
                delay = 0;
                break;
            case RANDOM:
                delay = RandomUtil.getSharedRandom().nextInt(JOIN_RETRY_DELAY)
                    * NetworkParams.HALFWAY_DELAY;
                break;
            case CONST:
                delay = JOIN_RETRY_DELAY * NetworkParams.HALFWAY_DELAY;
                break;
            }
            if (delay == 0) {
                n.joinAsync(n.pred);
            } else {
                n.post(new CmrJoinLater(n, delay, n.pred));
            }
            
        } else if (status == Status.LVG) {
            status = Status.IN;
        }
    }

    public void cmrjoinlater(CmrJoinLater msg) {
        if (status == Status.OUT) {
            n.joinAsync(msg.pred);
        }
    }
}
