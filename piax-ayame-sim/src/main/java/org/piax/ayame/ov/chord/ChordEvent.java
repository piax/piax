/*
 * ChordEvent.java - ChordEvent class
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame.ov.chord;

import org.piax.ayame.Event;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;

public abstract class ChordEvent {
    public static class Stabilize extends Event {
        public Stabilize(Node receiver, int delay) {
            super(receiver, delay);
        }

        @Override
        public void run() {
            // x = successor.predecessor;
            // if (x ∈ (n, successor))
            //    successor = x;
            // successor.notify(n);
            LocalNode n = getLocalNode();
            n.post(new GetPred(n.succ));
            n.post(new Stabilize(n, ChordStrategy.STABILIZE_PERIOD));
        }
    }

    public static class GetPred extends Event {
        public GetPred(Node receiver) {
            super(receiver);
        }

        @Override
        public void run() {
            LocalNode n = getLocalNode();
            n.post(new GetPredResp(origin, n.pred));
        }
    }

    public static class GetPredResp extends Event {
        Node x;

        public GetPredResp(Node receiver, Node x) {
            super(receiver);
            this.x = x;
        }

        @Override
        public void run() {
            LocalNode n = getLocalNode();
            if (Node.isIn(x.key, n.key, n.succ.key)) {
                n.succ = x;
            }
            n.post(new Notify(n.succ, n));
        }
    }

    public static class Notify extends Event {
        Node x;

        public Notify(Node receiver, Node x) {
            super(receiver);
            this.x = x;
        }

        @Override
        public void run() {
            // if (predecessor is nil or n′ ∈ (predecessor, n))
            //    predecessor = n′;
            LocalNode n = getLocalNode();
            if (n.pred == null
                    || Node.isIn3(x.key, n.pred.key, n.key)) {
                n.pred = x;
            }
        }
    }
}
