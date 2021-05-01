/*
 * CmrEvent.java - CmrEvent class
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame.ov.cmr;

import org.piax.ayame.Event;
import org.piax.ayame.Node;

public abstract class CmrEvent {
    public static class CmrJoin extends Event {
        Node a;

        public CmrJoin(Node receiver, Node a) {
            super(receiver);
            this.a = a;
        }

        @Override
        public void run() {
            ((CmrStrategy) getBaseStrategy()).cmrjoin(this);
        }
    }

    public static class CmrGrant extends Event {
        Node a;

        public CmrGrant(Node receiver, Node a) {
            super(receiver);
            this.a = a;
        }

        @Override
        public void run() {
            ((CmrStrategy) getBaseStrategy()).cmrgrant(this);
        }
    }

    public static class CmrAck extends Event {
        Node a;

        public CmrAck(Node receiver, Node a) {
            super(receiver);
            this.a = a;
        }

        @Override
        public void run() {
            ((CmrStrategy) getBaseStrategy()).cmrack(this);
        }
    }

    public static class CmrDone extends Event {
        public CmrDone(Node receiver) {
            super(receiver);
        }

        @Override
        public void run() {
            ((CmrStrategy) getBaseStrategy()).cmrdone(this);
        }
    }

    public static class CmrRetry extends Event {
        public CmrRetry(Node receiver) {
            super(receiver);
        }

        @Override
        public void run() {
            ((CmrStrategy) getBaseStrategy()).cmrretry(this);
        }
    }
    

    public static class CmrJoinLater extends Event {
        Node pred;
        public CmrJoinLater(Node receiver, long delay, Node pred) {
            super(receiver, delay);
            this.pred = pred;
        }

        @Override
        public void run() {
            ((CmrStrategy) getBaseStrategy()).cmrjoinlater(this);
        }
    }
}
