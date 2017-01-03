package org.piax.gtrans.ov.ddllasync;

import java.util.Set;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventHandler;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.Node.NodeEventCallback;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.ov.ddll.DdllKey;

public abstract class DdllEvent {
    public static class SetR extends Event {
        Node rNew, rCur;
        int rnewseq;
        NodeEventCallback job;

        public SetR(Node receiver, Node rNew, Node rCur, int newrseq,
                NodeEventCallback job) {
            super(receiver);
            this.rNew = rNew;
            this.rCur = rCur;
            this.rnewseq = newrseq;
            this.job = job;
        }

        @Override
        public void run() {
            //((DdllNode) receiver.).setr(this);
            ((DdllStrategy) getBaseStrategy()).setr(this);
        }
    }

    public static class SetRAck extends Event {
        int rnewnum;
        Set<Node> nbrs;

        public SetRAck(Node q, int rnewnum, Set<Node> nbrs) {
            super(q);
            this.rnewnum = rnewnum;
            this.nbrs = nbrs;
        }

        @Override
        public void run() {
            ((DdllStrategy) getBaseStrategy()).setrack(this, nbrs);
        }
    }

    public static class SetRNak extends Event {
        Node pred; // hint
        Node succ; // hint
        SetR setr; // for retry

        public SetRNak(Node receiver, Node pred, Node succ, SetR setr) {
            super(receiver);
            this.pred = pred;
            this.succ = succ;
            this.setr = setr;
        }

        @Override
        public void run() {
            ((DdllStrategy) getBaseStrategy()).setrnak(this);
        }
    }

    public static class SetL extends Event {
        Node lNew;
        int seq;
        Set<Node> nbrs;

        public SetL(Node receiver, Node lNew, int seq, Set<Node> nbrs) {
            super(receiver);
            this.lNew = lNew;
            this.seq = seq;
            this.nbrs = nbrs;
        }

        @Override
        public void run() {
            ((DdllStrategy) getBaseStrategy()).setl(this);
        }
    }

    public static class Ping extends RequestEvent<Ping, Pong> {
        public Ping(Node receiver, EventHandler<Pong> after) {
            super(receiver, after);
        }
        @Override
        public void run() {
            ((LocalNode)receiver).post(new Pong(origin, this));
        }
    }

    public static class Pong extends ReplyEvent<Ping, Pong> {
        public Pong(Node receiver, Ping req) {
            super(req);
        }
    }

    public static class PropagateNeighbors extends Event {
        DdllKey src;
        Set<Node> propset;
        DdllKey limit;

        public PropagateNeighbors(Node receiver, DdllKey src, Set<Node> propset,
                DdllKey limit) {
            super("PropagateNeighbors", receiver);
            this.src = src;
            this.propset = propset;
            this.limit = limit;
        }

        @Override
        public void run() {
            ((DdllStrategy) getBaseStrategy()).propagateNeighbors(this);
        }
    }
}
