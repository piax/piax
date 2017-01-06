package org.piax.gtrans.ov.ddllasync;

import java.util.Set;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventHandler;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.SuccessCallback;
import org.piax.gtrans.ov.ddll.DdllKey;

public abstract class DdllEvent {
    public static class SetR extends RequestEvent<SetR, SetRAckNak> {
        Node rNew, rCur;
        int rnewseq;
        transient SuccessCallback successCallback;

        public SetR(Node receiver, Node rNew, Node rCur, int newrseq,
                SuccessCallback job) {
            super(receiver, (SetRAckNak reply) -> {
                reply.handle();
            });
            this.rNew = rNew;
            this.rCur = rCur;
            this.rnewseq = newrseq;
            this.successCallback = job;
        }

        @Override
        public void run() {
            ((DdllStrategy) getBaseStrategy()).setr(this);
        }
    }

    public static abstract class SetRAckNak extends ReplyEvent<SetR, SetRAckNak> {
        public SetRAckNak(SetR request) {
            super(request);
        }
        public abstract void handle();
    }

    public static class SetRAck extends SetRAckNak {
        int rnewnum;
        Set<Node> nbrs;

        public SetRAck(SetR request, int rnewnum, Set<Node> nbrs) {
            super(request);
            this.rnewnum = rnewnum;
            this.nbrs = nbrs;
        }

        @Override
        public void handle() {
            ((DdllStrategy) getBaseStrategy()).setrack(this, nbrs);
        }
    }

    public static class SetRNak extends SetRAckNak {
        Node pred; // hint
        Node succ; // hint

        public SetRNak(SetR request, Node pred, Node succ) {
            super(request);
            this.pred = pred;
            this.succ = succ;
        }

        @Override
        public void handle() {
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
