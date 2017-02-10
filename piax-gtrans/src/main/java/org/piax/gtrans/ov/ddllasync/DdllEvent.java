package org.piax.gtrans.ov.ddllasync;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventHandler;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.LinkNum;
import org.piax.gtrans.ov.ddllasync.DdllStrategy.FixType;

public abstract class DdllEvent {
    @FunctionalInterface
    public static interface SetRJob extends Serializable {
        void run(LocalNode node);
    }
    public static class SetR extends RequestEvent<SetR, SetRAckNak> {
        final Node rNew, rCur;
        final LinkNum rnewseq;
        final FixType type;
        final SetRJob setRJob;
        final transient Runnable success;

        public SetR(Node receiver, FixType type, Node rNew, Node rCur,
                LinkNum newrseq, SetRJob job, Runnable success) {
            super(receiver, (SetRAckNak reply) -> {
                reply.handle();
            });
            this.type = type;
            this.rNew = rNew;
            this.rCur = rCur;
            this.rnewseq = newrseq;
            this.setRJob = job;
            this.success = success;
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
        // because run() is already used (overridden) by ReplyEvent, we have
        // to use another method here.
        public abstract void handle();
    }

    public static class SetRAck extends SetRAckNak {
        final LinkNum rnewnum;
        final Set<Node> nbrs;

        public SetRAck(SetR request, LinkNum rnewnum, Set<Node> nbrs) {
            super(request);
            this.rnewnum = rnewnum;
            this.nbrs = nbrs;
        }

        @Override
        public void handle() {
            ((DdllStrategy) getBaseStrategy()).setrack(this);
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
        LinkNum seq;
        Set<Node> nbrs;

        public SetL(Node receiver, Node lNew, LinkNum seq, Set<Node> nbrs) {
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
    
    public static class GetCandidates extends RequestEvent<GetCandidates, GetCandidatesResponse> {
        final Node node;
        public GetCandidates(Node receiver, Node node, EventHandler<GetCandidatesResponse> after) {
            super(receiver, after);
            this.node = node;
        }
        @Override
        public void run() {
            LocalNode n = getNodeImpl();
            List<Node> candidates = n.getNodesForFix(node.key);
            System.out.println("GetFixCandidates: returns " + candidates);
            n.post(new GetCandidatesResponse(this, candidates, n.succ));
        }
    }

    public static class GetCandidatesResponse extends ReplyEvent<GetCandidates, GetCandidatesResponse> {
        List<Node> candidates;
        Node succ;
        public GetCandidatesResponse(GetCandidates req, List<Node> candidates,
                Node succ) {
            super(req);
            this.candidates = candidates;
            this.succ = succ;
        }
    }
}
