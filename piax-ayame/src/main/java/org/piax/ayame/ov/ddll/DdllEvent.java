package org.piax.ayame.ov.ddll;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.piax.ayame.Event;
import org.piax.ayame.Event.ReplyEvent;
import org.piax.ayame.Event.RequestEvent;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.ov.ddll.DdllStrategy.SetRType;
import org.piax.common.DdllKey;

public abstract class DdllEvent {
    @FunctionalInterface
    public static interface SetRJob extends Serializable {
        void run(LocalNode node);
    }
    public static class SetR extends RequestEvent<SetR, SetRAckNak> {
        final Node rNew, rCur;
        final LinkNum rnewseq;
        final SetRType type;
        final SetRJob setRJob;

        public SetR(Node receiver, SetRType type, Node rNew, Node rCur,
                LinkNum newrseq, SetRJob job) {
            super(receiver);
            this.type = type;
            this.rNew = rNew;
            this.rCur = rCur;
            this.rnewseq = newrseq;
            this.setRJob = job;
        }

        @Override
        public void run() {
            DdllStrategy s = DdllStrategy.getDdllStrategy(getLocalNode());
            s.setr(this);
        }
    }

    public static abstract class SetRAckNak extends ReplyEvent<SetR, SetRAckNak> {
        public SetRAckNak(SetR request) {
            super(request);
        }
    }

    public static class SetRAck extends SetRAckNak {
        final LinkNum rnewnum;
        final Set<Node> nbrs;

        public SetRAck(SetR request, LinkNum rnewnum, Set<Node> nbrs) {
            super(request);
            this.rnewnum = rnewnum;
            this.nbrs = nbrs;
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
            DdllStrategy s = DdllStrategy.getDdllStrategy(getLocalNode());
            s.setl(this);
        }

        @Override
        public String toStringMessage() {
            return "SetL[lNew=" + lNew + ", seq=" + seq + ", nbrs=" + nbrs + "]";
        }
    }

    public static class PropagateNeighbors extends Event {
        Set<Node> propset;
        DdllKey limit;

        public PropagateNeighbors(Node receiver, Set<Node> propset,
                DdllKey limit) {
            super("PropagateNeighbors", receiver);
            this.propset = propset;
            this.limit = limit;
        }

        @Override
        public void run() {
            DdllStrategy s = DdllStrategy.getDdllStrategy(getLocalNode());
            s.propagateNeighbors(this);
        }
    }
    
    public static class GetCandidates extends RequestEvent<GetCandidates, GetCandidatesResponse> {
        final Node node;
        public GetCandidates(Node receiver, Node node) {
            super(receiver);
            this.node = node;
        }
        @Override
        public void run() {
            LocalNode n = getLocalNode();
            List<Node> candidates = n.getNodesForFix(node.key);
            // System.out.println("GetFixCandidates: returns " + candidates);
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

        @Override
        public String toStringMessage() {
            return "GetCandidatesResponse[cands="
                    + candidates + ", succ=" + succ + "]";
        }
    }
}
