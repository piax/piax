package org.piax.ayame.ov.chord;

import java.util.concurrent.CompletableFuture;

import org.piax.ayame.Event.Lookup;
import org.piax.ayame.Event.LookupDone;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.NodeStrategy;
import org.piax.ayame.ov.chord.ChordEvent.Stabilize;

public class ChordStrategy extends NodeStrategy {
    public static class ChordNodeFactory extends NodeFactory {
        @Override
        public void setupNode(LocalNode node) {
            node.pushStrategy(new ChordStrategy());
        }
        @Override
        public String toString() {
            return "Chord";
        }
    }

    public static int STABILIZE_PERIOD = 1000;

    @Override
    public String toStringDetail() {
        return "N" + n.key + "(succ=" + n.succ + ", pred=" + n.pred + ")";
    }

    @Override
    public void initInitialNode() {
        n.succ = n;
        n.pred = n;
        n.post(new Stabilize(n, getInitialPeriod()));
    }

    @Override
    public void join(LookupDone l, 
            CompletableFuture<Boolean> joinFuture) {
        join(l.succ);
        joinFuture.complete(true);
    }

    public void join(Node succ) {
        n.pred = null;
        n.succ = succ;
        n.post(new Stabilize(n, getInitialPeriod()));
    }

    public void handleLookup(Lookup l) {
        if (Node.isIn2(l.key, n.key, n.succ.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            n.forward(n.succ, l);
        }
    }

    private int getInitialPeriod() {
        int min = 300;
        int r = (int) (Math.random() * 700);
        int p = min + r;
        return p;//STABILIZE_PERIOD/2;
    }
}
