package org.piax.ayame.ov.atomic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.ayame.Event;
import org.piax.ayame.Event.Lookup;
import org.piax.ayame.Event.LookupDone;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.LocalNode;
import org.piax.ayame.NetworkParams;
import org.piax.ayame.Node;
import org.piax.ayame.NodeFactory;
import org.piax.ayame.NodeStrategy;
import org.piax.ayame.ov.atomic.AtomicRingEvent.JoinLater;
import org.piax.ayame.ov.atomic.AtomicRingEvent.JoinPoint;
import org.piax.ayame.ov.atomic.AtomicRingEvent.JoinReq;
import org.piax.ayame.ov.atomic.AtomicRingEvent.NewSucc;
import org.piax.ayame.ov.atomic.AtomicRingEvent.RetryJoin;
import org.piax.common.Option.BooleanOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Atomic Ring Maintenance algorithm, based on the following
 * paper.
 * 
 * <blockquote>
 * A. Ghodsi, "Distributed k-ary System: Algorithms for distributed hash tables,"
 * PhD Dissertation, KTH-Royal Institute of Technology, 2006.
 * </blockquote>
 */
public class AtomicRingStrategy extends NodeStrategy {
    public static class AtomicRingNodeFactory extends NodeFactory {
        @Override
        public void setupNode(LocalNode node) {
            node.pushStrategy(new AtomicRingStrategy());
        }
        @Override
        public String toString() {
            return "AtomicRing";
        }
    }
    
    private static final Logger logger
        = LoggerFactory.getLogger(AtomicRingStrategy.class);

    public static BooleanOption forwardPred
        = new BooleanOption(false, "-forwardPred", val -> {
            AtomicRingStrategy.JOIN_RETRY_DELAY = 2;
        });

    public static BooleanOption prompt = new BooleanOption(false, "-prompt");

    // best value for non-forwardpre mode
    public static int JOIN_RETRY_DELAY = 12;
    boolean lock = false;
    boolean joinForward = false, leaveForward = false;
    Node oldPred = null;
    List<Node> rejects = new ArrayList<Node>();
    long unlockTime;
    CompletableFuture<Boolean> joinFuture;

    public static enum Status {
        OUT, JOINREQ, JOINING, INSIDE
    };

    Status status = Status.OUT;
    
    public static void load() {
    }

    @Override
    public String toStringDetail() {
        return "N" + n.key + "(succ=" + n.succ + ", pred=" + n.pred + ", oldPred="
                + oldPred + ", lock=" + lock + ", status=" + status
                + ", joinForward=" + joinForward + ", leaveForward="
                + leaveForward + ")";
    }

    @Override
    public void initInitialNode() {
        n.succ = n;
        n.pred = n;
        status = Status.INSIDE;
    }

    public void join1(Node succ) {
        join(succ);
    }

    @Override
    public void join(LookupDone l, 
            CompletableFuture<Boolean> joinFuture) {
        join(l.succ);
        this.joinFuture = joinFuture;
    }

    public void join(Node succ) {
        lock();
        n.pred = null;
        n.succ = null;
        status = Status.JOINREQ;
        Event ev = new JoinReq(succ, n);
        n.post(ev);
    }

    void joinreq(JoinReq j) {
        if (joinForward && j.sender == oldPred) {
            logger.trace("joinreq: case1");
            n.forward(n.pred, j);
        } else if (leaveForward) {
            logger.trace("joinreq: case2");
            n.forward(n.succ, j);
        } else if (n.pred != null && n.pred != n && Node.isIn(j.d.key, n.key, n.pred.key)) {
            logger.trace("joinreq: case3");
            if (forwardPred.value()) {
                n.forward(n.pred, j);       // Improve
            } else {
                n.forward(n.succ, j);       // Original
            }
        } else {
            System.out.println("hops: " + j.route.size());
            System.out.println("route: " + j.route);
            if (lock || n.pred == null) {
                logger.trace("joinreq: case4");
                Event ev = new RetryJoin(j.origin);
                n.post(ev);
                rejects.add(j.origin);
            } else {
                logger.trace("joinreq: case5");
                joinForward = true;
                lock();
                Event ev = new JoinPoint(j.d, n.pred);
                n.post(ev);
                oldPred = n.pred;
                n.pred = j.d;
            }
        }
    }

    public void joinpoint(JoinPoint j) {
        status = Status.JOINING;
        n.pred = j.p;
        n.succ = j.origin;
        Event ev = new NewSucc(n.pred, n);
        n.post(ev);
    }

    public void handleLookup(Lookup l) {
        if (joinForward && l.origin == oldPred) {
            n.forward(n.pred, l);
            //post(new Lookup(pred, Event.ONEWAY_DELAY, l.id, l.src));
        } else if (leaveForward) {
            n.forward(n.succ, l);
            //post(new Lookup(succ, Event.ONEWAY_DELAY, l.id, l.src));
        } else if (n.pred != null && Node.isIn(l.key, n.pred.key, n.key)) {
            n.post(new LookupDone(l, n, n.succ));
        } else {
            n.forward(n.succ, l);
            //post(new Lookup(succ, Event.ONEWAY_DELAY, l.id, l.src));
        }
    }

    public void lock() {
        assert !lock;
        lock = true;
        if (unlockTime != 0) {
            logger.trace(this + ": unlocked duration="
                    + (EventExecutor.getVTime() - unlockTime));
        }
    }

    public void unlock() {
        assert lock;
        lock = false;
        unlockTime = EventExecutor.getVTime();
        if (rejects.isEmpty()) {
            return;
        }
        System.out.println(this + ": rejected=" + rejects);
        if (prompt.value()) {
            Node x = rejects.get(0);
            n.post(new JoinLater(x, NetworkParams.HALFWAY_DELAY, n));
        }
        rejects.clear();
    }
}
