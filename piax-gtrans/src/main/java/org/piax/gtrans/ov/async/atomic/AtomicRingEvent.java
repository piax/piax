package org.piax.gtrans.ov.async.atomic;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.async.atomic.AtomicRingStrategy.Status;
import org.piax.gtrans.ov.async.ddll.DdllStrategy;

public abstract class AtomicRingEvent {
    public static class JoinReq extends Event {
        Node d;

        public JoinReq(Node receiver, Node q) {
            super(receiver);
            this.d = q;
        }

        @Override
        public void run() {
            ((AtomicRingStrategy) getBaseStrategy()).joinreq(this);
        }
    }

    public static class JoinPoint extends Event {
        Node p;

        public JoinPoint(Node q, Node p) {
            super(q);
            this.p = p;
        }

        @Override
        public void run() {
            ((AtomicRingStrategy) getBaseStrategy()).joinpoint(this);
        }
    }

    public static class NewSucc extends Event {
        Node q;

        public NewSucc(Node receiver, Node q) {
            super(receiver);
            this.q = q;
        }

        @Override
        public void run() {
            LocalNode local = getLocalNode();
            Event ev = new NewSuccAck(local.succ, q);
            local.post(ev);
            local.succ = q;
        }
    }

    public static class NewSuccAck extends Event {
        Node q;

        public NewSuccAck(Node receiver, Node q) {
            super(receiver);
            this.q = q;
        }

        @Override
        public void run() {
            AtomicRingStrategy r = (AtomicRingStrategy) getBaseStrategy();
            r.unlock();
            r.joinForward = false;
            Event ev = new JoinDone(q);
            getLocalNode().post(ev);
        }
    }

    public static class JoinDone extends Event {
        public JoinDone(Node receiver) {
            super(receiver);
        }

        @Override
        public void run() {
            AtomicRingStrategy r = (AtomicRingStrategy) getBaseStrategy();
            r.status = Status.INSIDE;
            r.unlock();
            //receiver.inserted();
            r.joinFuture.complete(true);
        }
    }

    public static class RetryJoin extends Event {
        public RetryJoin(Node receiver) {
            super(receiver);
        }

        @Override
        public void run() {
            AtomicRingStrategy r = (AtomicRingStrategy) getBaseStrategy();
            r.status = Status.OUT;
            r.unlock();
            long delay = 0;
            switch (DdllStrategy.retryMode.value()) {
            case IMMED:
                delay = 0;
                break;
            case RANDOM:
                delay = EventExecutor.random()
                    .nextInt(AtomicRingStrategy.JOIN_RETRY_DELAY)
                    * NetworkParams.HALFWAY_DELAY;
                break;
            case CONST:
                delay = AtomicRingStrategy.JOIN_RETRY_DELAY * NetworkParams.HALFWAY_DELAY;
                break;
            }
            if (delay == 0) {
                r.join1(sender);
            } else {
                getLocalNode().post(new JoinLater(receiver, delay, sender));
            }
        }
    }

    public static class JoinLater extends Event {
        Node succ;

        public JoinLater(Node receiver, long delay, Node succ) {
            super(receiver, delay);
            this.succ = succ;
        }

        @Override
        public void run() {
            AtomicRingStrategy r = (AtomicRingStrategy) getBaseStrategy();
            if (r.status == Status.OUT) {
                if (sender != receiver) {
                    System.out.println("prompted");
                }
                //r.join0(r.introducer);
                r.join1(succ);
            } else if (sender != receiver) {
                System.out.println("unprompted");
            }
        }
    }
}
