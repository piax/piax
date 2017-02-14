package org.piax.gtrans.async;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.piax.gtrans.async.EventException.TimeoutException;
import org.piax.gtrans.ov.ddll.DdllKey;

/**
 * base class of any event
 */
public abstract class Event implements Comparable<Event>, Serializable, Cloneable {
    private static final long serialVersionUID = 6144568542654208895L;

    private static int count = 0; 

    private String type;
    public Node origin;
    public Node sender;
    public Node receiver;

    public long vtime;
    private final int serial;
    
    private int eventId = System.identityHashCode(this);

    public long delay;
    transient public FailureCallback failureCallback;    // run at sender node
    public List<Node> route = new ArrayList<>();
    public List<Node> routeWithFailed = new ArrayList<>();
    public Event(Node receiver) {
        this(receiver, Node.NETWORK_LATENCY);
    }

    public Event(String type, Node receiver) {
        this(type, receiver, Node.NETWORK_LATENCY);
    }

    public Event(Node receiver, long delay) {
        this(null, receiver, delay);
        this.type = this.getClass().getSimpleName();
    }

    public Event(String type, Node receiver, long delay) {
        this.receiver = receiver;
        this.delay = delay;
        this.type = type;
        this.serial = count++;
    }
    
    @Override
    protected Event clone() {
        try {
            Event copy = (Event) super.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new Error(e);
        }
    }

    public String getType() {
        return type;
    }

    public String toStringMessage() {
        return type;
    }

    @Override
    public String toString() {
        long rem = vtime - EventExecutor.getVTime();
        StringBuilder buf = new StringBuilder("[id=" + getEventId()
            + ", vt=" + vtime
            + "(rem=" + rem + "), "
            + toStringMessage());
        if (origin != null) {
            buf.append(", orig " + origin);
        }
        if (receiver != null) {
            buf.append(", to " + receiver);
        }
        buf.append("]");
        return buf.toString();
    }

    public int getEventId() {
        return eventId;
    }
    
    public LocalNode getNodeImpl() {
        return (LocalNode)this.receiver;
    }

    public NodeStrategy getBaseStrategy() {
        return getNodeImpl().baseStrategy;
    }

    public int hops() {
        return route.size() - 1;
    }

    public void beforeSendHook(LocalNode n) {
        // empty
    }

    public void beforeForwardHook(LocalNode n) {
        // empty
    }

    public void beforeRunHook(LocalNode n) {
        // empty
    }

    public abstract void run();

    @Override
    public int compareTo(Event o) {
        int x = Long.compare(vtime, o.vtime);
        if (x != 0) {
            return x;
        }
        if (serial != o.serial) {
            return serial - o.serial;
        }
        return 0;
    }

    public static class TimerEvent extends Event {
        final Consumer<TimerEvent> job;
        final long period;
        private boolean canceled = false;
        private boolean executed = false;
        public TimerEvent(long initial, long period, Consumer<TimerEvent> job) {
            super(null, initial);
            this.job = job;
            this.period = period;
        }
        @Override
        public void run() {
            executed = true;
            try {
                if (!canceled) {
                    job.accept(this);
                }
            } catch (Throwable e) {
                System.out.println("TimerTask: got " + e + " while executing job");
                e.printStackTrace();
            } finally {
                if (!canceled && period > 0) {
                    executed = false;
                    vtime = EventExecutor.getVTime() + period;
                    EventExecutor.enqueue(this);
                }
            }
            
        }
        public void cancel() {
            canceled = true;
        }
        public boolean isExecuted() {
            return executed;
        }
    }

    public static class AckEvent extends Event {
        int ackEventId = 0;
        public AckEvent(RequestEvent<?, ?> req, Node receiver) {
            super(receiver);
            if (req.sender == receiver) {
                // the above condition does not hold when a ReplyEvent 
                // (subclass of AckEvent) is sent for a RequestEvent
                // which is indirectly received.
                this.ackEventId = req.getEventId();
            }
        }
        @Override
        public void run() {
            if (ackEventId == 0) {
                return;
            }
            RequestEvent<?, ?> req = RequestEvent.removeNotAckedEvent(
                    (LocalNode)receiver, ackEventId);
            if (req == null) {
                System.out.println("already acked: ackEventId=" + ackEventId);
                return;
            }
            TimerEvent ev = req.ackTimeoutEvent;
            if (ev != null) {
                EventExecutor.cancelEvent(ev);
            }
        }

        public String toStringMessage() {
            return getType() + ", ackId=" + ackEventId;
        }
    }

    /**
     * base class of a request event
     *
     * @param <T> the type of request event
     * @param <U> the type of reply event
     */
    public static abstract class RequestEvent<T extends RequestEvent<T, U>,
            U extends ReplyEvent<T, U>> extends Event {
        final transient CompletableFuture<U> future;
        transient TimerEvent replyTimeoutEvent, ackTimeoutEvent; 
        public RequestEvent(Node receiver) {
            super(receiver);
            this.future = new CompletableFuture<U>();
        }

        public CompletableFuture<U> getCompletableFuture() {
            return this.future;
        }

        @Override
        public void beforeSendHook(LocalNode n) {
            // foundFailedNode must be called in prior to failureCallback
            // because foundFailedNode is used for registering the failed 
            // node and failureCallback relies on this.
            registerNotAckedEvent(n, this);
            this.ackTimeoutEvent = EventExecutor.sched(NetworkParams.NETWORK_TIMEOUT,
                    () -> {
                        if (receiver != n) {
                            n.topStrategy.foundFailedNode(receiver);
                        }
                    });

            registerRequestEvent(n, this);
            this.replyTimeoutEvent = EventExecutor.sched(NetworkParams.NETWORK_TIMEOUT,
                    () -> {
                        this.failureCallback.run(new TimeoutException());
                    });
        }

        @Override
        public void beforeForwardHook(LocalNode n) {
            registerNotAckedEvent(n, this);
            this.ackTimeoutEvent = EventExecutor.sched(NetworkParams.NETWORK_TIMEOUT,
                    () -> {
                        if (receiver != n) {
                            n.topStrategy.foundFailedNode(receiver);
                        }
                    });
            // when a request message is forwarded, we send AckEvent to the
            // sender node.
            n.post(new AckEvent(this, this.sender));
        }
 
        /*
         * handling `requestMap' should be in synchronized block because the
         * upper layer might be multi-threaded.
         */
        private synchronized static void registerRequestEvent(LocalNode n, RequestEvent<?, ?> ev) {
            n.ongoingRequests.put(ev.getEventId(), ev);
        }

        public synchronized static RequestEvent<?, ?> removeRequestEvent(LocalNode n, int id) {
            RequestEvent<?, ?> ev = n.ongoingRequests.remove(id);
            return ev;
        }

        private static void registerNotAckedEvent(LocalNode n, RequestEvent<?, ?> ev) {
            n.unAckedEvents.put(ev.getEventId(), ev);
        }

        public static RequestEvent<?, ?> removeNotAckedEvent(LocalNode n, int id) {
            RequestEvent<?, ?> ev = n.unAckedEvents.remove(id);
            return ev;
        }
    }

    /**
     * base class of a reply event
     *
     * @param <T> the type of request event
     * @param <U> the type of reply event
     */
    public static abstract class ReplyEvent<T extends RequestEvent<T, U>, 
        U extends ReplyEvent<T, U>> extends AckEvent {
        public transient T req;
        private final int reqEventId;
        public ReplyEvent(T req) {
            super(req, req.origin);
            this.req = req;
            this.reqEventId = req.getEventId();
            this.route.addAll(req.route);
            this.route.remove(this.route.size() - 1);
            this.routeWithFailed.addAll(req.routeWithFailed);
        }

        @Override
        public void beforeRunHook(LocalNode n) {
            // restore transient "req" field
            RequestEvent<?, ?> r = RequestEvent.removeRequestEvent(n, reqEventId);
            assert r != null;
            this.req = (T)r;
            //assert this.req.after != null;

            // remove timeout event
            if (req.replyTimeoutEvent != null) {
                EventExecutor.cancelEvent(req.replyTimeoutEvent);
                req.replyTimeoutEvent = null;
            }
        }
        
        @Override
        public void beforeSendHook(LocalNode n) {
            if (receiver != req.sender) {
                n.post(new AckEvent(req, req.sender));
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            super.run();
            req.future.complete((U)this);
        }
    }

    public static class Lookup extends RequestEvent<Lookup, LookupDone> {
        public DdllKey key;
        public Node src;
        // true when the sender node is requesting the receiver node's local
        // FTEntry.
        public boolean fill;
        public StringBuilder trace;

        public Lookup(Node receiver, DdllKey key, Node src) {
            super(receiver);
            this.key = key;
            this.src = src;
        }

        @Override
        public void run() {
            ((LocalNode)receiver).handleLookup(this);
        }

        @Override
        public String toStringMessage() {
            return "Lookup(key=" + key + ", src=" + src + ", index="
                    + fill +")";
        }
    }

    public static class LookupDone extends ReplyEvent<Lookup, LookupDone> {
        public Node pred;
        public Node succ;

        public LookupDone(Lookup req, Node pred, Node succ) {
            super(req);
            this.pred = pred;
            this.succ = succ;
            assert Node.isOrdered(pred.key, true, req.key, succ.key, false);
        }
    }
    
    public static class ErrorEvent extends ReplyEvent {
        final public EventException reason;

        public ErrorEvent(RequestEvent req, EventException reason) {
            super(req);
            this.reason = reason;
        }
        @Override
        public void run() {
            System.out.println("ErrorEvent");
            if (req.failureCallback != null) {
                FailureCallback h = req.failureCallback;
                req.failureCallback = null;
                h.run(reason);
            }
        }
        @Override
        public String toStringMessage() {
            return "ErrorEvent";
        }
    }
}
