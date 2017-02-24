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
    
    public LocalNode getLocalNode() {
        return (LocalNode)this.receiver;
    }

    public NodeStrategy getBaseStrategy() {
        return getLocalNode().getBaseStrategy();
    }

    public NodeStrategy getTopStrategy() {
        return getLocalNode().getTopStrategy();
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

    public boolean beforeRunHook(LocalNode n) {
        return true;
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
        final String name;
        final Consumer<TimerEvent> job;
        final long period;
        private boolean canceled = false;
        private boolean executed = false;
        public TimerEvent(String name, long initial, long period, 
                Consumer<TimerEvent> job) {
            super(null, initial);
            if (name != null) {
                this.name = name;
            } else {
                this.name = "NONAME-" + System.identityHashCode(this);
            }
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
        @Override
        public String toStringMessage() {
            return "TimerEvent[" + name + "]";
        }
        public void cancel() {
            canceled = true;
        }
        public boolean isExecuted() {
            return executed;
        }
    }

    public static class LocalEvent extends Event {
        final Runnable run;
        public LocalEvent(LocalNode receiver, Runnable run) {
            super(receiver, 0);
            this.run = run;
        }
        @Override
        public void run() {
            run.run();
        }
    }

    public static class AckEvent extends Event {
        int ackEventId = 0;
        protected boolean expectMuptipleAck = false;
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
                if (!expectMuptipleAck) {
                    System.out.println("already acked: ackEventId=" + ackEventId);
                }
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
     * a request that allows multiple replies
     *
     * @param <T>
     * @param <U>
     */
    public static abstract class StreamingRequestEvent<T extends StreamingRequestEvent<T, U>,
        U extends ReplyEvent<T, U>> extends RequestEvent<T, U> {
        transient Consumer<U> replyReceiver;
        transient Consumer<Throwable> exceptionReceiver;
        
        public StreamingRequestEvent(Node receiver, boolean isReceiverHalf) {
            super(receiver);
            this.isReceiverHalf = isReceiverHalf;
            super.getCompletableFuture().whenComplete((rep, exc) -> {
                System.out.println("SreamingRequestEvent: complete! " + rep + ", " + exc);
                assert rep == null;
                exceptionReceiver.accept(exc);
            });
        }

        protected void setReplyReceiver(Consumer<U> receiver) {
            assert this.replyReceiver == null;
            this.replyReceiver = receiver;
        }

        protected void setExceptionReceiver(Consumer<Throwable> receiver) {
            assert this.exceptionReceiver == null;
            this.exceptionReceiver = receiver;
        }
        
        @Override
        public CompletableFuture<U> getCompletableFuture() {
            throw new UnsupportedOperationException("don't use getCompletableFuture()");
        }

        @Override
        public void receiveReply(U reply) {
            replyReceiver.accept(reply);
        }

        @Override
        protected Event clone() {
            StreamingRequestEvent<?, ?> ev
                = (StreamingRequestEvent<?, ?>)super.clone();
            ev.replyReceiver = null;
            ev.exceptionReceiver = null;
            return ev;
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
        protected boolean isReceiverHalf = false;
        transient CompletableFuture<U> future;
        transient TimerEvent replyTimeoutEvent, ackTimeoutEvent;
        protected transient List<Runnable> cleanup = new ArrayList<>();
        transient LocalNode local;

        public RequestEvent(Node receiver) {
            super(receiver);
            this.future = new CompletableFuture<U>();
        }

        public CompletableFuture<U> getCompletableFuture() {
            return this.future;
        }

        @Override
        protected Event clone() {
            RequestEvent<?, ?> ev = (RequestEvent<?, ?>)super.clone();
            ev.future = null;
            ev.cleanup = new ArrayList<>();
            ev.replyTimeoutEvent = null;
            ev.ackTimeoutEvent = null;
            return ev;
        }

        /*
         * cleanup the instance at sender half
         */
        public void cleanup() {
            System.out.println("cleanup is called!: " + this);

            cleanup.stream().forEach(r -> r.run());
            cleanup.clear();
        }

        public void receiveReply(U reply) {
            cleanup();
            future.complete(reply);
        }

        @Override
        public boolean beforeRunHook(LocalNode n) {
            super.beforeRunHook(n);
            this.isReceiverHalf = true;
            this.local = n;
            return true;
        }

        @Override
        public void beforeSendHook(LocalNode n) {
            prepareForAck(n);
            prepareForReply(n);
        }

        @Override
        public void beforeForwardHook(LocalNode n) {
            prepareForAck(n);
            // when a request message is forwarded, we send AckEvent to the
            // sender node.
            n.post(new AckEvent(this, this.sender));
        }

        // override if necessary
        protected long getAckTimeoutValue() {
            return NetworkParams.NETWORK_TIMEOUT;
        }

        // override if necessary
        protected long getReplyTimeoutValue() {
            return NetworkParams.NETWORK_TIMEOUT;
        }
 
        private void prepareForAck(LocalNode n) {
            long acktimeout = getAckTimeoutValue();
            if (acktimeout != 0) {
                // foundFailedNode must be called in prior to failureCallback
                // because foundFailedNode is used for registering the failed 
                // node and failureCallback relies on this.
                registerNotAckedEvent(n, this);
                this.ackTimeoutEvent = EventExecutor.sched(
                        "acktimer-" + getEventId(),
                        acktimeout,
                        () -> {
                            if (receiver != n) {
                                n.getTopStrategy().foundFailedNode(receiver);
                            }
                        });
                cleanup.add(() -> EventExecutor.cancelEvent(ackTimeoutEvent));
            }
        }

        private void prepareForReply(LocalNode n) {
            long replytimeout = getReplyTimeoutValue();
            if (replytimeout != 0) {
                registerRequestEvent(n, this);
                Runnable r = () -> removeRequestEvent(n, getEventId());
                cleanup.add(r);
                this.replyTimeoutEvent = EventExecutor.sched(
                        "replyTimer-" + getEventId(),
                        replytimeout,
                        () -> {
                            RequestEvent<?, ?> ev1 = removeNotAckedEvent(n, getEventId());
                            RequestEvent<?, ?> ev2 = removeRequestEvent(n, getEventId());
                            if (ev1 == null) {
                                // probably we have already received the ack
                                System.out.println("removeNotAck: not found: " + getEventId());
                            }
                            //assert ev1 != null;
                            assert ev2 != null;
                            System.out.println("reply timed out: " + this);
                            this.failureCallback.run(new TimeoutException());
                        });
                System.out.println("schedule reply timer: " + replyTimeoutEvent);
                cleanup.add(() -> EventExecutor.cancelEvent(replyTimeoutEvent));
            }
        }

        /*
         * handling `requestMap' should be in synchronized block because the
         * upper layer might be multi-threaded.
         */
        public synchronized static void registerRequestEvent(LocalNode n,
                RequestEvent<?, ?> ev) {
            n.ongoingRequests.put(ev.getEventId(), ev);
        }

        public synchronized static RequestEvent<?, ?>
        removeRequestEvent(LocalNode n, int id) {
            RequestEvent<?, ?> ev = n.ongoingRequests.remove(id);
            return ev;
        }

        public synchronized static RequestEvent<?, ?>
        lookupRequestEvent(LocalNode n, int id) {
            RequestEvent<?, ?> ev = n.ongoingRequests.get(id);
            return ev;
        }

        private static void registerNotAckedEvent(LocalNode n, 
                RequestEvent<?, ?> ev) {
            n.unAckedRequests.put(ev.getEventId(), ev);
        }

        public static RequestEvent<?, ?> removeNotAckedEvent(LocalNode n,
                int id) {
            RequestEvent<?, ?> ev = n.unAckedRequests.remove(id);
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
        public boolean beforeRunHook(LocalNode n) {
            // restore transient "req" field
            RequestEvent<?, ?> r = RequestEvent.lookupRequestEvent(n, reqEventId);
            if (r == null) {
                System.out.println("ReplyEvent#beforeRunHook: reqEventId=" + reqEventId + ": not found");
                return false;
            }
            System.out.println("ReplyEvent#beforeRunHook: reqEventId=" + reqEventId);
            this.req = (T)r;
            return true;
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
            req.receiveReply((U)this);
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
