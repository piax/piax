package org.piax.ayame;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.piax.ayame.EventException.AckTimeoutException;
import org.piax.ayame.EventException.TimeoutException;
import org.piax.ayame.Node.NodeMode;
import org.piax.common.DdllKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * base class of any event
 */
public abstract class Event implements Comparable<Event>, Serializable, Cloneable {
    private static final long serialVersionUID = 6144568542654208895L;
    private static final Logger logger = LoggerFactory.getLogger(Event.class);
    private String type;
    public Node origin;
    public Node sender;
    public Node receiver;

    public long vtime;
    int serial; // filled by EventExecutor#enqueue();

    // private int eventId = System.identityHashCode(this);
    private static AtomicInteger nextEventId = new AtomicInteger(0x10000000);
    private int eventId = nextEventId.getAndIncrement();

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
        //long rem = vtime - EventExecutor.getVTime();
        StringBuilder buf = new StringBuilder("[id=" + getEventId()
            + ", vt=" + vtime
            // + "(rem=" + rem + "), "
            + ", "
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
        return Integer.compare(serial, o.serial);
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
                System.err.println("TimerEvent: got " + e + " while executing job");
                e.printStackTrace();
                System.exit(1);
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
            EventExecutor.cancelEvent(this);
        }
        public boolean isExecuted() {
            return executed;
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
                    logger.debug("already acked: ackEventId={}", ackEventId);
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
     * @param <T> type of a request event
     * @param <U> type of the corresponding reply event
     */
    public static abstract class StreamingRequestEvent<T extends StreamingRequestEvent<T, U>,
        U extends ReplyEvent<T, U>> extends RequestEvent<T, U> {
        transient Consumer<U> replyReceiver;
        transient Consumer<Throwable> exceptionReceiver;
        
        public StreamingRequestEvent(Node receiver, boolean isReceiverHalf) {
            super(receiver);
            this.isReceiverHalf = isReceiverHalf;
            super.onReply((rep, exc) -> {
                logger.trace("SreamingRequestEvent: complete! {}, {}", rep,
                        exc.toString());
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
        
        /*@Override
        public CompletableFuture<U> getCompletableFuture() {
            throw new UnsupportedOperationException("don't use getCompletableFuture()");
        }*/

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

        /*public CompletableFuture<U> getCompletableFuture() {
            return this.future;
        }*/

        /**
         * assign a reply handler for this request.
         *
         * @param handler reply handler
         */
        public void onReply(BiConsumer<U, Throwable> handler) {
            // change Throwable to EventException (?)
            this.future.handle((rep, exc) -> {
                if (exc != null) {
                    logger.debug("onReply: got {}", exc.toString());
                    // replyが得られなかったとしても，途中のノードの故障による場合も
                    // あるため，故障ノードには追加しない．
                    // (送信先ノードが故障している場合は AckTimeout で追加される
                    // ため，ここで追加しなくても問題ない)
                }
                handler.accept(rep, exc);
                return false;
            }).exceptionally(exc -> {
                // CompletableFuture#handle(handler) conceals exceptions in
                // handlers.  Not to miss such exceptions and make it easy to 
                // debug, we catch them and terminate the process here.
                if (exc instanceof CompletionException) {
                    exc = exc.getCause();
                }
                System.err.println("got exception in onReply handler: " + exc
                        + ", req=" + this);
                exc.printStackTrace();
                System.err.println("exit");
                System.exit(1);
                return null;
            });
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
            logger.trace("cleanup() called for {}", this);
            cleanup.stream().forEach(r -> r.run());
            cleanup.clear();
        }

        public void receiveReply(U reply) {
            cleanup();
            // if the request has already been timed-out, the following
            // future.complete(reply) does nothing and the reply message is
            // ignored.
            // assert !future.isDone();
            future.complete(reply);
        }
        
        public void sendAck(LocalNode n) {
            n.post(new AckEvent(this, this.sender));
        }

        @Override
        public boolean beforeRunHook(LocalNode n) {
            super.beforeRunHook(n);
            this.isReceiverHalf = true;
            this.local = n;
            this.cleanup = new ArrayList<>();
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
            sendAck(n);
        }

        // override if necessary
        protected long getAckTimeoutValue() {
            return NetworkParams.ACK_TIMEOUT;
        }

        // override if necessary
        protected long getReplyTimeoutValue() {
            return NetworkParams.REPLY_TIMEOUT;
        }
 
        private void prepareForAck(LocalNode n) {
            long acktimeout = getAckTimeoutValue();
            if (acktimeout != 0) {
                // addMaybeFailedNode must be called in prior to failureCallback
                // because addMaybeFailedNode is used for registering the failed 
                // node and failureCallback relies on this.
                registerNotAckedEvent(n, this);
                cleanup.add(() -> removeNotAckedEvent(n, getEventId()));
                assert this.failureCallback != null;
                this.ackTimeoutEvent = EventExecutor.sched(
                        "acktimer-" + getEventId(),
                        acktimeout,
                        () -> {
                            if (n.mode == NodeMode.DELETED) {
                                return;
                            }
                            if (receiver != n) {
                                n.addMaybeFailedNode(receiver);
                            }
                            this.failureCallback.run(new AckTimeoutException(receiver));
                        });
                cleanup.add(() -> EventExecutor.cancelEvent(ackTimeoutEvent));
            }
        }

        private void prepareForReply(LocalNode n) {
            long replytimeout = getReplyTimeoutValue();
            if (replytimeout != 0) {
                registerRequestEvent(n, this);
                cleanup.add(() -> removeRequestEvent(n, getEventId()));
                this.replyTimeoutEvent = EventExecutor.sched(
                        "replyTimer-" + getEventId(),
                        replytimeout,
                        () -> {
                            if (n.mode == NodeMode.DELETED) {
                                return;
                            }
                            RequestEvent<?, ?> ev1 = removeNotAckedEvent(n, getEventId());
                            RequestEvent<?, ?> ev2 = removeRequestEvent(n, getEventId());
                            if (ev1 == null) {
                                // probably we have already received the ack
                                logger.debug("removeNotAck: not found: {}", getEventId());
                            }
                            assert ev2 != null;
                            logger.debug("reply timed out: {}", this);
                            // In reply timeout case, unlike ack timeout case,
                            // the receiver node is not considered to be failed.
                            this.failureCallback.run(new TimeoutException());
                        });
                // System.out.println("schedule reply timer: " + replyTimeoutEvent);
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

        @SuppressWarnings("unchecked")
        @Override
        public boolean beforeRunHook(LocalNode n) {
            // restore transient "req" field
            RequestEvent<?, ?> r = RequestEvent.lookupRequestEvent(n, reqEventId);
            if (r == null) {
                logger.debug("ReplyEvent#beforeRunHook: reqEventId={}: not found", reqEventId);
                return false;
            }
            // System.out.println("ReplyEvent#beforeRunHook: reqEventId=" + reqEventId);
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
        // true when the sender node is requesting the receiver node's local
        // FTEntry.
        public boolean fill;

        public Lookup(Node receiver, DdllKey key) {
            super(receiver);
            this.key = key;
        }

        @Override
        public void run() {
            getLocalNode().getTopStrategy().handleLookup(this);
        }

        @Override
        public String toStringMessage() {
            return "Lookup(key=" + key + ", fill=" + fill +")";
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
    
    @SuppressWarnings("rawtypes")
    public static class ErrorEvent extends ReplyEvent {
        final public EventException reason;

        @SuppressWarnings("unchecked")
        public ErrorEvent(RequestEvent req, EventException reason) {
            super(req);
            this.reason = reason;
        }
        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            logger.debug("ErrorEvent");
            TimerEvent ev = req.ackTimeoutEvent;
            if (ev != null) {
                // for now, ErrorEvent is sent if the recipient node is DELETED.
                // we treat this ErrorEvent as ACK timed out.
                logger.debug("invoke ackTimeoutEvent handler");
                EventExecutor.cancelEvent(ev);
                req.ackTimeoutEvent = null;
                ev.run();
            }
        }
        @Override
        public String toStringMessage() {
            return "ErrorEvent";
        }
    }
}
