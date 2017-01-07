package org.piax.gtrans.async;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.piax.gtrans.async.EventException.TimeoutException;
import org.piax.gtrans.ov.ddll.DdllKey;

/**
 * base class of any event
 */
public abstract class Event implements Comparable<Event>, Serializable {
    private static final long serialVersionUID = 6144568542654208895L;

    private static int count = 0; 

    public String type;
    public Node origin;
    public Node sender;
    public Node receiver;

    public long vtime;
    private final int serial;
    
    private final int eventId = System.identityHashCode(this);

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

    public String getType() {
        return type;
    }

    public String toStringMessage() {
        return type;
    }

    @Override
    public String toString() {
        long rem = vtime - EventDispatcher.getVTime();
        return "[id=" + getEventId() + ", vt=" + vtime + "(rem=" + rem + "), " + toStringMessage()
            + " from " + origin + " to " + receiver + "]";
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
    
    public void beforeSendHook() {
        // empty
    }

    public void beforeRunHook() {
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

    public static class ScheduleEvent extends Event {
        Runnable after;
        public ScheduleEvent(Node receiver, long delay, Runnable after) {
            super(receiver, delay);
            this.after = after;
        }
        @Override
        public void run() {
            after.run();
        }
        @Override
        public String toString() {
            return "ScheduleEvent";
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
        private static Map<Integer, RequestEvent<?, ?>> requestMap =
                new HashMap<>();
        final transient EventHandler<U> after;
        transient ScheduleEvent timeoutEvent; 
        public RequestEvent(Node receiver, EventHandler<U> after) {
            super(receiver);
            this.after = after;
        }

        @Override
        public void beforeSendHook() {
            registerRequestEvent(this);
            this.timeoutEvent = EventDispatcher.sched(NetworkParams.NETWORK_TIMEOUT,
                    () -> {
                        this.failureCallback.run(new TimeoutException());
                    });
        }
        
        /*
         * handling `requestMap' should be in synchronized block because the
         * upper layer might be multi-threaded.
         */
        private synchronized static void registerRequestEvent(RequestEvent<?, ?> ev) {
            requestMap.put(ev.getEventId(), ev);
        }

        public synchronized static RequestEvent<?, ?> removeRequestEvent(int id) {
            RequestEvent<?, ?> ev = requestMap.remove(id);
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
        U extends ReplyEvent<T, U>> extends Event {
        public transient /*final*/T req;
        private final int reqEventId;
        public ReplyEvent(T req) {
            super(req.origin);
            this.req = req;
            this.reqEventId = req.getEventId();
            this.route.addAll(req.route);
            this.route.remove(this.route.size() - 1);
            this.routeWithFailed.addAll(req.routeWithFailed);
        }

        @Override
        public void beforeRunHook() {
            if (req.timeoutEvent != null) {
                EventDispatcher.cancelEvent(req.timeoutEvent);
                req.timeoutEvent = null;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            req.after.handle((U)this);
        }

        @SuppressWarnings("unchecked")
        private void readObject(ObjectInputStream stream) throws IOException,
            ClassNotFoundException {
            stream.defaultReadObject();
            RequestEvent<?, ?> r = RequestEvent.removeRequestEvent(reqEventId);
            assert r != null;
            this.req = (T)r;
            assert this.req.after != null;
        }
    }

    public static class Lookup extends RequestEvent<Lookup, LookupDone> {
        public DdllKey key;
        public Node src;
        public int index;
        public boolean getFTEntry;
        public StringBuilder trace;

        public Lookup(Node receiver, DdllKey key, Node src,
                EventHandler<LookupDone> after) {
            super(receiver, after);
            this.key = key;
            this.src = src;
            //System.out.println("LookupEvent: src=" + src + ", evid="+ this.getEventId());
        }

        @Override
        public void run() {
            ((LocalNode)receiver).handleLookup(this);
        }

        @Override
        public String toStringMessage() {
            return "Lookup(key=" + key + ", src=" + src + ", index=" + index
                    + ", getFTEnt=" + getFTEntry + ")"; 
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
            return "LookupError";
        }
    }
}
