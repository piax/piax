package org.piax.ayame;

import java.util.concurrent.CompletableFuture;

import org.piax.common.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface EventSender {
    static final Logger logger = LoggerFactory.getLogger(EventSender.class);
    CompletableFuture<Void> send(Event ev);

    Endpoint getEndpoint();

    public static class EventSenderSim implements EventSender {
        private static EventSenderSim instance = new EventSenderSim();
        private static LatencyProvider latencyProvider;

        private EventSenderSim() {
        }

        public static EventSenderSim getInstance() {
            return instance;
        }

        @Override
        public Endpoint getEndpoint() {
            return null;
        }

        public static void setLatencyProvider(LatencyProvider p) {
            latencyProvider = p;
        }

        @Override
        public CompletableFuture<Void> send(Event ev) {
            if (ev.delay == Node.NETWORK_LATENCY) {
                ev.delay = latency(ev.sender, ev.receiver);
            }
            ev.vtime = EventExecutor.getVTime() + ev.delay;
            if (logger.isTraceEnabled()) {
                if (ev.delay != 0) {
                    logger.trace("{} |send/forward event {}, (arrive at T{})", ev.sender, ev, ev.vtime);
                } else {
                    logger.trace("{} |send/forward event {}", ev.sender, ev);
                }
            }
            /*if (((LocalNode)ev.receiver).isFailed()) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(new IOException("remote node failure: "
                        + ev.receiver));
                return f;
            }*/
            // because sender Events and receiver Events are distinguished,
            // we have to clone the event even if sender == receiver.
            Event copy = ev.clone();
            EventExecutor.enqueue(copy);
            return CompletableFuture.completedFuture(null);
        }

        private static long latency(Node a, Node b) {
            if (EventExecutor.realtime.value()) {
                return 0;
            }
            if (a == b) {
                return 0;
            }
            if (latencyProvider == null) {
                return 100;
            }
            return latencyProvider.latency(a, b);
        }
    }
/*
    public static interface EventReceiverIf extends RPCIf {
        @RemoteCallable(Type.ONEWAY)
        void recv(Event ev) throws RPCException;
    }

    public static class EventSenderNet<E extends Endpoint>
            extends RPCInvoker<EventReceiverIf, E>
            implements EventSender, EventReceiverIf {
        public static TransportId DEFAULT_TRANSPORT_ID =
                new TransportId("GTEvent");

        public EventSenderNet(TransportId transId, ChannelTransport<E> trans)
                throws IdConflictException, IOException {
            super(transId, trans);
        }

        // getEndpoint() is implemented in the super class, RPCInvoker.

        @Override
        public CompletableFuture<Void> send(Event ev) {
            assert ev.delay == Node.NETWORK_LATENCY;
            //ev.vtime = EventExecutor.getVTime() + ev.delay;
            ev.vtime = 0;
            if (logger.isTraceEnabled()) {
                logger.trace("{}|send/forward event {}", ev.sender, ev);
            }
            @SuppressWarnings("unchecked")
            EventReceiverIf stub = getStub((E) ev.receiver.addr,
                    GTransConfigValues.rpcTimeout);
            try {
                stub.recv(ev);
            }
            catch (Exception e) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(e);
                return f;
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void recv(Event ev) {
            EventExecutor.enqueue(ev);
        }
    }
    */
}
