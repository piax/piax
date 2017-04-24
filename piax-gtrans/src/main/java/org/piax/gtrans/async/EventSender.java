package org.piax.gtrans.async;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;
import org.piax.gtrans.async.Event.LocalEvent;

public interface EventSender {
    void send(Event ev) throws RPCException;

    void forward(Event ev) throws RPCException;

    Endpoint getEndpoint();

    public static class EventSenderSim implements EventSender {
        private static EventSenderSim instance = new EventSenderSim();

        private EventSenderSim() {
        }

        public static EventSenderSim getInstance() {
            return instance;
        }

        @Override
        public Endpoint getEndpoint() {
            return null;
        }

        @Override
        public void send(Event ev) {
            // because sender Events and receiver Events are distinguished,
            // we have to clone the event even if sender == receiver.
            Event copy = ev.clone();
            copy.vtime = EventExecutor.getVTime() + ev.delay;
            EventExecutor.enqueue(copy);
        }

        @Override
        public void forward(Event ev) {
            Event copy = ev.clone();
            copy.vtime = EventExecutor.getVTime() + ev.delay;
            EventExecutor.enqueue(copy);
        }
    }

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
        public void send(Event ev) throws RPCException {
            if (ev instanceof LocalEvent) {
                // not to get NotSerializableException
                recv(ev);
            } else {
                EventReceiverIf stub = getStub((E) ev.receiver.addr,
                        GTransConfigValues.rpcTimeout);
                stub.recv(ev);
            }
        }

        @Override
        public void forward(Event ev) throws RPCException {
            EventReceiverIf stub = getStub((E) ev.receiver.addr,
                    GTransConfigValues.rpcTimeout);
            stub.recv(ev);
        }

        @Override
        public void recv(Event ev) {
            EventExecutor.enqueue(ev);
        }
    }
}
