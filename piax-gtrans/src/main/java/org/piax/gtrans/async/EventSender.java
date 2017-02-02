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

public interface EventSender {
    void send(Event ev) throws RPCException;

    void forward(Event ev) throws RPCException;

    public static class EventSenderSim implements EventSender {
        private static EventSenderSim instance = new EventSenderSim();

        private EventSenderSim() {
        }

        public static EventSenderSim getInstance() {
            return instance;
        }

        @Override
        public void send(Event ev) {
            Event copy = ev.clone();
            copy.vtime = EventDispatcher.getVTime() + ev.delay;
            EventDispatcher.enqueue(copy);
        }

        @Override
        public void forward(Event ev) {
            Event copy = ev.clone();
            copy.vtime = EventDispatcher.getVTime() + ev.delay;
            EventDispatcher.enqueue(copy);
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

        @Override
        public void send(Event ev) throws RPCException {
            EventReceiverIf stub = getStub((E) ev.receiver.addr,
                    GTransConfigValues.rpcTimeout);
            stub.recv(ev);
        }

        @Override
        public void forward(Event ev) throws RPCException {
            EventReceiverIf stub = getStub((E) ev.receiver.addr,
                    GTransConfigValues.rpcTimeout);
            stub.recv(ev);
        }

        @Override
        public void recv(Event ev) {
            EventDispatcher.enqueue(ev);
        }
    }
}
