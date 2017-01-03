package org.piax.gtrans.async;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;

public interface EventSender {
    void send(Event ev);

    void forward(Event ev);

    public static class EventSenderSim implements EventSender {
        private static EventSenderSim instance = new EventSenderSim();

        private EventSenderSim() {
        }

        public static EventSenderSim getInstance() {
            return instance;
        }

        @Override
        public void send(Event ev) {
            EventDispatcher.enqueue(ev);
        }

        @Override
        public void forward(Event ev) {
            EventDispatcher.enqueue(ev);
        }
    }

    public static interface EventReceiverIf extends RPCIf {
        @RemoteCallable(Type.ONEWAY)
        void recv(Event ev);
    }

    public static class EventSenderNet
            extends RPCInvoker<EventReceiverIf, Endpoint>
            implements EventSender, EventReceiverIf {
        public static TransportId DEFAULT_TRANSPORT_ID =
                new TransportId("DdllAsync");

        public EventSenderNet(ChannelTransport<?> trans)
                throws IdConflictException, IOException {
            super(DEFAULT_TRANSPORT_ID, (ChannelTransport<Endpoint>) trans);
        }

        @Override
        public void send(Event ev) {
            EventReceiverIf stub =
                    getStub(ev.receiver.addr, GTransConfigValues.rpcTimeout);
            ev.beforeSendHook();
            stub.recv(ev);
        }

        @Override
        public void forward(Event ev) {
            EventReceiverIf stub =
                    getStub(ev.receiver.addr, GTransConfigValues.rpcTimeout);
            stub.recv(ev);
        }

        @Override
        public void recv(Event ev) {
            EventDispatcher.enqueue(ev);
        }
    }
}
