package test.misc.generics;

import java.io.IOException;
import java.util.Collection;

import org.piax.common.Destination;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.ProtocolUnsupportedException;

/**
 * Proposal2に続き、
 * さらに、TransportにおけるDestinationの指定も型パラメータにした場合。
 * 問題なく定義できる。
 */
public class Proposal5 {

    interface Transport<D extends Destination> {
        <L> void setListener(ObjectId upper, L listener);
        <L> L getListener(ObjectId upper);
        void send(ObjectId sender, ObjectId receiver, D dst, Object msg)
                throws ProtocolUnsupportedException, IOException;
    }

    interface TransportListener<D extends Destination> {
        <T> void onReceive(T trans, ReceivedMessage<D> rmsg);
    }
    
    interface ChannelTransport<D extends Destination> extends Transport<D> {
        Channel newChannel(ObjectId sender, ObjectId receiver, D dst,
                boolean isDuplex, int timeout)
                throws ProtocolUnsupportedException, IOException;
    }
    interface RequestTransport<D extends Destination> extends Transport<D> {
//        void setListener(ObjectId upper, RequestTransportListener<D> listener);
//        RequestTransportListener<D> getListener(ObjectId upper);
        FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
                Object msg, int timeout) throws ProtocolUnsupportedException,
                IOException;
    }

    interface RequestTransportListener<D extends Destination> extends
            TransportListener<D> {
//        void onReceive(RequestTransport<D> trans, ReceivedMessage<D> rmsg);
        <T> FutureQueue<?> onReceiveRequest(T trans,
                ReceivedMessage<D> rmsg);
    }

    interface Overlay<D extends Destination, K extends Key> extends
            RequestTransport<D> {
//        void setListener(ObjectId upper, OverlayListener<D, K> listener);
//        OverlayListener<D, K> getListener(ObjectId upper);
    }

    interface OverlayListener<D extends Destination, K extends Key> extends
            RequestTransportListener<D> {
//        void onReceive(Overlay<D, K> ov, OvReceivedMessage<D, K> rmsg);
//        FutureQueue<?> onReceiveRequest(Overlay<D, K> ov,
//                OvReceivedMessage<D, K> rmsg);
    }

    static class ReceivedMessage<D extends Destination> {
        ObjectId getSender() {
            return null;
        }

        D getSource() {
            return null;
        }

        Object getMessage() {
            return null;
        }
    }

    static class OvReceivedMessage<D extends Destination, K extends Key>
            extends ReceivedMessage<D> {
        public Collection<K> getMatchedKeys() {
            return null;
        }
    }

    // 具体的実装

    static class FooOverlay<D extends PeerId, P extends PeerId> implements
            Overlay<D, P> {
        public FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
                Object msg, int timeout) throws ProtocolUnsupportedException,
                IOException {
            return null;
        }
        public void send(ObjectId sender, ObjectId receiver, D dst, Object msg)
                throws ProtocolUnsupportedException, IOException {
        }

        @Override
        public <L> void setListener(ObjectId upper, L listener) {
        }

        @Override
        public <L> L getListener(ObjectId upper) {
            return null;
        }
    }

    static class FooOverlayListener<D extends PeerId, P extends PeerId> implements
            OverlayListener<D, P> {
        @Override
        public <T> FutureQueue<?> onReceiveRequest(T trans,
                ReceivedMessage<D> rmsg) {
            return null;
        }

        @Override
        public <T> void onReceive(T trans, ReceivedMessage<D> rmsg) {
        }
    }

    static class DOLR<K extends Key> implements Overlay<K, K> {
        public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
                K dst, Object msg, int timeout)
                throws ProtocolUnsupportedException, IOException {
            return null;
        }
        public void send(ObjectId sender, ObjectId receiver, K dst, Object msg)
                throws ProtocolUnsupportedException, IOException {
        }
        @Override
        public <L> void setListener(ObjectId upper, L listener) {
        }
        @Override
        public <L> L getListener(ObjectId upper) {
            return null;
        }
    }
    
    public static void main(String[] args) {
    }
}
