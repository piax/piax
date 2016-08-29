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
 * Proposal3に続き、
 * xxTransport, xxListenerにnarrowingを忠実に入れたケースで、
 * 定義されたinterfaceをimplementsする際に、実装したくないメソッドが見えてしまう問題への対処として、
 * xxTransportでは対応するxxListenerを、xxListenerでは対応するxxTransportを型パラメータ
 * にして回避した方法。
 * 問題点としては、xxTransport及びxxListener間の継承関係が失われる。
 */
public class Proposal4 {

    interface _Transport<D extends Destination, L> {
        void setListener(ObjectId upper, L listener);

        L getListener(ObjectId upper);

        void send(ObjectId sender, ObjectId receiver, D dst, Object msg)
                throws ProtocolUnsupportedException, IOException;
    }

    interface Transport<D extends Destination> extends
            _Transport<D, TransportListener<D>> {
    }

    interface _TransportListener<D extends Destination, T, R> {
        void onReceive(T trans, R rmsg);
    }

    interface TransportListener<D extends Destination> extends
            _TransportListener<D, Transport<D>, ReceivedMessage<D>> {
    }

    interface ChannelTransport<D extends Destination> extends Transport<D> {
        Channel newChannel(ObjectId sender, ObjectId receiver, D dst,
                boolean isDuplex, int timeout)
                throws ProtocolUnsupportedException, IOException;
    }

    interface _RequestTransport<D extends Destination, L> extends
            _Transport<D, L> {
        FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
                Object msg, int timeout) throws ProtocolUnsupportedException,
                IOException;
    }

    interface RequestTransport<D extends Destination> extends
            _RequestTransport<D, RequestTransportListener<D>> {
    }

    interface _RequestTransportListener<D extends Destination, T, R>
            extends _TransportListener<D, T, R> {
        FutureQueue<?> onReceiveRequest(T trans, R rmsg);
    }

    interface RequestTransportListener<D extends Destination> extends
            _RequestTransportListener<D, Transport<D>, ReceivedMessage<D>> {
    }

    interface _Overlay<D extends Destination, K extends Key, L> extends
            _RequestTransport<D, L> {
    }

    interface Overlay<D extends Destination, K extends Key> extends
            _Overlay<D, K, OverlayListener<D, K>> {
    }

    interface _OverlayListener<D extends Destination, K extends Key, T, R>
            extends _RequestTransportListener<D, T, R> {
        // void onReceive(Overlay<D, K> ov, OvReceivedMessage<D, K> rmsg);
        // FutureQueue<?> onReceiveRequest(Overlay<D, K> ov,
        // OvReceivedMessage<D, K> rmsg);
    }

    interface OverlayListener<D extends Destination, K extends Key> extends
            _OverlayListener<D, K, Overlay<D, K>, OverlayReceivedMessage<D, K>> {
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

    static class OverlayReceivedMessage<D extends Destination, K extends Key>
            extends ReceivedMessage<D> {
        public Collection<K> getMatchedKeys() {
            return null;
        }
    }

    // 具体的実装

    static class FooOverlay<D extends PeerId, P extends PeerId> implements
            Overlay<D, P> {
        public void setListener(ObjectId upper, OverlayListener<D, P> listener) {
        }
        public OverlayListener<D, P> getListener(ObjectId upper) {
            return null;
        }
        public void send(ObjectId sender, ObjectId receiver, D dst, Object msg)
                throws ProtocolUnsupportedException, IOException {
        }
        public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
                D dst, Object msg, int timeout)
                throws ProtocolUnsupportedException, IOException {
            return null;
        }
    }

    static class FooOverlayListener<D extends PeerId, P extends PeerId>
            implements OverlayListener<D, P> {
        public void onReceive(Overlay<D, P> trans,
                OverlayReceivedMessage<D, P> rmsg) {
        }
        public FutureQueue<?> onReceiveRequest(Overlay<D, P> trans,
                OverlayReceivedMessage<D, P> rmsg) {
            return null;
        }
    }

    static class DOLR<K extends Key> implements Overlay<K, K> {
        public void setListener(ObjectId upper, OverlayListener<K, K> listener) {
        }
        public OverlayListener<K, K> getListener(ObjectId upper) {
            return null;
        }
        public void send(ObjectId sender, ObjectId receiver, K dst, Object msg)
                throws ProtocolUnsupportedException, IOException {
        }
        public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
                K dst, Object msg, int timeout)
                throws ProtocolUnsupportedException, IOException {
            return null;
        }
    }

    public static void main(String[] args) {
    }
}
