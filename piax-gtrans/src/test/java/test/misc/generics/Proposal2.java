package test.misc.generics;

import java.util.Collection;

import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.gtrans.FutureQueue;

/**
 * Proposal１に続き、
 * Overlayにおけるkeyを型パラメータにした場合。
 * 問題なく定義できる。
 */
public class Proposal2 {

    interface Transport {
        void setListener(ObjectId upper, TransportListener listener);
        TransportListener getListener(ObjectId upper);
    }

    interface TransportListener {
        void onReceive(Transport trans, ReceivedMessage rmsg);
    }

    interface RequestTransport extends Transport {
        void setListener(ObjectId upper, RequestTransportListener listener);
        RequestTransportListener getListener(ObjectId upper);
    }

    interface RequestTransportListener extends TransportListener {
        void onReceive(RequestTransport trans, ReceivedMessage rmsg);
        FutureQueue<?> onReceiveRequest(RequestTransport trans, ReceivedMessage rmsg);
    }

    interface Overlay<K extends Key> extends RequestTransport {
        void setListener(ObjectId upper, OverlayListener<K> listener);
        OverlayListener<K> getListener(ObjectId upper);
    }

    interface OverlayListener<K extends Key> extends RequestTransportListener {
        void onReceive(Overlay<K> ov, OvReceivedMessage<K> rmsg);
        FutureQueue<?> onReceiveRequest(Overlay<K> ov, OvReceivedMessage<K> rmsg);
    }

    static class ReceivedMessage {
        ObjectId getSender() {
            return null;
        }
        Endpoint getSource() {
            return null;
        }
        Object getMessage() {
            return null;
        }
    }

    static class OvReceivedMessage<K extends Key> extends ReceivedMessage {
        public Collection<K> getMatchedKeys() {
            return null;
        }
    }

    // 具体的実装
    
    static class FooOverlay<P extends PeerId> implements Overlay<P> {
        public void setListener(ObjectId upper, TransportListener listener) {
        } // 書きたくない
        public void setListener(ObjectId upper,
                RequestTransportListener listener) {
        } // 書きたくない

        public void setListener(ObjectId upper, OverlayListener<P> listener) {
        }
        public OverlayListener<P> getListener(ObjectId upper) {
            return null;
        }
    }
    
    static class FooOverlayListener<P extends PeerId> implements OverlayListener<P> {
        public void onReceive(Transport trans, ReceivedMessage rmsg) {
        } // 書かせたくない
        public void onReceive(RequestTransport trans, ReceivedMessage rmsg) {
        } // 書かせたくない
        public FutureQueue<?> onReceiveRequest(RequestTransport trans,
                ReceivedMessage rmsg) {
            return null;
        } // 書かせたくない

        public void onReceive(Overlay<P> ov, OvReceivedMessage<P> rmsg) {
        }
        public FutureQueue<?> onReceiveRequest(Overlay<P> ov,
                OvReceivedMessage<P> rmsg) {
            return null;
        }
    }

    public static void main(String[] args) {
    }
}
