package test.misc.generics;

import java.util.Collection;

import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.gtrans.FutureQueue;

/**
 * xxTransport, xxListenerにnarrowingを忠実に入れたもの。
 * 定義されたinterfaceをimplementsする際に、実装したくないメソッドが見えてしまう問題がある。
 */
public class Proposal1 {
    
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

    interface Overlay extends RequestTransport {
        void setListener(ObjectId upper, OverlayListener listener);
        OverlayListener getListener(ObjectId upper);
    }

    interface OverlayListener extends RequestTransportListener {
        void onReceive(Overlay ov, OvReceivedMessage rmsg);
        FutureQueue<?> onReceiveRequest(Overlay ov, OvReceivedMessage rmsg);
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

    static class OvReceivedMessage extends ReceivedMessage {
        public Collection<? extends Key> getMatchedKeys() {
            return null;
        }
    }

    // 具体的実装
    
    static class FooOverlay implements Overlay {
        public void setListener(ObjectId upper, TransportListener listener) {
        } // 書きたくない
        public void setListener(ObjectId upper,
                RequestTransportListener listener) {
        } // 書きたくない

        public void setListener(ObjectId upper, OverlayListener listener) {
        }
        public OverlayListener getListener(ObjectId upper) {
            return null;
        }
    }
    
    static class FooOverlayListener implements OverlayListener {
        public void onReceive(Transport trans, ReceivedMessage rmsg) {
        } // 書かせたくない
        public void onReceive(RequestTransport trans, ReceivedMessage rmsg) {
        } // 書かせたくない
        public FutureQueue<?> onReceiveRequest(RequestTransport trans,
                ReceivedMessage rmsg) {
            return null;
        } // 書かせたくない

        public void onReceive(Overlay ov, OvReceivedMessage rmsg) {
        }
        public FutureQueue<?> onReceiveRequest(Overlay ov,
                OvReceivedMessage rmsg) {
            return null;
        }
    }

    public static void main(String[] args) {
    }
}
