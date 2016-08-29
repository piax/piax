package test.misc.generics;

/**
 * あべ先生によるgenericsの活用パターン。
 * Proposal4へ反映
 */
public class GTest2 implements RequestTransportListener<Destination> {
    
    static class Hoge implements RequestTransport<Destination> {
        public RequestTransportListener<Destination> getListener(ObjectId upper) {
            return null;
        }
    }

    @Override
    public FutureQueue<?> onReceiveRequest(RequestTransport<Destination> trans,
            ReceivedMessage<Destination> rmsg) {
        return null;
    }

    @Override
    public void onReceive(RequestTransport<Destination> trans,
            ReceivedMessage<Destination> rmsg) {
    }
}

interface Destination {}

class ObjectId {}

class ReceivedMessage<T> {}

class FutureQueue<T> {}

// 継承用
interface _Transport<D extends Destination, L> {
    L getListener(ObjectId upper);
}

// 公開用
interface Transport<D extends Destination> extends
        _Transport<D, TransportListener<D>> {
}

// 継承用
interface _RequestTransport<D extends Destination, L> extends _Transport<D, L> {
}

// 公開用
interface RequestTransport<D extends Destination> extends
        _RequestTransport<D, RequestTransportListener<D>> {
}

//xxListenerは次のようにgenericsを使ってます。

// 継承用
//interface _TransportListener<D extends Destination, L, T extends _Transport<D, L>> {
//    void onReceive(T trans, ReceivedMessage<D> rmsg);
//}
interface _TransportListener<D extends Destination, T> {
    void onReceive(T trans, ReceivedMessage<D> rmsg);
}

// 公開用
//interface TransportListener<D extends Destination> extends
//        _TransportListener<D, TransportListener, _Transport<D, TransportListener>> {
//}
interface TransportListener<D extends Destination> extends
        _TransportListener<D, Transport<D>> {
}

// 継承用
//interface _RequestTransportListener<D extends Destination, L, T extends _RequestTransport<D, L>>
//        extends _TransportListener<D, L, _RequestTransport<D, L>> {
//    FutureQueue<?> onReceiveRequest(T trans, ReceivedMessage<D> rmsg);
//}
interface _RequestTransportListener<D extends Destination, T> extends
        _TransportListener<D, RequestTransport<D>> {
    FutureQueue<?> onReceiveRequest(T trans, ReceivedMessage<D> rmsg);
}

// 公開用
//interface RequestTransportListener<D extends Destination> extends
//        _RequestTransportListener<D, RequestTransportListener, RequestTransport<D>> {
//}
interface RequestTransportListener<D extends Destination> extends
        _RequestTransportListener<D, RequestTransport<D>> {
}
