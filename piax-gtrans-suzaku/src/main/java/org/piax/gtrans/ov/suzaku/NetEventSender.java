package org.piax.gtrans.ov.suzaku;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.ayame.Event;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.EventSender;
import org.piax.ayame.Node;
import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;

public class NetEventSender<E extends Endpoint> implements EventSender, TransportListener<E> {
    TransportId transId;
    ChannelTransport<E> trans;
    public static AtomicInteger count = new AtomicInteger(0);

    public NetEventSender(TransportId transId, ChannelTransport<E> trans) {
        this.transId = transId;
        this.trans = trans;
        trans.setListener(transId, this);
        if (count.incrementAndGet() == 1) {
            EventExecutor.reset();
            EventExecutor.startExecutorThread();
        }
    }

    public Endpoint getEndpoint() {
        return trans.getEndpoint();
    }

    public void fin() {
        if (count.decrementAndGet() == 0) {
            EventExecutor.terminate();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<Void> send(Event ev) {
        assert ev.delay == Node.NETWORK_LATENCY;
        //ev.vtime = EventExecutor.getVTime() + ev.delay;
        ev.vtime = 0;
        logger.trace("*** {}|send/forward event {}", ev.sender, ev);

        if (ev.receiver.addr.equals(getEndpoint())) {
            recv(ev);
            return CompletableFuture.completedFuture(null);
        }
        
        return trans.sendAsync(transId, (E) ev.receiver.addr, ev);
    }

    @Override
    public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
        logger.trace("*** recv (on: {} from: {}) {}", trans.getEndpoint(), rmsg.getSource(), rmsg.getMessage());
        recv((Event)rmsg.getMessage());
    }

    public void recv(Event ev) {
        EventExecutor.enqueue(ev);
    }
    
    public boolean isRunning() {
        return trans.isUp();
    }
}
