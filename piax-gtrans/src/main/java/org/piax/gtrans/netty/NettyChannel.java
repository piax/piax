package org.piax.gtrans.netty;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.netty.nat.NettyNATLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyChannel<E extends NettyEndpoint> implements Channel<E> {

    final ObjectId localObjectId;
    final ObjectId remoteObjectId;
    final E channelInitiator;
    final E dst;
    NettyRawChannel<E> raw;
    final boolean isCreator;
    final NettyChannelTransport<E> trans;
    private final BlockingQueue<Object> rcvQueue;
    final int id;
    boolean isClosed;
    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class.getName());

    public NettyChannel(int channelNo, E channelInitiator,
            E destination,
            ObjectId localObjectId, ObjectId remoteObjectId, boolean isCreator, NettyRawChannel<E> raw, NettyChannelTransport<E> trans) {
        this.id = channelNo;
        this.channelInitiator = channelInitiator;
        this.dst = destination;
        this.localObjectId = localObjectId;
        this.remoteObjectId = remoteObjectId;
        this.isCreator = isCreator;
        this.raw = raw;
        this.trans = trans;
        this.isClosed = false;
        rcvQueue = new LinkedBlockingQueue<Object>();
    }

    @Override
    public void close() {
        if (raw.getStat() == NettyRawChannel.Stat.RUN) {
            try {
                send(null);
            } catch (IOException e) {
                logger.warn("Exception occured while closing channel to {}.", dst);
            }
            // XXX need to wait for other side to close?
        }
        trans.deleteChannel(this);
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        //return raw.isClosed();
        return isClosed;
    }

    @Override
    public TransportId getTransportId() {
        return raw.getTransportId();
    }

    @Override
    public int getChannelNo() {
        return id;
    }

    @Override
    public E getLocal() {
        return (E)trans.getEndpoint();
      //return raw.getLocal();
    }

    @Override
    public ObjectId getLocalObjectId() {
        return localObjectId;
    }

    @Override
    public E getRemote() {
        return dst;
        //return raw.getRemote();
    }

    @Override
    public ObjectId getRemoteObjectId() {
        return remoteObjectId;
    }

    @Override
    public boolean isDuplex() {
        return raw.isDuplex();
    }

    @Override
    public boolean isCreatorSide() {
        logger.debug("{}={}?",channelInitiator, trans.locator);
        return channelInitiator.equals(trans.locator);
    }

    public E getChannelInitiator() {
        return channelInitiator;
    }

    @Override
    public void send(Object msg) throws IOException {
        if (!raw.isClosed()) {
            logger.debug("ch {}{} send {} from {} to {}", getChannelNo(), getChannelInitiator(), msg, trans.locator, getRemote());
        }
        else { // re-create the raw channel.
            logger.debug("re-creating the raw channel for {}", getRemote());
            raw = trans.getRawCreateAsClient(getRemote());
        }
        E src = raw.getLocal();
        trans.channelSendHook(src, dst);
        NettyMessage<E> nmsg = new NettyMessage<E>(remoteObjectId, src,
                dst,
                getChannelInitiator(), raw.getPeerId(), msg, true,
                getChannelNo());
        // returns null if transport is finished.
        if (raw != null) {
            raw.send(nmsg);
        }

    }

    private static final Object EOF = new Object();
    protected void putReceiveQueue(Object msg) {
        try {
            if (msg == null) {
                rcvQueue.put(EOF);
            } else {
                rcvQueue.put(msg);
            }
        } catch (InterruptedException ignore) {
            ignore.printStackTrace();
        }
    }

    public Object receive() {
        Object msg = rcvQueue.poll();
        logger.debug("ch {} received {} on {} thread={}", getChannelNo(), msg, trans.locator, Thread.currentThread());
        if (msg == EOF) {
            logger.debug("ch {} received EOF on {}", getChannelNo(), msg, trans.locator);
            isClosed = true;
            trans.deleteChannel(this);
            return null;
        }
        return msg;
    }

    public Object receive(int timeout) throws NetworkTimeoutException {
        try {
            Object msg = rcvQueue.poll(timeout, TimeUnit.MILLISECONDS);
            logger.debug("ch received(with timeout) {} on {} thread={}", msg, this, Thread.currentThread());
            if (msg == EOF) {
                logger.debug("ch {} received EOF on {}", getChannelNo(), msg, trans.locator);
                isClosed = true;
                trans.deleteChannel(this);
                return null;
            }
            if (msg == null) {
                throw new NetworkTimeoutException("ch.receive timed out");
            }
            return msg;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public String toString() {
        return getRemote().toString() + ":" + getLocalObjectId()+":" + getChannelNo();
    }

}
