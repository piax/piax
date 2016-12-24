package org.piax.gtrans.netty;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.netty.nat.NettyNATLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyChannel implements Channel<NettyLocator> {

    final ObjectId localObjectId;
    final ObjectId remoteObjectId;
    final NettyLocator channelInitiator;
    final NettyLocator dst;
    NettyRawChannel raw;
    final boolean isCreator;
    final NettyChannelTransport trans;
    private final BlockingQueue<Object> rcvQueue;
    final int id;
    boolean isClosed;
    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class.getName());

    public NettyChannel(int channelNo, NettyLocator channelInitiator,
            NettyLocator destination,
            ObjectId localObjectId, ObjectId remoteObjectId, boolean isCreator, NettyRawChannel raw, NettyChannelTransport trans) {
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
        try {
            send(null);
        } catch (IOException e) {
            logger.warn("Exception occured while closing channel to {}.", dst);
        }
        // XXX need to wait for other side to close?
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
    public NettyLocator getLocal() {
        return trans.locator;
      //return raw.getLocal();
    }

    @Override
    public ObjectId getLocalObjectId() {
        return localObjectId;
    }

    @Override
    public NettyLocator getRemote() {
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

    public NettyLocator getChannelInitiator() {
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
        NettyLocator src = raw.getLocal();
        if (NettyChannelTransport.NAT_SUPPORT) {
            if (src instanceof NettyNATLocator) {
                logger.debug("on {}, update from {}", raw, src);
                ((NettyNATLocator)src).updateRawChannelLocators(trans);
                logger.debug("updated to {}", src);
            }
            //if (raw.getRemote() instanceof NettyNATLocator) {
                logger.debug("on {}, send to {}, channel.remote={}", raw, raw.getRemote(), getRemote());
            //}
        }

        NettyMessage nmsg = new NettyMessage(remoteObjectId, src,
                // raw.getRemote() does not work on NAT.
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
