package org.piax.gtrans.netty;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.NetworkTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyChannel implements Channel<NettyLocator> {

    final ObjectId localObjectId;
    final ObjectId remoteObjectId;
    final NettyLocator channelInitiator;
    final NettyRawChannel raw;
    final boolean isCreator;
    final NettyChannelTransport trans;
    private final BlockingQueue<Object> rcvQueue;
    final int id;
    //final boolean isSenderChannel;
    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class.getName());

    public NettyChannel(int channelNo, NettyLocator channelInitiator, ObjectId localObjectId, ObjectId remoteObjectId, boolean isCreator, NettyRawChannel raw, NettyChannelTransport trans) {
        this.id = channelNo;
        
        this.channelInitiator = channelInitiator;
        this.localObjectId = localObjectId;
        this.remoteObjectId = remoteObjectId;
        this.isCreator = isCreator;
        this.raw = raw;
        this.trans = trans;
        rcvQueue = new LinkedBlockingQueue<Object>();
    }
   
    @Override
    public void close() {
        trans.deleteChannel(this);
    }

    @Override
    public boolean isClosed() {
        return raw.isClosed();
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
        return raw.getLocal();
    }

    @Override
    public ObjectId getLocalObjectId() {
        return localObjectId;
    }

    @Override
    public NettyLocator getRemote() {
        return raw.getRemote();
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
        //return isSenderChannel;
    }
/*    
    public boolean isSenderChannel() {
        return isSenderChannel;
    }
  */  
    public NettyLocator getChannelInitiator() {
        return channelInitiator;
    }

    @Override
    public void send(Object msg) throws IOException {
        NettyMessage nmsg = new NettyMessage(remoteObjectId, raw.getLocal(), getChannelInitiator(), raw.getPeerId(), msg, true,
                getChannelNo());
        logger.debug("ch {}{} send {} from {} to {}", getChannelNo(), getChannelInitiator(), msg, trans.locator, getRemote());
        raw.send(nmsg);
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
        return msg;
    }

    public Object receive(int timeout) throws NetworkTimeoutException {
        try {
            Object msg = rcvQueue.poll(timeout, TimeUnit.MILLISECONDS);
            logger.debug("ch received(with timeout) {} on {} thread={}", msg, this, Thread.currentThread());
            if (msg == EOF) {
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
