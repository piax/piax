package org.piax.gtrans.netty;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.NetworkTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyRawChannel implements Channel<NettyLocator> {

    private ChannelHandlerContext ctx;
    private final NettyLocator remote;
    private final NettyChannelTransport mother;
    private static final Logger logger = LoggerFactory.getLogger(NettyRawChannel.class.getName());
    boolean isActive = false;
    Integer attempt = null;
    enum Stat {
        INIT,
        WAIT,
        DENIED,
        RUN,
        DEFUNCT
    }
    Stat stat;
    
    public NettyRawChannel(NettyLocator remote, NettyChannelTransport mother) {
        this.remote = remote;
        this.mother = mother;
        this.attempt = null;
        this.stat = Stat.INIT;
        this.ctx = null;
    }

    public PeerId getPeerId() {
        return mother.getPeerId();
    }

    @Override
    public void close() {
        ctx.close();
    }

    @Override
    public boolean isClosed() {
        return !ctx.channel().isOpen();
    }

    @Override
    public TransportId getTransportId() {
        return mother.getTransportId();
    }

    @Override
    public int getChannelNo() {
        return remote.getPort();
    }

    @Override
    public NettyLocator getLocal() {
        return mother.getEndpoint();
    }

    @Override
    public ObjectId getLocalObjectId() {
        return null;//localObjectId;
    }

    @Override
    public NettyLocator getRemote() {
        return remote;
    }

    @Override
    public ObjectId getRemoteObjectId() {
        return null; //remoteObjectId;
    }

    @Override
    public boolean isDuplex() {
        return true;
    }

    @Override
    public boolean isCreatorSide() {
        return false; // always false
    }

    public ChannelHandlerContext getContext() {
        return ctx;
    }

    /*
     * Set attempt value of the raw channel.
     * This value is used to decide which channel is used in the concurrent connection attempts.
     * If attempt is null, the connection is 
     * if null, 
     */
    public synchronized void setAttempt(int r) {
        attempt = r;
    }

    public synchronized Integer getAttempt() {
        return attempt;
    }

    public void unsetAttempt() {
        synchronized (attempt) {
            attempt = null;
        }
    }

    synchronized public void setStat(Stat stat) {
        this.stat = stat;
    }
    
    synchronized public Stat getStat() {
        return stat;
    }
    
    synchronized public void setContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void send(Object msg) throws IOException {
        // object is supposed to be a NettyMessage
        logger.debug("sending {} from {} to {}", ((NettyMessage)msg).getMsg(), getLocal(), getRemote());
        if (stat == Stat.RUN && ctx.channel().isOpen()) {
            ctx.writeAndFlush(msg);//.syncUninterruptibly();
            logger.debug("sent {} from {} to {}", ((NettyMessage)msg).getMsg(), getLocal(), getRemote());
        }
        else {
            new IOException("the sending channel is already closed.");
        }
    }

    @Override
    public Object receive() {
        // never reached.
        return null;
    }

    @Override
    public Object receive(int timeout) throws NetworkTimeoutException {
        // never reached.
        return null;
    }
    
    @Override
    public String toString() {
        return "(RAW local=" + getLocal() + "remote=" + getRemote() + ")";
    }
}
