package org.piax.gtrans.netty;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.netty.nat.NettyNATLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyRawChannel implements Channel<NettyLocator> {

    private ChannelHandlerContext ctx;
    private final NettyLocator remote;
    private final NettyChannelTransport mother;
    private static final Logger logger = LoggerFactory.getLogger(NettyRawChannel.class.getName());
    Integer attempt = null;
    long lastUse;
    boolean isCreatorSide; // true if the raw channel is generated as a client.
    int priority; // 1 or 0; 
    
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
        isCreatorSide = false;
        lastUse = System.currentTimeMillis();
        priority = 0;
    }
    
    public NettyRawChannel(NettyLocator remote, NettyChannelTransport mother, boolean isCreatorSide) {
        this.remote = remote;
        this.mother = mother;
        this.attempt = null;
        this.stat = Stat.INIT;
        this.ctx = null;
        this.isCreatorSide = isCreatorSide;
        lastUse = System.currentTimeMillis();
        priority = 0;
    }
    
    public void setPriority(int p) {
        priority = p;
    }

    public PeerId getPeerId() {
        return mother.getPeerId();
    }
    
    public void touch() {
        lastUse = System.currentTimeMillis();
    }

    @Override
    public void close() {
        synchronized(mother.raws) {
            //mother.raws.remove(getRemote());
            mother.deleteRaw(this);
            setStat(Stat.DEFUNCT);
            ctx.close();//.syncUninterruptibly();
        }
    }

    @Override
    public boolean isClosed() {
        //return !ctx.channel().isOpen();
        return getStat() == Stat.DEFUNCT;
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
        return isCreatorSide;
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
        touch();
        if (NettyChannelTransport.NAT_SUPPORT) {
            // just for count
            if (getRemote() instanceof NettyNATLocator) {
                logger.debug("forwarding {} on {} to {}", ((NettyMessage)msg).getMsg(), getLocal(), ((NettyMessage)msg).getDestinationLocator());
                mother.forwardCount++;
            }
        }
        // object is supposed to be a NettyMessage
        logger.debug("sending {} from {} to {}", ((NettyMessage)msg).getMsg(), getLocal(), getRemote());
        if (stat == Stat.RUN && ctx.channel().isOpen()) {
            ctx.writeAndFlush(msg);//.syncUninterruptibly();
            logger.debug("sent {} from {} to {}", ((NettyMessage)msg).getMsg(), getLocal(), getRemote());
        }
        else {
            new IOException("the sending channel is closed.");
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
        return "(RAW: stat=" + getStat() + " local=" + getLocal() + ",remote=" + getRemote() + ",lastUse=" + lastUse + ")";
    }
}
