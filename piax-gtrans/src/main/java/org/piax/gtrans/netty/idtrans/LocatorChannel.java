package org.piax.gtrans.netty.idtrans;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.IOException;

import org.piax.common.PeerId;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocatorChannel {

    private Channel channel;
    private final NettyLocator remote;
    private final IdChannelTransport trans;
    private static final Logger logger = LoggerFactory.getLogger(LocatorChannel.class.getName());
    Integer attempt = null;
    long lastUse;

    public enum Stat {
        INIT,
        WAIT,
        DENIED,
        RUN,
        DEFUNCT
    }
    Stat stat;

    public LocatorChannel(IdChannelTransport trans) {
        this.remote = null; // loopback
        this.trans = trans;
        this.attempt = null;
        this.stat = Stat.RUN;
        this.channel = null;
        lastUse = System.currentTimeMillis();
    }
    
    public LocatorChannel(NettyLocator remote, IdChannelTransport trans) {
        this.remote = remote;
        this.trans = trans;
        this.attempt = null;
        this.stat = Stat.INIT;
        this.channel = null;
        lastUse = System.currentTimeMillis();
    }

    synchronized public void setNettyChannel(Channel channel) {
        this.channel = channel;
        lastUse = System.currentTimeMillis();
    }

    synchronized public Channel getNettyChannel() {
        return channel;
    }

    public PeerId getPeerId() {
        return trans.getPeerId();
    }

    public void touch() {
        lastUse = System.currentTimeMillis();
    }

    public ChannelFuture close() {
        setStat(Stat.DEFUNCT);
        return channel.close();
    }

    public boolean isClosed() {
        //return !ctx.channel().isOpen();
        return getStat() == Stat.DEFUNCT;
    }

    public int getChannelNo() {
        return remote.getPort();
    }
    
    public NettyLocator getRemote() {
        return remote;
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
        attempt = null;
    }

    synchronized public void setStat(Stat stat) {
        this.stat = stat;
    }
    
    synchronized public Stat getStat() {
        return stat;
    }
    
    synchronized public void setChannel(Channel channel) {
        this.channel = channel;
    }
    
    synchronized public Channel getChannel() {
        return this.channel;
    }

 // asynchronous send method.
    public void send(Object msg) throws IOException {
        touch();
        logger.debug("sending {} from {} to {}", ((NettyMessage)msg).getMsg(), trans.getEndpoint(), remote);
        if (stat == Stat.RUN && channel.isOpen()) {
            channel.writeAndFlush(msg);
        }
        else {
            new IOException("the sending channel is closed.");
        }
    }
    
 // asynchronous send method.
    public ChannelFuture sendAsync(Object msg) throws IOException {
        touch();
        logger.debug("sending {} from {} to {}", ((NettyMessage)msg).getMsg(), trans.getEndpoint(), remote);
        if (!(stat == Stat.RUN && channel.isOpen())) {
            new IOException("the sending channel is closed.");
        }
        return channel.writeAndFlush(msg);
    }

}
