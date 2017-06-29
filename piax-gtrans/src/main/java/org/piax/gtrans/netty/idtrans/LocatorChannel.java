package org.piax.gtrans.netty.idtrans;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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
    PrimaryKey primaryKey;
    AtomicInteger useCount;

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
        this.useCount = new AtomicInteger(0); 
        lastUse = System.currentTimeMillis();
    }

    public LocatorChannel(NettyLocator remote, IdChannelTransport trans) {
        this.remote = remote;
        this.trans = trans;
        this.attempt = null;
        this.stat = Stat.INIT;
        this.channel = null;
        this.useCount = new AtomicInteger(0); 
        lastUse = System.currentTimeMillis();
    }

    public synchronized void setPrimaryKey(PrimaryKey key) {
        logger.debug("set primary key for {} on {} as {}", remote, trans.getEndpoint(), key);
        this.primaryKey = key; 
    }
    
    public synchronized PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void use() {
        useCount.incrementAndGet();
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

    public CompletableFuture<Boolean> closeAsync(boolean force) {
        CompletableFuture<Boolean> ret = new CompletableFuture<>();
        if (force || useCount.getAndDecrement() == 0) {
            channel.close().addListener((future) -> {
                ret.complete(true);
            });
        }
        else {
            // not fource & useCount > 0
            ret.complete(true);
        }
        return ret;
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

    // synchronous send without confirmation
    public void send(Object msg) throws IOException {
        sendAsync(msg).syncUninterruptibly();
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
