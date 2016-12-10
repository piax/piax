package org.piax.gtrans.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.ChannelTransportImpl;
import org.piax.gtrans.netty.NettyRawChannel.Stat;
import org.piax.gtrans.netty.bootstrap.SslBootstrap;
import org.piax.gtrans.netty.bootstrap.TcpBootstrap;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap;
import org.piax.gtrans.netty.bootstrap.UdtBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyChannelTransport extends ChannelTransportImpl<NettyLocator> implements ChannelTransport<NettyLocator> {

    EventLoopGroup bossGroup;
    EventLoopGroup serverGroup;
    EventLoopGroup clientGroup;
    boolean supportsDuplex = true;
    NettyLocator locator = null;
    final PeerId peerId;
    // a map to hold active raw channels;
    final ConcurrentHashMap<String,NettyRawChannel> raws =
            new ConcurrentHashMap<String,NettyRawChannel>();
    final ConcurrentHashMap<String,NettyChannel> channels =
            new ConcurrentHashMap<String,NettyChannel>();
    final Random rand = new Random(System.currentTimeMillis());
    private static final Logger logger = LoggerFactory.getLogger(NettyChannelTransport.class.getName());
    boolean isRunning = false;
    final ChannelGroup schannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    final ChannelGroup cchannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    NettyBootstrap bs;
    
    AtomicInteger seq;

    enum AttemptType {
        ATTEMPT, ACK, NACK 
    }

    public NettyChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, null, true);
        this.locator = peerLocator;
        this.peerId = peerId;
        seq = new AtomicInteger(0);// sequence number (ID of the channel)
//        bossGroup = new NioEventLoopGroup(1);
//        serverGroup = new NioEventLoopGroup(10);
//        clientGroup = new NioEventLoopGroup(10);

        switch(peerLocator.getType()){
        case TCP:
            bs = new TcpBootstrap();
            break;
        case SSL:
            bs = new SslBootstrap(locator.getHost(), locator.getPort());
            break;
        case UDT:
            bs = new UdtBootstrap();
            break;
        default:
            throw new ProtocolUnsupportedException("not implemented yet.");
        }

        bossGroup = bs.getParentEventLoopGroup();
        serverGroup = bs.getChildEventLoopGroup();
        clientGroup = bs.getClientEventLoopGroup();
        
        ServerBootstrap b = bs.getServerBootstrap(this);
        b.bind(new InetSocketAddress(peerLocator.getHost(), peerLocator.getPort()));//.syncUninterruptibly();
        logger.debug("bound " + peerLocator);
        isRunning = true;
    }

    @Override
    public void fin() {
        logger.debug("running fin.");
        isRunning = false;
/*        for (NettyRawChannel raw : raws.values()) {
            raw.close();
        }*/
        cchannels.close().awaitUninterruptibly();
        schannels.close().awaitUninterruptibly();
        bossGroup.shutdownGracefully();
        serverGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver, NettyLocator dst,
            Object msg) throws ProtocolUnsupportedException, IOException {
        NettyRawChannel raw = getRawCreateAsClient(dst);
        if (raw == null) {
            throw new IOException("Getting new raw channel failed (maybe peer down).");
        }
        // generate a new channel
        logger.debug("oneway send to {} from {} msg={}", dst, locator, msg);
        NettyMessage nmsg = new NettyMessage(receiver, raw.getLocal(), null, raw.getPeerId(), msg, false, 0);
        raw.send(nmsg);
    }
    
    void putChannel(NettyLocator channelInitiator, NettyChannel ch) {
        logger.debug("" + ch.getChannelNo() + channelInitiator + "->" + ch + " on " + locator);
        channels.put("" + ch.getChannelNo() + channelInitiator, ch);
    }
    
    NettyChannel getChannel(int channelNo, NettyLocator channelInitiator) {
        logger.debug("" + channelNo + channelInitiator + " on " + locator);
        return channels.get("" + channelNo + channelInitiator);
    }
    
    void deleteChannel(NettyChannel ch) {
        channels.remove("" + ch.getChannelNo() + ch.getRemote(), ch);
    }
    
    // package local
    void putRaw(NettyLocator locator, NettyRawChannel ch) {
        raws.put(locator.toString(), ch);
    }
    
    NettyRawChannel getRaw(NettyLocator locator) {
        return raws.get(locator.toString());
    }
    
    void deleteRaw(NettyRawChannel raw) {
        raws.remove(raw.getRemote().toString(), raw);
    }
    
    NettyRawChannel getRawByContext(ChannelHandlerContext ctx) {
        NettyRawChannel ret = null;
        for (NettyRawChannel raw : raws.values()) {
            ChannelHandlerContext rc = raw.getContext();
            if (rc != null) {
                if (rc.channel().remoteAddress().equals(ctx.channel().remoteAddress())) {
                    ret = raw;
                    break;
                }
            }
        }
        return ret;
    }

    NettyRawChannel getRawCreateAsClient(NettyLocator dst) throws IOException {
        if (!isRunning) return null;
        final NettyRawChannel raw;
        synchronized (raws) {
            NettyRawChannel cached = getRaw(dst);
            if (cached != null) {
                while (cached.getStat() == Stat.INIT || cached.getStat() == Stat.WAIT || cached.getStat() == Stat.DENIED) {
                    try {
                        synchronized(cached) {
                            cached.wait(CHANNEL_ESTABLISH_TIMEOUT);
                            logger.info("waiting for RUN state. current:" + cached.getStat());
                        }
                    } catch (InterruptedException e) {
                    }
                }
                // next state should be RUN
                if (cached.getStat() == Stat.RUN) {
                    return cached;
                }
                else {
                    logger.info("getRawChannelAsClient: illegal state: " + cached.getStat());
                    cached.close();
                }
            }
            raw = new NettyRawChannel(dst, this);
            Bootstrap b = bs.getBootstrap(raw, this); 
            b.connect(dst.getHost(), dst.getPort());
        }
        while (raw.getStat() == Stat.INIT || raw.getStat() == Stat.WAIT || raw.getStat() == Stat.DENIED) {
            logger.debug("Stat={}", raw.getStat());
            try {
                synchronized(raw) {
                    raw.wait(CHANNEL_ESTABLISH_TIMEOUT);
                }
            } catch (InterruptedException e) {
            }
        }
        if (raw.getStat() == Stat.RUN) {
            logger.debug("created raw channel become available=" + raw);
            return raw;
        }
        logger.debug("getRawChannelAsClient: illegal state: " + raw.getStat());
        raw.close();
        throw new IOException("Channel establish failed.");
    }

    void outboundActive(NettyRawChannel raw, ChannelHandlerContext ctx) {
        logger.debug("outbound active: " + ctx.channel().remoteAddress());
        cchannels.add(ctx.channel());
        int attemptRand = rand.nextInt();
        // is this valid only for tcp channel?
        InetSocketAddress sa = (InetSocketAddress)ctx.channel().remoteAddress();
        NettyLocator dst = new NettyLocator(sa.getHostName(), sa.getPort());
        AttemptMessage attempt = new AttemptMessage(AttemptType.ATTEMPT, locator, attemptRand);
        synchronized(raws) {
            // NettyRawChannel raw = raws.get(locator);
            synchronized (raw) {
                raw.setAttempt(attemptRand);
                raw.setContext(ctx);
                raw.setStat(Stat.WAIT);
            }
            putRaw(dst, raw);
        }
        ctx.writeAndFlush(attempt);
        logger.debug("sent attempt to " + dst + " : " + ctx);
    }

    void outboundInactive(ChannelHandlerContext ctx) {
        logger.debug("outbound inactive: " + ctx.channel().remoteAddress());
        synchronized(raws) {
            NettyRawChannel raw = getRawByContext(ctx);
            if (raw != null) {
                this.deleteRaw(raw);
            }
            ctx.close();
        }
    }

    void inboundActive(ChannelHandlerContext ctx) {
        logger.debug("inbound active: " + ctx.channel().remoteAddress());
        schannels.add(ctx.channel());
    }

    void inboundInactive(ChannelHandlerContext ctx) {
        logger.debug("inbound inactive: " + ctx.channel().remoteAddress());
        synchronized(raws) {
            NettyRawChannel raw = getRawByContext(ctx);
            this.deleteRaw(raw);
        }
    }

    static final int CHANNEL_ESTABLISH_TIMEOUT = 10000; 

    void inboundReceive(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof AttemptMessage) {
            AttemptMessage attempt = (AttemptMessage) msg;
            logger.debug("received attempt: " + attempt.getArg() + " from "
                    + ctx);
            switch (attempt.type) {
            case ATTEMPT:
                synchronized (raws) {
                    NettyRawChannel raw = getRaw(attempt.getSource());
                    if (raw != null && locator.equals(attempt.getSource())) {
                        // loop back.
                        synchronized (raw) {
                            raw.setStat(Stat.RUN);
                            raw.setContext(ctx);
                            ctx.writeAndFlush(new AttemptMessage(
                                    AttemptType.ACK, locator, null));
                        }
                    } else if (raw != null && raw.attempt != null) {
                        synchronized (raw) {
                            // this side wins
                            if (raw.attempt > (int) attempt.getArg()) {
                                try {// wait for ACK for outbound attempt.
                                    while (raw.getStat() == Stat.WAIT) {
                                        raw.wait(CHANNEL_ESTABLISH_TIMEOUT);
                                    }
                                } catch (InterruptedException e) {
                                }
                                ctx.writeAndFlush(new AttemptMessage(
                                        AttemptType.NACK, locator, null));
                            } else { // opposite side wins.
                                ctx.writeAndFlush(new AttemptMessage(
                                        AttemptType.ACK, locator, null));
                                // not received NACK yet.
                                try {
                                    while (raw.getStat() == Stat.WAIT) {
                                        // wait for NACK for outbound
                                        // attempt...no need?
                                        raw.wait(CHANNEL_ESTABLISH_TIMEOUT);
                                    }
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        } // synchronized raw
                    }
                    // raw.setStat(Stat.RUN);
                    else {
                        // cache not found. just accept it.
                        raw = new NettyRawChannel(attempt.getSource(), this);
                        // accept attempt.
                        raw.setStat(Stat.RUN);
                        raw.setContext(ctx);
                        putRaw(attempt.getSource(), raw);
                        logger.debug("set run stat for raw from source="
                                + attempt.getSource());
                        ctx.writeAndFlush(new AttemptMessage(AttemptType.ACK,
                                locator, null));
                    }
                } // synchronized raws
                break;
            case ACK:
                logger.debug("illegal attempt ACK received from client");
                break;
            case NACK:
                logger.debug("illegal attempt NACK received from client");
                break;
            }
        } else if (msg instanceof NettyMessage) {
            NettyMessage nmsg = (NettyMessage) msg;
            logger.debug("inbound received msg: " + nmsg.getMsg() + " on " + locator
                    + " from " + nmsg.getSourceLocator());
            if (nmsg.isChannelSend()) {
                NettyChannel ch = null;
                synchronized (channels) {
                    ch = getChannel(nmsg.channelNo(), nmsg.getChannelInitiator());
                    if (ch == null) {
                        synchronized (raws) {
                            NettyRawChannel raw = getRaw(nmsg.getSourceLocator());
                            if (raw == null || raw.getStat() != Stat.RUN) {
                                logger.info(
                                        "receive in illegal state from {} (channel not running): throwing it away.",
                                        nmsg.getSourceLocator());
                            } else {
                                // channel is created on the first message
                                // arrival.
                                ch = new NettyChannel(nmsg.channelNo(),
                                        nmsg.getChannelInitiator(),
                                        nmsg.getObjectId(), nmsg.getObjectId(),
                                        false, raw, this);
                                putChannel(nmsg.getChannelInitiator(), ch);
                            }
                        }
                    }
                    else {
                        logger.debug("response for call from inbound on {} received.", ch);
                    }
                }
                if (ch != null) {
                    messageReceived(ch, nmsg);
                }
            } else {
                logger.debug("received oneway msg={}", msg);
                messageReceived(null, nmsg);
            }
        }
    }

    void outboundReceive(NettyRawChannel raw, ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof AttemptMessage) {
            AttemptMessage resp = (AttemptMessage) msg;
            logger.debug("outbound attempt response=" + resp.type);
            switch(resp.type) {
            case ATTEMPT:
                logger.debug("illegal attempt received from server");
                break;
            case ACK:
                synchronized(raws) {
//                    NettyRawChannel raw = getRaw(resp.getSource());
                    synchronized(raw) {
                        raw.setStat(Stat.RUN);
                        raw.notifyAll();
                    }
                }
                break;
            case NACK:
                // there should be a attempt thread on this peer.
                synchronized(raws) {
//                    NettyRawChannel raw = getRaw(resp.getSource());
                    synchronized(raw) {
                        switch(raw.getStat()) {
                        case RUN:
                            // nothing to do.
                            break;
                        case WAIT:
                            raw.setStat(Stat.DENIED);
                            raw.notifyAll();
                            break;
                        default:
                            logger.debug("illegal state: " + raw.getStat());
                            // retry?
                            break;
                        }
                    }
                }
                break;
            }
        }
        else if (msg instanceof NettyMessage) {
            NettyMessage nmsg = (NettyMessage) msg;
            logger.debug("outbound received msg: " + nmsg.getMsg() + " on " + locator + " from " + nmsg.getSourceLocator());
            if (nmsg.isChannelSend()) {
            NettyChannel ch;
            synchronized (channels) {
                // get a channel that has remote as the nmsg.source. 
                ch = getChannel(nmsg.channelNo(), nmsg.getChannelInitiator());
                logger.debug("got stored ch=" + ch + " for msg: " + nmsg.getMsg());
                if (ch == null) {
                    // not assumed status.
                    ch = new NettyChannel(nmsg.channelNo(),
                            nmsg.getChannelInitiator(),
                            nmsg.getObjectId(),
                            nmsg.getObjectId(), false, raw, this);
                    putChannel(nmsg.getChannelInitiator(), ch);
                }
            }
            if (ch != null) {
                messageReceived(ch, nmsg);
            }
            }
            else {
                logger.debug("received oneway msg={}", msg);
                messageReceived(null, nmsg);
            }
        }
    }

    // call necessary listeners.
    void messageReceived(NettyChannel c, NettyMessage nmsg) {
        if (!nmsg.isChannelSend()) {
            TransportListener<NettyLocator> listener = getListener(nmsg.getObjectId());
            if (listener != null) {
                ReceivedMessage rmsg = new ReceivedMessage(nmsg.getObjectId(), nmsg.getSourceLocator(), nmsg.getMsg());
                logger.debug("trans received {} on {}", rmsg.getMessage(), nmsg.getSourceLocator());
                listener.onReceive(this, rmsg);
            }
        }
        else {
            ChannelListener<NettyLocator> clistener = this.getChannelListener(nmsg.getObjectId());
            c.putReceiveQueue(nmsg.getMsg());
            if (clistener != null) {
                clistener.onReceive(c);
            }
        }
    }
    
    @Override
    public NettyLocator getEndpoint() {
        return locator;
    }
    
    @Override
    public Channel<NettyLocator> newChannel(ObjectId sender, ObjectId receiver,
            NettyLocator dst, boolean isDuplex, int timeout)
            throws ProtocolUnsupportedException, IOException {
        logger.debug("new channel for: " + dst + " on " + locator);
        NettyRawChannel raw = getRawCreateAsClient(dst);
        if (raw == null) {
            throw new IOException("Getting new raw channel failed (maybe peer down).");
        }
        NettyChannel ch;
        synchronized(channels) {
            ch = new NettyChannel(seq.incrementAndGet(), locator, sender, receiver, true, raw, this);
            putChannel(locator, ch);
        }
        return ch;
    }
}
