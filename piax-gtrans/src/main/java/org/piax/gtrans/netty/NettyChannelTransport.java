package org.piax.gtrans.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.ChannelTransportImpl;
import org.piax.gtrans.netty.NettyRawChannel.Stat;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap;
import org.piax.gtrans.netty.nat.NATLocatorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NettyChannelTransport<E extends NettyEndpoint> extends ChannelTransportImpl<E> implements ChannelTransport<E> {

    protected static final Logger logger = LoggerFactory.getLogger(NettyChannelTransport.class.getName());
    protected EventLoopGroup bossGroup;
    protected EventLoopGroup serverGroup;
    protected EventLoopGroup clientGroup;
    boolean supportsDuplex = true;
    protected E locator = null;
    final PeerId peerId;
    // a map to hold active raw channels;
    protected final ConcurrentHashMap<E,NettyRawChannel<E>> raws =
            new ConcurrentHashMap<E,NettyRawChannel<E>>();
    protected final ConcurrentHashMap<String,NettyChannel<E>> channels =
            new ConcurrentHashMap<String,NettyChannel<E>>();
    final Random rand = new Random(System.currentTimeMillis());
    protected boolean isRunning = false;
    
    final ChannelGroup schannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    final ChannelGroup cchannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    // XXX should be private
    public NATLocatorManager nMgr; 
    protected NettyBootstrap bs;
    protected AtomicInteger seq;
    final public int RAW_POOL_SIZE = 10;
    
    public AttributeKey<String> rawKey = AttributeKey.valueOf("rawKey");

    enum AttemptType {
        ATTEMPT, ACK, NACK 
    }
    
    //static boolean NAT_SUPPORT = true;

    public NettyChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, null, true);
        //this.locator = peerLocator;
        this.peerId = peerId;
        seq = new AtomicInteger(0);// sequence number (ID of the channel)
        
        bootstrap(peerLocator);
        isRunning = true;
    }

    @Override
    public void fin() {
        logger.debug("running fin.");
        isRunning = false;
        cchannels.close().awaitUninterruptibly();
        schannels.close().awaitUninterruptibly();
        bossGroup.shutdownGracefully();
        serverGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver, E dst,
            Object msg) throws ProtocolUnsupportedException, IOException {
        NettyRawChannel<E> raw = getRawCreateAsClient(dst);
        if (raw == null) {
            throw new IOException("Getting new raw channel failed (maybe peer down).");
        }
        // generate a new channel
        logger.debug("oneway send to {} from {} msg={}", dst, locator, msg);
        E src = (E)raw.getLocal();
        NettyMessage<E> nmsg = new NettyMessage<E>(receiver, src, dst, null, raw.getPeerId(), msg, false, 0);
        raw.touch();
        raw.send(nmsg);
    }

    void putChannel(NettyChannel<E> ch) {
        logger.debug("" + ch.getChannelNo() + ch.channelInitiator.hashCode() + "->" + ch + " on " + locator);
        channels.put("" + ch.getChannelNo() + ch.channelInitiator.hashCode(), ch);
    }

    NettyChannel<E> getChannel(int channelNo, E channelInitiator) {
        logger.debug("" + channelNo + channelInitiator.hashCode() + " on " + locator);
        return channels.get("" + channelNo + channelInitiator.hashCode());
    }

    void deleteChannel(NettyChannel<?> ch) {
        channels.remove("" + ch.getChannelNo() + ch.channelInitiator.hashCode(), ch);
    }

    // package local
    void putRaw(E locator, NettyRawChannel<E> ch) {
        if (raws.size() == 0) { // the first channel. its premier.
            ch.setPriority(1);
        }
        raws.put(locator, ch);
    }

    NettyRawChannel<E> getRaw(E locator) {
        return raws.get(locator);
    }

    void deleteRaw(NettyRawChannel<E> raw) {
        raws.remove(raw.getRemote(), raw);
    }
    
    void deleteRaw(String key) {
        raws.remove(key);
    }

    public List<NettyRawChannel<E>> getCreatedRawChannels() {
        return raws.values().stream()
                .filter(x -> x.isCreatorSide())
                 // If the last use is close to current time, it is located at the top of the list
                .sorted((x, y) ->{return (int)(y.lastUse - x.lastUse);})
                .sorted((x, y) ->{return (int)(y.priority - x.priority);})
                .collect(Collectors.toList());
    }

    public List<NettyEndpoint> getRawChannelLocators() {
        return raws.values().stream()
                .sorted((x, y) ->{return (int)(y.lastUse - x.lastUse);})
                .sorted((x, y) ->{return (int)(y.priority - x.priority);})
                .map(x -> {return x.isClosed() ? null : x.getRemote();})
                .filter(x -> x != null)
                .collect(Collectors.toList());
    }
/*
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
*/
    protected abstract NettyRawChannel<E> getRawCreateAsClient(E dst) throws IOException;
    protected abstract boolean filterMessage(NettyMessage<E> msg);
    protected abstract void bootstrap(NettyLocator locator) throws ProtocolUnsupportedException;
    
    // do nothing by default.
    protected void channelSendHook(E src, E dst) {}
    
    protected NettyRawChannel<E> getRawCreateAsClient0(E dst) throws IOException {
        if (!isRunning) return null;
        final NettyRawChannel<E> raw;
        synchronized (raws) {
            NettyRawChannel<E> cached = getRaw(dst);
            if (cached != null) {
                while (cached.getStat() == Stat.INIT || cached.getStat() == Stat.WAIT || cached.getStat() == Stat.DENIED) {
                    try {
                        synchronized(cached) {
                            cached.wait(CHANNEL_ESTABLISH_TIMEOUT);
                        }
                    } catch (InterruptedException e) {
                    }
                }
                // next state should be RUN
                if (cached.getStat() == Stat.RUN) {
                    return cached;
                }
                else {
                    // DEFUNCT. Try a new connection.
                }
            }
            int count = 0;
            for (NettyRawChannel<E> r : getCreatedRawChannels()) {
                // in order of most recently used. 
                if (RAW_POOL_SIZE - 1 <= count) {
                    // logger.info("closing {}, curtime={}", r, System.currentTimeMillis());
                    r.close(); // should close gracefully.
                }
                count++;
            }
            raw = new NettyRawChannel<E>(dst, this, true);
            Bootstrap b = bs.getBootstrap(raw, this); 
            b.connect(dst.getHost(), dst.getPort());
        }
        while (raw.getStat() == Stat.INIT || raw.getStat() == Stat.WAIT) {
            try {
                synchronized(raw) {
                    raw.wait(CHANNEL_ESTABLISH_TIMEOUT);
                }
            } catch (InterruptedException e) {
            }
        }
        if (raw.getStat() == Stat.RUN) {
            return raw;
        }
        else {
            while (raw.getStat() == Stat.DENIED) {
                try {
                    synchronized(raw) {
                        raw.wait(CHANNEL_ESTABLISH_TIMEOUT);
                    }
                }
                catch (InterruptedException e) {
                }
            }
            // accepted other side. 
            if (raw.getStat() == Stat.RUN) {
                return raw;
            }
            // the channel was established but was closed 
            logger.error("getRawChannelAsClient: illegal state: " + raw.getStat());
            raw.close();
            throw new IOException("Channel establish failed.");
        }
    }
    
    abstract protected E createEndpoint(String host, int port);

    void outboundActive(NettyRawChannel<E> raw, ChannelHandlerContext ctx) {
        logger.debug("outbound active: " + ctx.channel().remoteAddress());
        ctx.channel().attr(rawKey).set(raw.getRemote().getKeyString());
        cchannels.add(ctx.channel());
        int attemptRand = rand.nextInt();
        // is this valid only for tcp channel?
        
        InetSocketAddress sa = (InetSocketAddress)ctx.channel().remoteAddress();
        
        E dst = createEndpoint(sa.getHostName(), sa.getPort());
        AttemptMessage<E> attempt = new AttemptMessage<E>(AttemptType.ATTEMPT, locator, attemptRand);
        synchronized(raws) {
            // NettyRawChannel raw = raws.get(locator);
            synchronized (raw) {
                raw.setAttempt(attemptRand);
                raw.setContext(ctx);
                if (raw.getStat() == Stat.INIT) {
                    raw.setStat(Stat.WAIT);
                }
            }
            putRaw(dst, raw);
        }
        ctx.writeAndFlush(attempt);
        logger.debug("sent attempt to " + dst + " : " + ctx);
    }

    void outboundInactive(ChannelHandlerContext ctx) {
        logger.debug("outbound inactive: " + ctx.channel().remoteAddress());
        String key = ctx.channel().attr(rawKey).get();
        //logger.info("outbound raw key: " + key);
        deleteRaw(key);
        /*
        synchronized(raws) {
            NettyRawChannel raw = getRawByContext(ctx);
            if (raw != null) {
                raw.setStat(Stat.DEFUNCT);
                this.deleteRaw(raw);
            }
        }*/
        ctx.close();
    }

    void inboundActive(ChannelHandlerContext ctx) {
        logger.debug("inbound active: " + ctx.channel().remoteAddress());
        schannels.add(ctx.channel());
    }

    void inboundInactive(ChannelHandlerContext ctx) {
        logger.debug("inbound inactive: " + ctx.channel().remoteAddress());
        String key = ctx.channel().attr(rawKey).get();
        //logger.info("inbound raw key : {} on {}", key, ctx.channel().localAddress());
        // Sometimes, the key is null.
        if (key != null) {
            deleteRaw(key);
        }
        /*
        synchronized(raws) {
            NettyRawChannel raw = getRawByContext(ctx);
            if (raw != null) {
                raw.setStat(Stat.DEFUNCT);
                this.deleteRaw(raw);
            }
        }
        */
        ctx.close();
    }

    protected static final int CHANNEL_ESTABLISH_TIMEOUT = 10000;
    protected static final int NAT_FORWARD_HOPS_LIMIT = 3;
    public int forwardCount = 0;

    void inboundReceive(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof AttemptMessage<?>) {
            AttemptMessage<E> attempt = (AttemptMessage<E>) msg;
            logger.debug("received attempt: " + attempt.getArg() + " from "
                    + ctx);
            switch (attempt.type) {
            case ATTEMPT:
                synchronized (raws) {
                    NettyRawChannel<E> raw = getRaw(attempt.getSource());
                    if (raw != null && locator.equals(attempt.getSource())) {
                        // loop back.
                        synchronized (raw) {
                            raw.touch();
                            raw.setStat(Stat.RUN);
                            raw.setContext(ctx);
                            ctx.writeAndFlush(new AttemptMessage<E>(
                                    AttemptType.ACK, locator, null));
                        }
                    } else if (raw != null && raw.attempt != null) {
                        synchronized (raw) {
                            raw.touch();
                            // this side won
                            if (raw.attempt > (int) attempt.getArg()) {
                                logger.debug("attempt won on " + raw);
                                ctx.writeAndFlush(new AttemptMessage<E>(
                                        AttemptType.NACK, locator, null));
                                
                                
                            } else { // opposite side wins.
                                logger.debug("attempt lose on " + raw);
                                ctx.writeAndFlush(new AttemptMessage<E>(
                                        AttemptType.ACK, locator, null));
                                //if (raw.getStat() == Stat.DENIED) {
                                //logger.info("NACK is already received: " + raw);
                                // if NACK is already received, it goes to RUN state.
                                    raw.setContext(ctx);
                                    raw.setStat(Stat.RUN);
                                    raw.notifyAll();
                                //}
                            }
                        } // synchronized raw
                    }
                    // raw.setStat(Stat.RUN);
                    else {
                        // cache not found. just accept it.
                        raw = new NettyRawChannel<E>(attempt.getSource(), this);
                        synchronized(raw) {
                            ctx.channel().attr(rawKey).set(raw.getRemote().getKeyString());
                            // accept attempt.
                            raw.setStat(Stat.RUN);
                            raw.setContext(ctx);
                            logger.debug("set run stat for raw from source="
                                    + attempt.getSource());
                        }
                        ctx.writeAndFlush(new AttemptMessage<E>(AttemptType.ACK,
                                locator, null));
                        putRaw(attempt.getSource(), raw);
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
        } else if (msg instanceof NettyMessage<?>) {
            NettyMessage<E> nmsg = (NettyMessage<E>) msg;
            logger.debug("inbound received msg: " + nmsg.getMsg() + " on " + locator
                    + " from " + nmsg.getSourceLocator() + " to " + nmsg.getDestinationLocator());
            
            if (filterMessage(nmsg)) {
                return;
            }
            
            if (nmsg.isChannelSend()){ 
                NettyChannel<E> ch = null;
                synchronized (channels) {
                    ch = getChannel(nmsg.channelNo(), (E)nmsg.getChannelInitiator());
                    if (ch == null) {
                        synchronized (raws) {
                            NettyRawChannel<E> raw = getRaw(nmsg.getSourceLocator());
                            if (raw == null || raw.getStat() != Stat.RUN) {
                                // might receive message in WAIT state.
                                logger.info(
                                        "receive in illegal state {} from {} (channel not running): throwing it away.",
                                        raw == null ? "null" : raw.getStat(),
                                        nmsg.getSourceLocator());
                            } else {
                                // channel is created on the first message
                                // arrival.
                                ch = new NettyChannel<E>(nmsg.channelNo(),
                                        (E)nmsg.getChannelInitiator(),
                                        (E)nmsg.getChannelInitiator(), // channel initiator is the destination
                                        nmsg.getObjectId(), nmsg.getObjectId(),
                                        false, raw, this);
                                putChannel(ch);
                            }
                        }
                    }
                    else {
                        // If closed, throw the received message away.
                        if (ch.isClosed()) {
                            return;
                        }
                        logger.debug("response for call from inbound on {} received.", ch);
                    }
                }
                if (ch != null) {
                    ch.raw.touch();
                    messageReceived((NettyChannel<E>)ch, nmsg);
                }
            } else {
                logger.debug("received oneway msg={}", msg);
                messageReceived(null, nmsg);
            }
        }
    }

    void outboundReceive(NettyRawChannel<E> raw, ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof AttemptMessage) {
            AttemptMessage<E> resp = (AttemptMessage<E>) msg;
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
                // the connection is not needed anymore.
                ctx.close();
                synchronized(raws) {
//                    NettyRawChannel raw = getRaw(resp.getSource());
                    synchronized(raw) {
                        switch(raw.getStat()) {
                        case RUN:
                            // nothing to do.
                            break;
                        case WAIT:
                            raw.setStat(Stat.DENIED);
                            raw.setContext(null); // not valid context.
                            raw.notifyAll();
                            break;
                        default:
                            logger.debug("illegal raw state {}" + raw);
                            // retry?
                            break;
                        }
                    }
                }
                break;
            }
        }
        else if (msg instanceof NettyMessage) {
            NettyMessage<E> nmsg = (NettyMessage<E>) msg;
            logger.debug("outbound received msg: " + nmsg.getMsg() + " on " + locator + " from " + nmsg.getSourceLocator()  + " to " + nmsg.getDestinationLocator());

            if (filterMessage(nmsg)) {
                return;
            }
            synchronized(raw) {
                if (raw.getStat() != Stat.RUN && raw.getStat() != Stat.DEFUNCT) {
                    // this may happen when the server send a message
                    // immediately after a connection accept.
                    // go to RUN state if there is no explicit close
                    // to allow immediate response.
                    raw.setStat(Stat.RUN);
                }
            }
            if (nmsg.isChannelSend()) {
                NettyChannel<E> ch;
                synchronized (channels) {
                    // get a channel that has remote as the nmsg.source. 
                    ch = getChannel(nmsg.channelNo(), (E)nmsg.getChannelInitiator());
                    logger.debug("got stored ch=" + ch + " for msg: " + nmsg.getMsg());
                    if (ch == null) {
                        // a first call from server.
                        ch = new NettyChannel<E>(nmsg.channelNo(),
                                (E)nmsg.getChannelInitiator(),
                                (E)nmsg.getChannelInitiator(),
                                nmsg.getObjectId(),
                                nmsg.getObjectId(), false, raw, this);
                        putChannel(ch);
                    }
                }
                if (ch != null) {
                    messageReceived((NettyChannel<E>)ch, nmsg);
                }
            }
            else {
                logger.debug("received oneway msg={}", msg);
                messageReceived(null, nmsg);
            }
        }
    }

    // call necessary listeners.
    void messageReceived(NettyChannel<E> c, NettyMessage<E> nmsg) {
        if (!nmsg.isChannelSend()) {
            TransportListener<E> listener = (TransportListener<E>)getListener(nmsg.getObjectId());
            if (listener != null) {
                ReceivedMessage rmsg = new ReceivedMessage(nmsg.getObjectId(), nmsg.getSourceLocator(), nmsg.getMsg());
                logger.debug("trans received {} on {}", rmsg.getMessage(), nmsg.getSourceLocator());
                listener.onReceive((Transport<E>)this, rmsg);
            }
        }
        else {
            ChannelListener<E> clistener = this.getChannelListener(nmsg.getObjectId());
            c.putReceiveQueue(nmsg.getMsg());
            if (clistener != null) {
                clistener.onReceive(c);
            }
        }
    }

    @Override
    public E getEndpoint() {
        return (E)locator;
    }

    @Override
    public Channel<E> newChannel(ObjectId sender, ObjectId receiver,
            E dst, boolean isDuplex, int timeout)
            throws ProtocolUnsupportedException, IOException {
        logger.debug("new channel for: " + dst + " on " + locator);
        NettyRawChannel<E> raw = getRawCreateAsClient(dst);
        if (raw == null) {
            throw new IOException("Getting new raw channel failed (maybe peer down).");
        }
        NettyChannel<E> ch;
        synchronized(channels) {
            ch = new NettyChannel<E>(seq.incrementAndGet(), locator, dst, sender, receiver, true, raw, this);
            putChannel(ch);
        }
        return ch;
    }
}
