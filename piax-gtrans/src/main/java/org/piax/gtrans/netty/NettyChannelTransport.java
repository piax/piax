package org.piax.gtrans.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
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
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.ChannelTransportImpl;
import org.piax.gtrans.netty.ControlMessage.ControlType;
import org.piax.gtrans.netty.NettyRawChannel.Stat;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap;
import org.piax.gtrans.netty.bootstrap.SslBootstrap;
import org.piax.gtrans.netty.bootstrap.TcpBootstrap;
import org.piax.gtrans.netty.bootstrap.UdtBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NettyChannelTransport<E extends NettyEndpoint> extends ChannelTransportImpl<E> implements ChannelTransport<E> {

    protected static final Logger logger = LoggerFactory.getLogger(NettyChannelTransport.class.getName());
    protected EventLoopGroup bossGroup;
    protected EventLoopGroup serverGroup;
    protected EventLoopGroup clientGroup;
    boolean supportsDuplex = true;
    protected E ep = null;
    final protected PeerId peerId;
    // a map to hold active raw channels;
    protected final ConcurrentHashMap<E,NettyRawChannel<E>> raws =
            new ConcurrentHashMap<E,NettyRawChannel<E>>();
    protected final ConcurrentHashMap<String,NettyChannel<E>> channels =
            new ConcurrentHashMap<String,NettyChannel<E>>();
    protected final Random rand = new Random(System.currentTimeMillis());
    protected boolean isRunning = false;
    
    final protected ChannelGroup schannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    final protected ChannelGroup cchannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    // XXX should be private
    protected NettyBootstrap<E> bs;
    protected AtomicInteger seq;

    static public int RAW_POOL_SIZE = 30;
    static public boolean PARALLEL_RECEIVE = true;
    
    public AttributeKey<String> rawChannelKey = AttributeKey.valueOf("rawKey");

    //static boolean NAT_SUPPORT = true;

    public NettyChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, null, true);
        this.ep = (E)peerLocator;
        this.peerId = peerId;
        seq = new AtomicInteger(0);// sequence number (ID of the channel)
        if (peerLocator != null) {
            switch (peerLocator.getType()) {
            case TCP:
                bs = new TcpBootstrap<E>();
                break;
            case SSL:
                bs = new SslBootstrap<E>(ep.getHost(), ep.getPort());
                break;
            case UDT:
                bs = new UdtBootstrap<E>();
                break;
            default:
                throw new ProtocolUnsupportedException("not implemented yet.");
            }
            bossGroup = bs.getParentEventLoopGroup();
            serverGroup = bs.getChildEventLoopGroup();
            clientGroup = bs.getClientEventLoopGroup();

            ServerBootstrap b = bs.getServerBootstrap(new NettyInboundHandler(this));
            b.bind(new InetSocketAddress(ep.getHost(), ep.getPort()))
                    .syncUninterruptibly();
            logger.debug("bound " + ep);
        }
        isRunning = true;
    }

    // for subclass implementation.
    public NettyChannelTransport(Peer peer, TransportId transId, PeerId peerId) throws IdConflictException, IOException {
        super(peer, transId, null, true);
        this.peerId = peerId;
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
            Object msg, TransOptions opts) throws ProtocolUnsupportedException, IOException {
        // opts is ignored in this layer.
//        E src = (E)raw.getLocal();
        E src = ep;
        channelSendHook(src, dst);
        NettyMessage<E> nmsg = new NettyMessage<E>(receiver, src, dst, null, getPeerId(), msg, false, 0);
        // generate a new channel if not exists
        NettyRawChannel<E> raw = getRawCreateAsClient(dst, nmsg);
        if (raw == null) {
            throw new IOException("Getting new raw channel failed (maybe peer down).");
        }
        logger.debug("oneway send to {} from {} msg={}", dst, ep, msg);
        raw.touch();
        raw.send(nmsg);
    }

    void putChannel(NettyChannel<E> ch) {
        logger.debug("" + ch.getChannelNo() + ch.channelInitiator.hashCode() + "->" + ch + " on " + ep);
        channels.put("" + ch.getChannelNo() + ch.channelInitiator.hashCode(), ch);
    }

    protected NettyChannel<E> getChannel(int channelNo, E channelInitiator) {
        logger.debug("" + channelNo + channelInitiator.hashCode() + " on " + ep);
        return channels.get("" + channelNo + channelInitiator.hashCode());
    }

    void deleteChannel(NettyChannel<?> ch) {
        channels.remove("" + ch.getChannelNo() + ch.channelInitiator.hashCode(), ch);
    }

    // package local
    void putRaw(E ep, NettyRawChannel<E> ch) {
        if (raws.size() == 0) { // the first channel. its premier.
            ch.setPriority(1);
        }
        raws.put(ep, ch);
    }

    protected NettyRawChannel<E> getRaw(E ep) {
        return raws.get(ep);
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
    protected abstract NettyRawChannel<E> getRawCreateAsClient(E dst, NettyMessage<E> msg) throws IOException;
    protected abstract boolean filterMessage(NettyMessage<E> msg);

    protected abstract NettyRawChannel<E> getResolvedRawChannel(E ep) throws IOException;
    protected abstract NettyLocator directLocator(E ep);
    
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
                    logger.debug("closing {}, curtime={}", r, System.currentTimeMillis());
                    r.close(); // should close gracefully.
                }
                count++;
            }
            raw = new NettyRawChannel<E>(dst, this, true);
            NettyLocator l = directLocator(dst);
            if (l != null) {
                Bootstrap b = bs.getBootstrap(l, new NettyOutboundHandler(raw, this));
                b.connect(l.getHost(), l.getPort());
            }
            else {
                logger.debug("destination is not directly connectable.");
            }
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

    protected void outboundActive(NettyRawChannel<E> raw, ChannelHandlerContext ctx) {
        logger.debug("outbound active: " + ctx.channel().remoteAddress());
        ctx.channel().attr(rawChannelKey).set(raw.getRemote().getKeyString());
        cchannels.add(ctx.channel());
        int attemptRand = rand.nextInt();
        // is this valid only for tcp channel?
        InetSocketAddress sa = (InetSocketAddress)ctx.channel().remoteAddress();
        E dst = createEndpoint(sa.getHostName(), sa.getPort());
        ControlMessage<E> attempt = new ControlMessage<E>(ControlType.ATTEMPT, ep, null, attemptRand);
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

    protected void outboundInactive(ChannelHandlerContext ctx) {
        logger.debug("outbound inactive: " + ctx.channel().remoteAddress());
        String key = ctx.channel().attr(rawChannelKey).get();
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

    protected void inboundActive(ChannelHandlerContext ctx) {
        logger.debug("inbound active: " + ctx.channel().remoteAddress());
        schannels.add(ctx.channel());
    }

    protected void inboundInactive(ChannelHandlerContext ctx) {
        logger.debug("inbound inactive: " + ctx.channel().remoteAddress());
        String key = ctx.channel().attr(rawChannelKey).get();
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

    protected void handleControlMessage(ControlMessage<E> cmsg) {
        // do nothing.
        logger.warn("unhandled control message:" + cmsg);
    }
    
    protected void inboundReceive(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ControlMessage<?>) {
            ControlMessage<E> cmsg = (ControlMessage<E>) msg;
            logger.debug("received attempt: " + cmsg.getArg() + " from " + ctx);
            switch (cmsg.type) {
            case ATTEMPT:
                synchronized (raws) {
                    NettyRawChannel<E> raw = getRaw(cmsg.getSource());
                    if (raw != null && ep.equals(cmsg.getSource())) {
                        // loop back.
                        synchronized (raw) {
                            raw.touch();
                            raw.setStat(Stat.RUN);
                            raw.setContext(ctx);
                            ctx.writeAndFlush(new ControlMessage<E>(
                                    ControlType.ACK, ep, ep, null));
                        }
                    } else if (raw != null && raw.attempt != null) {
                        synchronized (raw) {
                            raw.touch();
                            // this side won
                            if (raw.attempt > (int) cmsg.getArg()) {
                                logger.debug("attempt won on " + raw);
                                ctx.writeAndFlush(new ControlMessage<E>(
                                        ControlType.NACK, ep, null, null));
                                
                                
                            } else { // opposite side wins.
                                logger.debug("attempt lose on " + raw);
                                ctx.writeAndFlush(new ControlMessage<E>(
                                        ControlType.ACK, ep, null, null));
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
                        raw = new NettyRawChannel<E>(cmsg.getSource(), this);
                        synchronized(raw) {
                            ctx.channel().attr(rawChannelKey).set(raw.getRemote().getKeyString());
                            // accept attempt.
                            raw.setStat(Stat.RUN);
                            raw.setContext(ctx);
                            logger.debug("set run stat for raw from source="
                                    + cmsg.getSource());
                        }
                        ctx.writeAndFlush(new ControlMessage<E>(ControlType.ACK,
                                ep, null, null));
                        putRaw(cmsg.getSource(), raw);
                    }
                } // synchronized raws
                break;
            case ACK:
                logger.debug("illegal attempt ACK received from client");
                break;
            case NACK:
                logger.debug("illegal attempt NACK received from client");
                break;
            default:
                handleControlMessage(cmsg);
                break;
            }
        } else if (msg instanceof NettyMessage<?>) {
            NettyMessage<E> nmsg = (NettyMessage<E>) msg;
            logger.debug("inbound received msg: " + nmsg.getMsg() + " on " + ep
                    + " from " + nmsg.getSource() + " to " + nmsg.getDestination());
            
            if (filterMessage(nmsg)) {
                return;
            }
            
            if (nmsg.isChannelSend()){ 
                NettyChannel<E> ch = null;
                synchronized (channels) {
                    ch = getChannel(nmsg.channelNo(), (E)nmsg.getChannelInitiator());
                    if (ch == null) {
                        synchronized (raws) {
                            NettyRawChannel<E> raw = getRaw(nmsg.getSource());
                            if (raw == null || raw.getStat() != Stat.RUN) {
                                // might receive message in WAIT state.
                                logger.warn(
                                        "receive in illegal state {} from {} (channel not running): throwing it away.",
                                        raw == null ? "null" : raw.getStat(),
                                        nmsg.getSource());
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
        if (msg instanceof ControlMessage) {
            ControlMessage<E> resp = (ControlMessage<E>) msg;
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
            logger.debug("outbound received msg: " + nmsg.getMsg() + " on " + ep + " from " + nmsg.getSource()  + " to " + nmsg.getDestination());

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
                ReceivedMessage rmsg = new ReceivedMessage(nmsg.getObjectId(), nmsg.getSource(), nmsg.getMsg());
                logger.debug("trans received {} on {}", rmsg.getMessage(), nmsg.getSource());
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
        return (E)ep;
    }

    @Override
    public Channel<E> newChannel(ObjectId sender, ObjectId receiver,
            E dst, boolean isDuplex, int timeout)
            throws ProtocolUnsupportedException, IOException {
        logger.debug("new channel for: " + dst + " on " + ep);
        NettyRawChannel<E> raw = getRawCreateAsClient(dst, null);
        if (raw == null) {
            throw new IOException("Getting new raw channel failed (maybe peer down).");
        }
        NettyChannel<E> ch;
        synchronized(channels) {
            ch = new NettyChannel<E>(seq.incrementAndGet(), ep, dst, sender, receiver, true, raw, this);
            putChannel(ch);
        }
        return ch;
    }
}
