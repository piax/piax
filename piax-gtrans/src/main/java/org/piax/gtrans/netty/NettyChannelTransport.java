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
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.ChannelTransportImpl;
import org.piax.gtrans.netty.NettyRawChannel.Stat;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap;
import org.piax.gtrans.netty.bootstrap.SslBootstrap;
import org.piax.gtrans.netty.bootstrap.TcpBootstrap;
import org.piax.gtrans.netty.bootstrap.UdtBootstrap;
import org.piax.gtrans.netty.nat.NATLocatorManager;
import org.piax.gtrans.netty.nat.NettyNATLocator;
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
    // XXX should be private
    public NATLocatorManager nMgr; 
    NettyBootstrap bs;
    AtomicInteger seq;
    final public int RAW_POOL_SIZE = 10;
    
    public AttributeKey<String> rawKey = AttributeKey.valueOf("rawKey");

    enum AttemptType {
        ATTEMPT, ACK, NACK 
    }
    
    static boolean NAT_SUPPORT = true;

    public NettyChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, null, true);
        this.locator = peerLocator;
        this.peerId = peerId;
        seq = new AtomicInteger(0);// sequence number (ID of the channel)
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
        if (NAT_SUPPORT && peerLocator instanceof NettyNATLocator) {
        }
        else {
            ServerBootstrap b = bs.getServerBootstrap(this);
            b.bind(new InetSocketAddress(peerLocator.getHost(), peerLocator.getPort()));//.syncUninterruptibly();
            logger.debug("bound " + peerLocator);
        }
        if (NAT_SUPPORT) {
            nMgr = new NATLocatorManager();
            if (peerLocator instanceof NettyNATLocator) {
                nMgr.register((NettyNATLocator)peerLocator);
            }
        }
        isRunning = true;
    }

    @Override
    public void fin() {
        logger.debug("running fin.");
        if (NAT_SUPPORT) {
            nMgr.fin();
        }
        isRunning = false;
        cchannels.close().awaitUninterruptibly();
        schannels.close().awaitUninterruptibly();
        bossGroup.shutdownGracefully();
        serverGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
    }
    
    private NettyRawChannel findExistingRawChannel(List<NettyLocator> list) {
        NettyRawChannel ret = null;
        for(ListIterator<NettyLocator> it=list.listIterator(list.size()); it.hasPrevious();){
            if (it.previousIndex() == 0) { // don't return the first element(default);
                return null;
            }
            NettyLocator l = it.previous();
            ret = getRaw(l);
            if (ret != null) {
                break;
            }
        }
        return ret;
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
        NettyLocator src = raw.getLocal();
        NettyMessage nmsg = new NettyMessage(receiver, src, dst, null, raw.getPeerId(), msg, false, 0);
        raw.touch();
        raw.send(nmsg);
    }

    void putChannel(NettyLocator channelInitiator, NettyChannel ch) {
        logger.debug("" + ch.getChannelNo() + channelInitiator.getKeyString() + "->" + ch + " on " + locator);
        channels.put("" + ch.getChannelNo() + channelInitiator.getKeyString(), ch);
    }

    NettyChannel getChannel(int channelNo, NettyLocator channelInitiator) {
        logger.debug("" + channelNo + channelInitiator.getKeyString() + " on " + locator);
        return channels.get("" + channelNo + channelInitiator.getKeyString());
    }

    void deleteChannel(NettyChannel ch) {
        channels.remove("" + ch.getChannelNo() + ch.channelInitiator.getKeyString(), ch);
    }

    // package local
    void putRaw(NettyLocator locator, NettyRawChannel ch) {
        if (raws.size() == 0) { // the first channel. its premier.
            ch.setPriority(1);
        }
        raws.put(locator.getKeyString(), ch);
    }

    NettyRawChannel getRaw(NettyLocator locator) {
        return raws.get(locator.getKeyString());
    }

    void deleteRaw(NettyRawChannel raw) {
        raws.remove(raw.getRemote().getKeyString(), raw);
    }
    
    void deleteRaw(String key) {
        raws.remove(key);
    }

    public List<NettyRawChannel> getCreatedRawChannels() {
        return raws.values().stream()
                .filter(x -> x.isCreatorSide())
                 // If the last use is close to current time, it is located at the top of the list
                .sorted((x, y) ->{return (int)(y.lastUse - x.lastUse);})
                .sorted((x, y) ->{return (int)(y.priority - x.priority);})
                .collect(Collectors.toList());
    }

    public List<NettyLocator> getRawChannelLocators() {
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
    NettyRawChannel getRawCreateAsClient(NettyLocator dst) throws IOException {
        return getRawCreateAsClient(dst, false);
    }
    
    NettyRawChannel getRawCreateAsClient(NettyLocator dst, boolean useDefaultForNAT) throws IOException {
        NettyRawChannel raw = null;
        if (NAT_SUPPORT) {
            if (dst instanceof NettyNATLocator) {
                dst = nMgr.getRegister((NettyNATLocator)dst); // get fresh locator;
                List<NettyLocator> list = ((NettyNATLocator) dst).getRawChannelLocators();
                logger.debug("fresh dst length={} on {} for {}", list.size(), locator, dst);
                if (useDefaultForNAT) {
                    raw = getRawCreateAsClient0(list.get(0));
                    logger.debug("use rawchannel(default) for {} is {} on {}", dst, raw.getRemote(), locator);
                    return raw;
                }
                raw = getRaw(dst);
                if (raw != null) {
                    logger.debug("existing rawchannel for {} is {} on {}", dst, raw.getRemote(), locator);
                    return raw;
                }
                else {
                    raw = findExistingRawChannel(list);
                    if (raw == null) { // no exiting channel. contact latest peer.
                        if (list.size() == 1) {
                            raw = getRawCreateAsClient0(list.get(0));
                            logger.debug("only one rawchannel for {} is {} on {}", dst, raw.getRemote(), locator);
                        }
                        else {
                            int pos = (int)(Math.random() * list.size() - 2);
                            raw = getRawCreateAsClient0(list.get(pos + 1));
                            logger.debug("created rawchannel for {} is {} on {}", dst, raw.getRemote(), locator);
                        }
                    }
                    else {
                        logger.debug("forward existing to rawchannel for {} is {} on {}", dst, raw.getRemote(), locator);
                    }
                }
            }
        }
        
        if (raw == null) {
            raw = getRawCreateAsClient0(dst);
        }
        if (NAT_SUPPORT) {
            if (locator instanceof NettyNATLocator) {
                ((NettyNATLocator) locator).updateRawChannelLocators(this);
            }
        }
        return raw;
    }

    NettyRawChannel getRawCreateAsClient0(NettyLocator dst) throws IOException {
        if (!isRunning) return null;
        final NettyRawChannel raw;
        synchronized (raws) {
            NettyRawChannel cached = getRaw(dst);
            if (cached != null) {
                while (cached.getStat() == Stat.INIT || cached.getStat() == Stat.WAIT || cached.getStat() == Stat.DENIED) {
                    try {
                        synchronized(cached) {
                            cached.wait(CHANNEL_ESTABLISH_TIMEOUT);
                            logger.info("waiting for RUN/DEFUNCT state. current:" + cached.getStat());
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
            for (NettyRawChannel r : getCreatedRawChannels()) {
                // in order of most recently used. 
                if (RAW_POOL_SIZE - 1 <= count) {
                    // logger.info("closing {}, curtime={}", r, System.currentTimeMillis());
                    r.close(); // should close gracefully.
                }
                count++;
            }
            raw = new NettyRawChannel(dst, this, true);
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
            logger.debug("created raw channel become available=" + raw);
            return raw;
        }
        else {
            // not accepted other side yet...
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
            logger.warn("getRawChannelAsClient: illegal state: " + raw.getStat());
            //raw.close();
            throw new IOException("Channel establish failed.");
        }
    }

    void outboundActive(NettyRawChannel raw, ChannelHandlerContext ctx) {
        logger.debug("outbound active: " + ctx.channel().remoteAddress());
        ctx.channel().attr(rawKey).set(raw.getRemote().getKeyString());
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

    static final int CHANNEL_ESTABLISH_TIMEOUT = 10000;
    static final int NAT_FORWARD_HOPS_LIMIT = 3;
    public int forwardCount = 0;

    boolean forwardNATMessage(NettyMessage nmsg) {
        NettyLocator dst = nmsg.getDestinationLocator();
        if (locator.equals(dst)) {
            return false; // go to receive process.
        }
        if (dst instanceof NettyNATLocator || !dst.equals(locator)) {
            if (nmsg.getHops() == NAT_FORWARD_HOPS_LIMIT) {
                logger.warn("Exceeded forward hops to {} on {}.", dst, locator);
                return true;
            }
            nmsg.incrementHops();
            logger.debug("NAT forwarding from {} to {} on {}", nmsg.getSourceLocator(), dst, locator);
            try {
                NettyRawChannel raw = getRawCreateAsClient(dst, (nmsg.getHops() == NAT_FORWARD_HOPS_LIMIT -1));
                raw.touch();
                raw.send(nmsg);
            } catch (IOException e) {
                logger.warn("Exception occured: " + e.getMessage());
            }
            return true;
        }
        return false;
    }

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
                            raw.touch();
                            raw.setStat(Stat.RUN);
                            raw.setContext(ctx);
                            ctx.writeAndFlush(new AttemptMessage(
                                    AttemptType.ACK, locator, null));
                        }
                    } else if (raw != null && raw.attempt != null) {
                        raw.touch();
                        synchronized (raw) {
                            // this side won
                            if (raw.attempt > (int) attempt.getArg()) {
                                logger.info("attempt won on " + raw);
                                ctx.writeAndFlush(new AttemptMessage(
                                        AttemptType.NACK, locator, null));
                                
                                
                            } else { // opposite side wins.
                                logger.info("attempt lose on " + raw);
                                ctx.writeAndFlush(new AttemptMessage(
                                        AttemptType.ACK, locator, null));
                                if (raw.getStat() == Stat.DENIED) {
                                    // if NACK is already received, it goes to RUN state.
                                    raw.setContext(ctx);
                                    raw.setStat(Stat.RUN);
                                    raw.notifyAll();
                                }
                            }
                        } // synchronized raw
                    }
                    // raw.setStat(Stat.RUN);
                    else {
                        // cache not found. just accept it.
                        raw = new NettyRawChannel(attempt.getSource(), this);
                        ctx.channel().attr(rawKey).set(raw.getRemote().getKeyString());
                        // accept attempt.
                        raw.setStat(Stat.RUN);
                        raw.setContext(ctx);
                        logger.debug("set run stat for raw from source="
                                + attempt.getSource());
                        ctx.writeAndFlush(new AttemptMessage(AttemptType.ACK,
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
        } else if (msg instanceof NettyMessage) {
            NettyMessage nmsg = (NettyMessage) msg;
            logger.debug("inbound received msg: " + nmsg.getMsg() + " on " + locator
                    + " from " + nmsg.getSourceLocator() + " to " + nmsg.getDestinationLocator());
            if (NAT_SUPPORT) {
                NettyLocator src = nmsg.getSourceLocator();
                if (src instanceof NettyNATLocator) {
                    src = nMgr.getRegister((NettyNATLocator)src);
                    nmsg.setSourceLocator(src);
                }
                if (forwardNATMessage(nmsg)) {
                    return;
                }
                logger.debug("not a NAT message or received for {} on {}", nmsg.getDestinationLocator(), locator);
            }
            if (nmsg.isChannelSend()){ 
                NettyChannel ch = null;
                synchronized (channels) {
                    ch = getChannel(nmsg.channelNo(), nmsg.getChannelInitiator());
                    if (ch == null) {
                        synchronized (raws) {
                            NettyRawChannel raw = getRaw(nmsg.getSourceLocator());
                            if (raw == null || raw.getStat() != Stat.RUN) {
                                // might receive message in WAIT state.
                                logger.info(
                                        "receive in illegal state {} from {} (channel not running): throwing it away.",
                                        raw == null ? "null" : raw.getStat(),
                                        nmsg.getSourceLocator());
                            } else {
                                // channel is created on the first message
                                // arrival.
                                ch = new NettyChannel(nmsg.channelNo(),
                                        nmsg.getChannelInitiator(),
                                        nmsg.getChannelInitiator(), // channel initiator is the destination
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
                    ch.raw.touch();
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
                logger.info("NACK received on {}", raw);
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
            NettyMessage nmsg = (NettyMessage) msg;
            logger.debug("outbound received msg: " + nmsg.getMsg() + " on " + locator + " from " + nmsg.getSourceLocator()  + " to " + nmsg.getDestinationLocator());
            if (NAT_SUPPORT) {
                NettyLocator src = nmsg.getSourceLocator();
                if (src instanceof NettyNATLocator) {
                    src = nMgr.getRegister((NettyNATLocator)src);
                    nmsg.setSourceLocator(src);
                }
                if (forwardNATMessage(nmsg)) {
                    return;
                }
                logger.debug("not a NAT message or received for {} on {}", nmsg.getDestinationLocator(), locator);
            }
            if (nmsg.isChannelSend()) {
                NettyChannel ch;
                synchronized (channels) {
                    // get a channel that has remote as the nmsg.source. 
                    ch = getChannel(nmsg.channelNo(), nmsg.getChannelInitiator());
                    logger.debug("got stored ch=" + ch + " for msg: " + nmsg.getMsg());
                    if (ch == null) {
                        // a first call from server.
                        ch = new NettyChannel(nmsg.channelNo(),
                                nmsg.getChannelInitiator(),
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
            ch = new NettyChannel(seq.incrementAndGet(), locator, dst, sender, receiver, true, raw, this);
            putChannel(locator, ch);
        }
        return ch;
    }
}
