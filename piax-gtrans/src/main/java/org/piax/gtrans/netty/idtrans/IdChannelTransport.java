package org.piax.gtrans.netty.idtrans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.ObjectId;
import org.piax.common.Option.IntegerOption;
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
import org.piax.gtrans.netty.ControlMessage;
import org.piax.gtrans.netty.ControlMessage.ControlType;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap;
import org.piax.gtrans.netty.bootstrap.SslBootstrap;
import org.piax.gtrans.netty.bootstrap.TcpBootstrap;
import org.piax.gtrans.netty.bootstrap.UdtBootstrap;
import org.piax.gtrans.netty.idtrans.LocatorChannel.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;

/* TODO:
 * - close asynchronously & gracefully.
 * - NAT handling.
 * - locator change notification / periodic ping / failure detector.
 * - 
 */
public class IdChannelTransport extends ChannelTransportImpl<PrimaryKey> implements ChannelTransport<PrimaryKey> {
    LocatorManager mgr;
    protected static final Logger logger = LoggerFactory.getLogger(IdChannelTransport.class.getName());
    protected final ConcurrentHashMap<String,IdChannel> ichannels = new ConcurrentHashMap<String,IdChannel>();
    
    protected EventLoopGroup bossGroup;
    protected EventLoopGroup serverGroup;
    protected EventLoopGroup clientGroup;
    boolean supportsDuplex = true;
    protected PrimaryKey ep = null;
    final protected PeerId peerId;
    // a map to hold active raw channels;

    private ConcurrentHashMap<NettyLocator, LocatorChannelEntry> raws;
    protected final Random rand = new Random(System.currentTimeMillis());
    protected boolean isRunning = false;

    final protected ChannelGroup schannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    final protected ChannelGroup cchannels =
            new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    final ChannelFuture serverFuture;

    private NettyBootstrap<PrimaryKey> bs;
    private AtomicInteger seq;

    static public IntegerOption RAW_POOL_SIZE = new IntegerOption(30, "-pool-size");
    
    public AttributeKey<String> rawChannelKey = AttributeKey.valueOf("rawKey");

    // LocatorChannelEntry:
    // channel ... the locator channel. when its stat becomes RUN, the future must be completed.
    // primaryKey ... the primaryKey of the remote node.
    // future is completed when:
    // 1) connection is opened, sent control: ATTEMPT and control: ACK is received
    // 2) connection is opened, sent control: ATTEMPT and control: NACK is received, and connection is accepted
    // 3) control: WAIT is received, and connection is accepted.
    class LocatorChannelEntry {
        public CompletableFuture<LocatorChannel> future;
        public LocatorChannel channel;

        public LocatorChannelEntry(LocatorChannel channel,
                CompletableFuture<LocatorChannel> future) {
            this.channel = channel;
            this.future = future;
        }
    }

    @Sharable
    class InboundHandler extends ChannelInboundHandlerAdapter {
        IdChannelTransport trans;
        public InboundHandler(IdChannelTransport trans) {
            this.trans = trans;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            trans.inboundActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            trans.getPeer().execute(() -> {
                trans.inboundReceive(ctx, msg);
            });
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.debug("Inactive={}", ctx);
            trans.inboundInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.info("Exception:", cause);
            ctx.close();
        }
    }

    @Sharable
    class OutboundHandler extends ChannelInboundHandlerAdapter {
        LocatorChannelEntry ent;
        IdChannelTransport trans;
        public OutboundHandler(LocatorChannelEntry ent, IdChannelTransport trans) {
            this.ent = ent;
            this.trans = trans;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // this handler is a client-side handler.
            trans.outboundActive(ent, ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            trans.getPeer().execute(() -> {
                trans.outboundReceive(ent, ctx, msg);
            });
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // this handler is a client-side handler.
            logger.debug("Inactive={}", ctx);
            trans.outboundInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.info("exception={}", cause);
            trans.outboundInactive(ctx);
        }
    }

    protected void inboundActive(ChannelHandlerContext ctx) {
        logger.debug("inbound active: " + ctx.channel().remoteAddress());
        schannels.add(ctx.channel());
    }

    @SuppressWarnings("unchecked")
    protected void inboundReceive(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ControlMessage<?>) {
            ControlMessage<PrimaryKey> cmsg = (ControlMessage<PrimaryKey>) msg;
            PrimaryKey curSrc = mgr.updateAndGet(cmsg.getSource());
            logger.debug("received attempt: " + cmsg.getArg() + " from " + ctx);
            switch (cmsg.type) {
            case ATTEMPT:
                synchronized (raws) {
                    LocatorChannelEntry ent = raws.get(curSrc.getLocator());
                    if (ent != null && ep.equals(curSrc)) {
                        logger.debug("loop back:" + ep);
                        // loop back.
                        synchronized (ent) {
                            ent.channel.touch();
                            ent.channel.setChannel(ctx.channel());
                            ent.channel.setStat(Stat.RUN);
                            ent.channel.setPrimaryKey(ep);
                            ent.future.complete(ent.channel);
                            ctx.writeAndFlush(new ControlMessage<PrimaryKey>(
                                    ControlType.ACK, ep, ep, null));
                        }
                    } else if (ent != null && ent.channel.attempt != null) {
                        logger.debug("NOT a loop back:" + ep);
                        synchronized (ent) {
                            ent.channel.touch();
                            // this side won
                            if (ent.channel.attempt > (int) cmsg.getArg()) {
                                logger.debug("attempt won on " + ent.channel);
                                ctx.writeAndFlush(new ControlMessage<PrimaryKey>(
                                        ControlType.NACK, ep, null, null));
                            } else { // opposite side wins.
                                logger.debug("attempt lose on " + ent.channel);
                                ctx.writeAndFlush(new ControlMessage<PrimaryKey>(
                                        ControlType.ACK, ep, null, null));
                                if (ent.channel.getStat() == Stat.WAIT || ent.channel.getStat() == Stat.DENIED) {
                                    logger.debug("set as RUN by accepting channel on loser: {}", ep);
                                    if (ent.channel.getChannel() != null) { // null: already denied.
                                        ent.channel.getChannel().close();
                                    }
                                    ent.channel.setChannel(ctx.channel());
                                    ent.channel.touch();
                                    ent.channel.setStat(Stat.RUN);
                                    ent.channel.setPrimaryKey(curSrc);
                                    ent.future.complete(ent.channel); // a channel initiated by opposite side
                                }
                                //}
                            }
                        } // synchronized raw
                    }
                    // raw.setStat(Stat.RUN);
                    else {
                        // cache not found. just accept it.
                        ent = new LocatorChannelEntry(new LocatorChannel(curSrc.getLocator(), this),
                                new CompletableFuture<>());
                        synchronized(ent) {
                            ctx.channel().attr(LCE_KEY).set(ent);
                            // accept attempt.
                            ent.channel.setStat(Stat.RUN);
                            ent.channel.setChannel(ctx.channel());
                            ent.channel.setPrimaryKey(curSrc);
                            ent.future.complete(ent.channel);
                            logger.debug("set run stat for raw from source="
                                    + curSrc);
                        }
                        ctx.writeAndFlush(new ControlMessage<PrimaryKey>(ControlType.ACK,
                                ep, null, null));
                        raws.put(curSrc.getLocator(), ent);
                    }
                } // synchronized raws
                break;
            case ACK:
                logger.debug("attempt ACK received from client");
                break;
            case NACK:
                logger.debug("attempt NACK received from client");
                break;
            case CLOSE:
                logger.debug("close id channel: {}/{}", (int)cmsg.getArg(), curSrc);
                IdChannel c = ichannels.get(IdChannel.getKeyString((int)cmsg.getArg(), curSrc));
                if (c != null) {
                    closeIdChannel(ichannels.get(IdChannel.getKeyString((int)cmsg.getArg(),curSrc)));
                }
                break;
            default:
                logger.error("Unimplemented control message was received.");
                break;
            }
        } else if (msg instanceof NettyMessage<?>) {
            NettyMessage<PrimaryKey> nmsg = (NettyMessage<PrimaryKey>) msg;
            logger.debug("inbound received msg{}: on {}", nmsg, ep);
            PrimaryKey curSrc = mgr.updateAndGet(nmsg.getSource());
            /*if (filterMessage(nmsg)) {
                return;
            }*/
            if (nmsg.isChannelSend()){ 
                IdChannel ch = null;
                synchronized (ichannels) {
                    ch = ichannels.get(IdChannel.getKeyString(nmsg.channelNo(), (PrimaryKey)nmsg.getChannelInitiator()));
                    if (ch == null) {
                        // piggybacking the channel initiation.
                        synchronized (raws) {
                            LocatorChannelEntry ent = raws.get(curSrc.getLocator());
                            logger.debug("locator channel for {} is {} ", curSrc.getLocator(), ent);
                            if (ent == null || ent.channel.getStat() != Stat.RUN) {
                                // might receive message in WAIT state.
                                logger.warn(
                                        "receive in illegal state {} from {} (channel not running): throwing it away.",
                                        ent == null ? "null" : ent.channel.getStat(),
                                        nmsg.getSource());
                            } else {
                                // channel is created on the first message arrival
                                expireClosedIdChannels(); // XXX performance
                                ch = new IdChannel(nmsg.channelNo(),
                                        (PrimaryKey)nmsg.getChannelInitiator(),
                                        (PrimaryKey)nmsg.getChannelInitiator(), // channel initiator is the destination
                                        nmsg.getObjectId(), nmsg.getObjectId(),
                                        false, ent.channel, this);
                                ichannels.put(ch.getKeyString(), ch);
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
                    messageReceived((IdChannel)ch, nmsg);
                }
            } else {
                logger.debug("received oneway msg={}", msg);
                messageReceived(null, nmsg);
            }
        }
    }
    
    void messageReceived(IdChannel c, NettyMessage<PrimaryKey> nmsg) {
        if (!nmsg.isChannelSend()) {
            TransportListener<PrimaryKey> listener = (TransportListener<PrimaryKey>)getListener(nmsg.getObjectId());
            if (listener != null) {
                ReceivedMessage rmsg = new ReceivedMessage(nmsg.getObjectId(), nmsg.getSource(), nmsg.getMsg());
                logger.debug("trans received {} from {}", rmsg.getMessage(), nmsg.getSource());
                listener.onReceive((Transport<PrimaryKey>)this, rmsg);
            }
        }
        else {
            ChannelListener<PrimaryKey> clistener = this.getChannelListener(nmsg.getObjectId());
            c.putReceiveQueue(nmsg.getMsg());
            if (clistener != null) {
                clistener.onReceive(c);
            }
        }
    }
    
    public AttributeKey<LocatorChannelEntry> LCE_KEY = AttributeKey.valueOf("locatorChannelEntry");

    protected void inboundInactive(ChannelHandlerContext ctx) {
        logger.debug("inbound inactive: " + ctx.channel().remoteAddress());
        LocatorChannelEntry lce = ctx.channel().attr(LCE_KEY).get();
        if (lce != null) {
            raws.remove(lce.channel.getRemote());
        }
        ctx.close();
    }

    void outboundActive(LocatorChannelEntry ent, ChannelHandlerContext ctx) {
        logger.debug("outbound active: {} on {}", ctx.channel().remoteAddress(), ep);
        ctx.channel().attr(LCE_KEY).set(ent);
        cchannels.add(ctx.channel());
        int attemptRand = rand.nextInt();
        // is this valid only for tcp channel?
        InetSocketAddress sa = (InetSocketAddress)ctx.channel().remoteAddress();
        NettyLocator dst = new NettyLocator(sa.getHostName(), sa.getPort());
        ControlMessage<PrimaryKey> attempt = new ControlMessage<PrimaryKey>(ControlType.ATTEMPT, ep, null, attemptRand); // XXX dst key should be specified.
        synchronized(raws) {
            // NettyRawChannel raw = raws.get(locator);
            synchronized (ent) {
                ent.channel.setAttempt(attemptRand);
                ent.channel.setChannel(ctx.channel());
                if (ent.channel.getStat() == Stat.INIT) {
                    ent.channel.setStat(Stat.WAIT);
                }
            }
            raws.put(dst, ent);
        }
        ctx.writeAndFlush(attempt);
        logger.debug("sent attempt to " + dst + " : " + ctx);
    }

    @SuppressWarnings("unchecked")
    void outboundReceive(LocatorChannelEntry ent, ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ControlMessage) {
            ControlMessage<PrimaryKey> cmsg = (ControlMessage<PrimaryKey>) msg;
            PrimaryKey curSrc = mgr.updateAndGet(cmsg.getSource());// primary key of the opposite side 
            logger.debug("outbound attempt response=" + cmsg.type + ",from=" + curSrc + ",on=" + ep);
            switch(cmsg.type) {
            case ATTEMPT:
                logger.debug("attempt received from server");
                break;
            case ACK:
                synchronized(ent) {
                    ent.channel.setStat(Stat.RUN);
                    ent.channel.setPrimaryKey(curSrc);
                    ent.future.complete(ent.channel);
                }
                break;
            case NACK:
                // there should be a attempt thread on this peer.
                // the connection is not needed anymore.
                ctx.close();
                synchronized(ent) {
                    switch(ent.channel.getStat()) {
                    case RUN:
                        // nothing to do...other side
                        break;
                    case WAIT:
                        logger.debug("received NAC while WAITING, set DENIED.");
                        ent.channel.setStat(Stat.DENIED);
                        ent.channel.setChannel(null); // wait for the other side set a valid channel
                        break;
                    default:
                        logger.debug("illegal raw state {}" + ent.channel);
                        // retry?
                        break;
                    }
                }
                break;
            case CLOSE:
                logger.debug("close id channel: {}/{}", (int)cmsg.getArg(), curSrc);
                IdChannel c = ichannels.get(IdChannel.getKeyString((int)cmsg.getArg(), curSrc));
                if (c != null) {
                    closeIdChannel(ichannels.get(IdChannel.getKeyString((int)cmsg.getArg(), curSrc)));
                }
                break;
            }
        }
        else if (msg instanceof NettyMessage) {
            NettyMessage<PrimaryKey> nmsg = (NettyMessage<PrimaryKey>) msg;
            PrimaryKey curSrc = mgr.updateAndGet(nmsg.getSource());// primary key of the opposite side 
            logger.debug("outbound received msg {} on {}", nmsg, ep);

            /*if (filterMessage(nmsg)) {
                return;
            }*/
            synchronized(ent) {
                if (ent.channel.getStat() != Stat.RUN && ent.channel.getStat() != Stat.DEFUNCT) {
                    // this may happen when the server send a message
                    // immediately after a connection accept.
                    // go to RUN state if there is no explicit close
                    // to allow immediate response.
                    // ent.channel.setStat(Stat.RUN);
                }
            }
            if (nmsg.isChannelSend()) {
                IdChannel ch = null;
                synchronized (ichannels) {
                    // get a channel that has remote as the nmsg.source. 
                    ch = ichannels.get(IdChannel.getKeyString(nmsg.channelNo(), nmsg.getChannelInitiator()));
                    logger.debug("got stored ch=" + ch + " for msg: " + nmsg.getMsg());
                    if (ch == null) {
                        // a first call from server.
                        expireClosedIdChannels(); // XXX performance
                        ch = new IdChannel(nmsg.channelNo(),
                                nmsg.getChannelInitiator(),
                                nmsg.getChannelInitiator(),
                                nmsg.getObjectId(),
                                nmsg.getObjectId(), false, ent.channel, this);
                        logger.debug("new channel for {} by {}", ent, nmsg.getMsg());
                        ichannels.put(ch.getKeyString(), ch);
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

    protected void outboundInactive(ChannelHandlerContext ctx) {
        logger.debug("outbound inactive: {} on {}", ctx.channel().remoteAddress(), ep);
        LocatorChannelEntry ent = ctx.channel().attr(LCE_KEY).get();
        raws.remove(ent.channel.getRemote());
        ctx.close();
    }

    protected static final int FORWARD_HOPS_LIMIT = 5;

    public int forwardCount = 0; // just for monitoring

    protected static final int ID_CHANNEL_REMOVE_CLOSED_THRESHOLD = 2000;
    void closeIdChannel(IdChannel ch) {
        ch.isClosed = true; // just mark as closed.
        // the IdChannel entry will be expired after ID_CHANNEL_REMOVE_CLOSED_THRESHOLD
    }
    void expireClosedIdChannels() {
        for (Iterator<Entry<String, IdChannel>> it = ichannels.entrySet().iterator();
                it.hasNext();) {
            if (it.next().getValue().elapsedTimeAfterClose() > ID_CHANNEL_REMOVE_CLOSED_THRESHOLD) {
                it.remove();
            }
        }
    }


    // By default, peerId becomes the primary key. 
    public IdChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        this(peer, transId, peerId, new PrimaryKey(peerId, peerLocator));
    }

    public IdChannelTransport(Peer peer, TransportId transId, PeerId peerId, PrimaryKey key) throws IdConflictException, IOException {
        super(peer, transId, null, true);
        this.peerId = peerId;
        if (key.getRawKey() == null) { // This means empty key or '*' is specified in spec.
            key.setRawKey(peerId);
        }
        this.ep = key;
        NettyLocator peerLocator = key.getLocator(); 
        mgr = new LocatorManager();
        raws = new ConcurrentHashMap<>();
        seq = new AtomicInteger(0);// sequence number (ID of the channel)
        if (peerLocator != null) {
            switch (peerLocator.getType()) {
            case TCP:
                bs = new TcpBootstrap<PrimaryKey>();
                break;
            case SSL:
                bs = new SslBootstrap<PrimaryKey>(peerLocator.getHost(), peerLocator.getPort());
                break;
            case UDT:
                bs = new UdtBootstrap<PrimaryKey>();
                break;
            default:
                throw new ProtocolUnsupportedException("not implemented yet.");
            }
            bossGroup = bs.getParentEventLoopGroup();
            serverGroup = bs.getChildEventLoopGroup();
            clientGroup = bs.getClientEventLoopGroup();
            AbstractBootstrap b = bs.getServerBootstrap(new InboundHandler(this));
            serverFuture = b.bind(new InetSocketAddress(peerLocator.getHost(), peerLocator.getPort()))
                    .syncUninterruptibly();
            logger.debug("bound " + ep);
        }
        else {
            serverFuture = null;
        }
        isRunning = true;
    }

    public IdChannelTransport(Peer peer, TransportId transId, PeerId peerId) throws IdConflictException, IOException {
        this(peer, transId, peerId, new PrimaryKey(peerId, null));
    }

    
/*
    public boolean filterMessage (NettyMessage<PrimaryKey> nmsg) {
        PrimaryKey src = nmsg.getSource();
        src = mgr.registerAndGet(src);
        nmsg.setSource(src);
        return forwardMessage(nmsg);
    }

    // forwarding the channel attempt signal:
    // first, use channels for neighbors if already exists.
    // second, initiate a channel for neighbors if not exists.

    @Deprecated
    boolean forwardMessage(NettyMessage<PrimaryKey> nmsg) {
        PrimaryKey dst = (PrimaryKey)nmsg.getDestination();
        if (ep.equals(dst)) {
            return false; // go to receive process.
        }
        else {
            if (nmsg.getHops() == FORWARD_HOPS_LIMIT) {
                logger.warn("Exceeded forward hops to {} on {}.", dst, ep);
                return true;
            }
            nmsg.incrementHops();
            logger.debug("NAT forwarding from {} to {} on {}", nmsg.getSource(), dst, ep);
            try {
                // the hop count affects on resolving a locator.
                NettyRawChannel<PrimaryKey> raw = getRawCreateAsClient(dst, nmsg);
                raw.touch();
                raw.send(nmsg);
            } catch (IOException e) {
                logger.warn("Exception occured: " + e.getMessage());
            }
            return true;
        }
    }
*/
    /*
    // handle the control message for IdChannelTrans.
    protected void handleControlMessage(ControlMessage<PrimaryKey> cmsg) {
        switch(cmsg.getType()) {
        case UPDATE: // update locator information.
            {
                PrimaryKey k = (PrimaryKey)cmsg.getArg();
            } 
            break;
        case INIT: // start a new channel initiation
            {
                PrimaryKey k = (PrimaryKey)cmsg.getArg();
            }
            break;
        case WAIT:
            // wait for the connection from specified locator...do nothing.
            break;
        default:
            break;
        }
    }*/

    public void waitForFin() {
        serverFuture.channel().closeFuture().awaitUninterruptibly();
    }

    @Override
    public boolean isUp() {
        return isRunning;
    }
    
    public void fin() {
        logger.debug("running fin.");
        isRunning = false;
        cchannels.close().awaitUninterruptibly();
        schannels.close().awaitUninterruptibly();
        if (serverFuture.channel().isOpen()) {
            serverFuture.channel().close().awaitUninterruptibly();
        }
        bossGroup.shutdownGracefully();
        serverGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        mgr.fin();
    }

    private CompletableFuture<LocatorChannel> createLocatorChannel(NettyLocator locator, TransOptions opts) {
        LocatorChannelEntry ent = new LocatorChannelEntry(new LocatorChannel(locator, this), new CompletableFuture<>());
        logger.debug("initiating locator channel to {}", locator);
        raws.put(locator, ent);
        if (!isRunning) {
            ent.future.completeExceptionally(new IOException("generating a channel on stopped transport."));
        }
        else {
            Bootstrap b = bs.getBootstrap(locator, new OutboundHandler(ent, this));
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int)opts.getTimeout());
            
            ChannelFuture f = bs.connect(b, locator.getHost(), locator.getPort());
            
            if (f != null) {
            f.addListener((future) -> {
                if (future.isCancelled()) {
                    logger.trace("connection timed out to {}: {} sec", locator, opts.getTimeout());
                    ent.future.completeExceptionally(new IOException("connection timed out to " + locator + ": " + opts.getTimeout() + " sec"));
                }
                else if (!future.isSuccess()){
                    ent.future.completeExceptionally(new IOException(f.cause()));
                }
                // change locator channel entry to the one with <future and channel>
                ent.channel.setNettyChannel(((ChannelFuture)future).channel()); 
            });
            }
        }
        return ent.future;
    }

    public CompletableFuture<LocatorChannel> getRawCreate(PrimaryKey key, NettyLocator dst, TransOptions opts) {
        synchronized(raws) {
            ArrayList<LocatorChannelEntry> obsoletes = new ArrayList<>();
            raws.values().stream()
           .sorted((x, y) ->{return (int)(x.channel.lastUse - y.channel.lastUse);})
           .limit(RAW_POOL_SIZE.value() > raws.size() ? 0 : raws.size() - RAW_POOL_SIZE.value())
           .forEach((ent) -> {
                // in order of most recently unused. 
               obsoletes.add(ent);
            });
            // XXX this may takes a while.
            obsoletes.stream().forEach((ent) -> {
                try {
                    if (ent.channel.closeAsync(true).get()) {
                        raws.remove(ent.channel.getRemote());
                    }
                } catch (Exception e) {
                    // remove anyway.
                    raws.remove(ent.channel.getRemote());
                }
                logger.debug("channel expired for: " + ent + " on " + ep);
            });

            LocatorChannelEntry ent = raws.get(dst);
            if (ent == null) {
                return createLocatorChannel(dst, opts);
            }
            else {
                return ent.future;
            }
        }
    }

    public CompletableFuture<IdChannel> getChannelCreate(int channelNo, PrimaryKey dst, ObjectId sender, ObjectId receiver, TransOptions opts) {
        synchronized(ichannels) {
            // dst.key == null means wildcard. should be new.
            IdChannel ch = (dst.rawKey == null) ? null : ichannels.get(IdChannel.getKeyString(channelNo, dst));
            if (ch == null) {
                NettyLocator direct = mgr.getLocator(dst);//dst.getLocator();
                if (direct != null) {// outside NAT
                    PrimaryKey curDst = mgr.updateAndGet(dst); 
                    CompletableFuture<IdChannel> future = new CompletableFuture<>();
                    CompletableFuture<LocatorChannel> lfuture = getRawCreate(curDst, direct, opts);
                    lfuture.whenComplete((ret, e) -> {
                        logger.debug("getRawCreate is completed with:" + ret);
                        if (e != null) {
                            future.completeExceptionally(e);
                        }
                        else {
                            expireClosedIdChannels(); // XXX performance
                            IdChannel newCh = new IdChannel(seq.incrementAndGet(), ep,
                                    //dst, 
                                    ret.getPrimaryKey(), // destination: the primary key obtained from remote.
                                    sender, receiver, true, ret, this);
                            // update wildcard
                            if (curDst.rawKey == null) {
                                // key of the locator is obtained from remote.
                                mgr.updateKey(direct, ret.getPrimaryKey());
                                //dst.key = ret.getPrimaryKey().key;
                            }
                            ichannels.put(newCh.getKeyString(), newCh);
                            future.complete(newCh);
                        }
                    });
                    return future;
                }
            }
            return CompletableFuture.completedFuture(ch);
        }
    } 

    @Override
    public Channel<PrimaryKey> newChannel(ObjectId sender, ObjectId receiver,
            PrimaryKey dst, boolean isDuplex, int timeout)
            throws ProtocolUnsupportedException, IOException {
        logger.debug("new channel for: " + dst + " on " + ep);

        IdChannel ret;
        try {
            ret = getChannelCreate(seq.incrementAndGet(), dst, sender, receiver, new TransOptions(timeout)).get();
        } catch (Exception e) {
            throw new IOException(e);
        }
        return ret;
    }
    
    @Override
    public void send(ObjectId sender, ObjectId receiver, PrimaryKey dst,
            Object msg, TransOptions opts) throws ProtocolUnsupportedException, IOException {
        sendAsync(sender, receiver, dst, msg, opts);
    }

    @Override
    public CompletableFuture<Void> sendAsync(ObjectId sender, ObjectId receiver, PrimaryKey dst,
            Object msg, TransOptions opts) {
        if (opts == null) {
            opts = new TransOptions();
        }
        // opts is ignored in this layer.
        NettyMessage<PrimaryKey> nmsg = new NettyMessage<PrimaryKey>(receiver, ep, dst, null, getPeerId(), msg, false, 0);
        if (ep.equals(dst)) { // loop back
            messageReceived(null, nmsg);
            return CompletableFuture.completedFuture(null);
        }
        else {
        
        // generate a new channel if not exists
        logger.debug("sending async {}", nmsg);
        CompletableFuture<Void> retf = new CompletableFuture<>();
        if (!isRunning) {
            logger.debug("{} is not running", this);
            retf.completeExceptionally(new IOException("transport is not running"));
            return retf;
        }
        CompletableFuture<IdChannel> f = getChannelCreate(0, dst, sender, receiver, opts);
        f.whenComplete((ret, e) -> {
            if (e == null) {
                try {
                    logger.debug("sending async when completed: {} {}", ret, nmsg);
                    ret.sendAsync(nmsg).addListener((cf) -> {
                        retf.complete(null);
                    });
                } catch (Exception e1) {
                    retf.completeExceptionally(e1);
                }
            }
            else {
                retf.completeExceptionally(e);
            }
        });
        return retf;
        }
    }

    @Override
    public PrimaryKey getEndpoint() {
        return ep;
    }

}
