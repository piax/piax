/*
 * UdpChannelTransport.java - A Transport on UDP
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.udp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.ChannelTransportImpl;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.kryo.KryoUtil;
import org.piax.gtrans.netty.udp.Signaling.Request;
import org.piax.gtrans.netty.udp.Signaling.Response;
import org.piax.gtrans.netty.udp.UdpPrimaryKey.SIGTYPE;
import org.piax.gtrans.netty.udp.direct.DirectSignaling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpChannelTransport extends ChannelTransportImpl<UdpPrimaryKey> implements ChannelTransport<UdpPrimaryKey> {

    final UdpPrimaryKey ep;
    private EventLoopGroup workerGroup;
    io.netty.channel.Channel bindChannel;
    boolean isRunning;
    Signaling signaling;
    AtomicInteger seq;
    protected static final Logger logger = LoggerFactory.getLogger(UdpChannelTransport.class.getName());
    
    // key=>id channel future.
    ConcurrentHashMap<Comparable<?>,CompletableFuture<UdpIdChannel>> channelFutureMap;
    
    // By default, peerId becomes the primary key. 
    public UdpChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        this(peer, transId, peerId, new UdpPrimaryKey(peerId, peerLocator));
    }
    
    class InboundHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.err.println(cause.getMessage());
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
            ByteBuf buf = msg.content();
            byte[] bytes;
            int offset;
            int length = buf.readableBytes();
            logger.trace("received length={}", buf.readableBytes());
            if (buf.hasArray()) {
                bytes = buf.array();
                offset = buf.arrayOffset();
            } else {
                bytes = new byte[length];
                buf.getBytes(buf.readerIndex(), bytes);
                offset = 0;
            }
            Object obj = KryoUtil.decode(bytes);
            
            if (obj instanceof Request) {
                Request req = (Request)obj;
                req.setSender(msg.sender());
                logger.trace("received request {} from={} on {}", req, req.sender, ep);
                signaling.received(req);
            }
            if (obj instanceof Response) {
                Response resp = (Response)obj;
                resp.setSender(msg.sender());
                logger.trace("received response from={} body={} on {}", resp.sender, resp.body, ep);
                signaling.received(resp);
            }
            else if (obj instanceof NettyMessage<?>) {
                NettyMessage<UdpPrimaryKey> nmsg = (NettyMessage<UdpPrimaryKey>) obj;
                logger.trace("inbound received msg{}: on {}", nmsg, ep);
                messageReceived(null, nmsg);
            }
        }
    }

    @Override
    public void fin() {
        logger.debug("running fin.");
        isRunning = false;
        if (bindChannel.isOpen()) {
            bindChannel.close().awaitUninterruptibly();
        }
        workerGroup.shutdownGracefully();
    }

    class ServerChannelInitializer extends ChannelInitializer<DatagramChannel> {
        @Override
        protected void initChannel(DatagramChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new InboundHandler());
        }
    }
    final protected PeerId peerId;

    public UdpPrimaryKey getSpecifiedEndpoint() {
        return ep;
    }
    
    public UdpChannelTransport(Peer peer, TransportId transId, PeerId peerId, UdpPrimaryKey key) throws IdConflictException, IOException {
        super(peer, transId, null, true);
        
        this.peerId = peerId;
        if (key.getRawKey() == null) { // This means empty key or '*' is specified in spec.
            key.setRawKey(peerId);
        }
        this.ep = key;
        int port = key.getLocator().getPort();

        workerGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ServerChannelInitializer());
        bindChannel = bootstrap.bind(new InetSocketAddress(port)).syncUninterruptibly().channel();
        isRunning = true;
        seq = new AtomicInteger(0);
        if (key.sigType.equals(SIGTYPE.DIRECT)) {
            signaling = new DirectSignaling(this); // signaling is assigned in it.
        }
        else {
            throw new IOException("only direct signaling is implemented for now.");
        }
        channelFutureMap = new ConcurrentHashMap<>();
    }
    
    public NettyLocator getPrimaryLocator(Comparable<?> key) {
        return signaling.getLocatorManager().getPrimaryLocator(key);
    }

    public UdpChannelTransport(Peer peer, TransportId transId, PeerId peerId) throws IdConflictException, IOException {
        this(peer, transId, peerId, new UdpPrimaryKey(peerId, null));
    }

    @Override
    public UdpPrimaryKey getEndpoint() {
        return ep;
        /*if (ep.getRawKey() == null) { // self is wildcard? NO.
            System.err.println("self key is null");
        }
        return new UdpPrimaryKey(ep.getRawKey(), signaling.getLocatorManager().getPrimaryLocator(ep.getRawKey())); */
    }
    
    public ChannelFuture rawSend(NettyLocator addr, Object obj) {
        logger.trace("sending to " + addr);
        return rawSend(addr.getSocketAddress(), obj);
    }
    
    public ChannelFuture rawSend(InetSocketAddress addr, Object obj) {
        logger.trace("raw send to={}", addr);
        assert(addr != null) : "destination addr is null";
        ByteBuf buf = Unpooled.wrappedBuffer(KryoUtil.encode(obj, 256, 256));
        logger.trace("writing length={}, to={}", buf.readableBytes(), addr);
        return bindChannel.writeAndFlush(new DatagramPacket(buf, addr));
    }
    
    public CompletableFuture<UdpIdChannel> newChannelAsync(ObjectId sender, ObjectId receiver, UdpPrimaryKey dst) {
        CompletableFuture<UdpIdChannel> retf = new CompletableFuture<>();
        CompletableFuture<UdpRawChannel> f = signaling.doSignaling(ep, dst);
        f.whenComplete((raw, ex) -> {
            logger.debug("signaling completed");
            if (ex != null) {
                retf.completeExceptionally(ex);
            }
           retf.complete(new UdpIdChannel(seq.incrementAndGet(), ep, raw.dst, sender, receiver, true, raw, this)); 
        });
        return retf;
    }

    @Override
    public Channel<UdpPrimaryKey> newChannel(ObjectId sender, ObjectId receiver, UdpPrimaryKey dst, boolean isDuplex,
            int timeout) throws ProtocolUnsupportedException, IOException {
        try {
            return newChannelAsync(sender, receiver, dst).get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.info("While channel signaling: ", e);
        }
        return null;
    }
    
    @Override
    public CompletableFuture<Void> sendAsync(ObjectId sender, ObjectId receiver, UdpPrimaryKey dst,
            Object msg, TransOptions opts) {
        if (opts == null) {
            opts = new TransOptions();
        }
        // opts is ignored in this layer.
        NettyMessage<UdpPrimaryKey> nmsg = new NettyMessage<UdpPrimaryKey>(receiver, ep, dst, null, getPeerId(), msg, false, 0);
        logger.debug("dst={}, src={}", dst ,ep);
        if (ep.getRawKey() != null && ep.getRawKey().equals(dst.getRawKey())) { // loop back
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
            CompletableFuture<UdpIdChannel> f;
            synchronized (channelFutureMap) {
                f = dst.getRawKey() == null ? channelFutureMap.get(dst.getLocator().getKeyString()) : channelFutureMap.get(dst.getRawKey());
                if (f == null) {
                    logger.debug("not found for {} on {}.", dst.getRawKey(), ep);
                    // opts is ignored at this time.
                    f = newChannelAsync(sender, receiver, dst);
                    channelFutureMap.put(dst.getRawKey() == null ? dst.getLocator().getKeyString() : dst.getRawKey(), f);
                    logger.debug("put {} on {}.", dst, ep);
                }
                else {
                    logger.debug("found {} for {}.", f, dst.getRawKey());
                }
            }
            CompletableFuture<UdpIdChannel> ffin = f; // for effectively final.
            f.whenComplete((ret, e) -> {
                logger.debug("COMPLETED {}", ret);
                if (e == null) {
                    if (dst.getRawKey() == null) { // was wildcard.
                        logger.debug("add mapping from {} to {} for {}.", dst.getLocator().getKeyString(), ret.dst.getRawKey(), ret);
                        //channelFutureMap.remove(dst.getLocator().getKeyString());
                        channelFutureMap.put(ret.dst.getRawKey(), ffin);
                    }
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

    void messageReceived(UdpIdChannel c, NettyMessage<UdpPrimaryKey> nmsg) {
        if (!nmsg.isChannelSend()) {
            TransportListener<UdpPrimaryKey> listener = (TransportListener<UdpPrimaryKey>)getListener(nmsg.getObjectId());
            if (listener != null) {
                ReceivedMessage rmsg = new ReceivedMessage(nmsg.getObjectId(), nmsg.getSource(), nmsg.getMsg());
                logger.debug("trans received {} from {}", rmsg.getMessage(), nmsg.getSource());
                listener.onReceive((Transport<UdpPrimaryKey>)this, rmsg);
            }
        }
        else {
            logger.debug("channel trans received {} from {}", nmsg.getMsg(), nmsg.getSource());
            ChannelListener<UdpPrimaryKey> clistener = this.getChannelListener(nmsg.getObjectId());
            c.putReceiveQueue(nmsg.getMsg());
            if (clistener != null) {
                clistener.onReceive(c);
            }
        }
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver, UdpPrimaryKey dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        sendAsync(sender, receiver, dst, msg, opts);
    }
}
