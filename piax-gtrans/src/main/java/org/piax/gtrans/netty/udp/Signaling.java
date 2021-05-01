/*
 * Signaling.java - Signaling definition
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.udp;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyLocator.TYPE;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Signaling {
    final ConcurrentHashMap<Integer,Request> pending = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Function<Request,CompletableFuture>> responders = new ConcurrentHashMap<>();
    static AtomicInteger seq = new AtomicInteger();
    static int SIGNAL_TIMEOUT = 10000;
    protected final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    final protected UdpChannelTransport trans;
    protected static final Logger logger = LoggerFactory.getLogger(Signaling.class);

    public static class SignalingException extends Exception {
        public SignalingException(String s) {
            super(s);
        }
        public SignalingException(Exception e) {
            super(e);
        }
    }
    static class SignalingMessage implements Serializable {
    }
    
    static public class Request<T> extends SignalingMessage {
        int id;
        transient public CompletableFuture<Response<T>> future;
        transient Signaling sig;
        transient protected InetSocketAddress sender; // null in sender side;
        final public UdpPrimaryKey senderKey;
        final public NettyLocator dst;
        final boolean requireResponse;

        public String toString() {
            return "[" + id + ",senderKey=" + senderKey + ",dst=" + (dst==null? "NULL" : dst) + ",resp=" + requireResponse + "]";
        }
        
        // no response
        public Request(Signaling sig, UdpPrimaryKey senderKey, NettyLocator dst) {
            this.id = seq.incrementAndGet();
            this.future = null;
            this.dst = dst;
            this.sig = sig;
            this.senderKey = senderKey;
            sender = null;
            requireResponse = false;
        }
        // request and response
        public Request(Signaling sig, UdpPrimaryKey senderKey, NettyLocator dst, BiConsumer<Response<T>, ? super Throwable> consumer) {
            this.id = seq.incrementAndGet();
            this.future = new CompletableFuture<>();
            future.whenComplete(consumer);
            this.dst = dst;
            this.sig = sig;
            this.senderKey = senderKey;
            sender = null;
            requireResponse = true;
        }
        
        public void setSender(InetSocketAddress sender) {
            this.sender = sender;
        }
        
        public InetSocketAddress getSender() {
            return sender;
        }

        public void post() throws SignalingException {
            if (requireResponse && future != null) {
                sig.pending(this);
            }
            try {
                sig.post(dst, this);
            } catch (Exception e) {
                throw new SignalingException(e);
            }
        }
        
        
    }
    
    public void setResponder(String name, Function<Request, CompletableFuture> f) {
        responders.put(name, f);
    }
    
    protected void post(InetSocketAddress dst, Object obj) {
        trans.rawSend(dst, obj);
    }
    
    protected void post(NettyLocator dst, Object obj) {
        trans.rawSend(dst, obj);
    }
    
    public Signaling(UdpChannelTransport trans) {
        this.trans = trans;
    }

    static public class Response<T> extends SignalingMessage {
        // request identifier;
        final public int id;
        final public T body;
        final public UdpPrimaryKey senderKey;
        final NettyLocator dst;
        InetSocketAddress sender;
        public Response(int id, NettyLocator dst, UdpPrimaryKey senderKey, T body) {
            this.id = id;
            this.dst = dst;
            this.senderKey = senderKey;
            this.body = body;
        }
        public NettyLocator getDestination() {
            return dst;
        }
        public void setSender(InetSocketAddress sender) {
            this.sender = sender;
        }
        public InetSocketAddress getSender() {
            return sender;
        }
    }

    private void pending(Request req) {
        pending.put(req.id, req);
        scheduler.schedule(()->{
            Request pr = pending.get(req.id);
            if (pr != null) {
                req.future.completeExceptionally(new Exception());
                pending.remove(req.id);
            }
        }, SIGNAL_TIMEOUT, TimeUnit.MILLISECONDS);
    }
    
    // request receiver side
    public void received(Request req) {
        CompletableFuture f = responders.get(req.getClass().getName()).apply(req);
        logger.debug("responder got {}", req);
        f.whenComplete((ret, e) -> {
            if (e != null) {
                // logger.warn("not response was not obtained.");
            }
            else {
                logger.debug("send response to {} from {}", req.sender, trans.getEndpoint());
                post(req.sender, new Response(req.id, 
                        new NettyLocator(TYPE.UDP, req.sender), trans.getEndpoint(), ret));
            }
        });
    }

    // request sender side
    public void received(Response resp) {
        Request req = pending.get(resp.id);
        if (req != null) {
            req.future.complete(resp);
            pending.remove(resp.id);
        }
    }

    // do a signaling to obtain a channel to the destination.
    public abstract CompletableFuture<UdpRawChannel> doSignaling(PrimaryKey src, PrimaryKey dst);
    
    public abstract UdpLocatorManager getLocatorManager();
}
