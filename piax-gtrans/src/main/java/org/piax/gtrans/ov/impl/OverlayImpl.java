/*
 * OverlayImpl.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: OverlayImpl.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.ov.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.dcl.DCLTranslator;
import org.piax.gtrans.dcl.parser.ParseException;
import org.piax.gtrans.impl.RequestTransportImpl;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public abstract class OverlayImpl<D extends Destination, K extends Key> extends
        RequestTransportImpl<D> implements Overlay<D, K> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(OverlayImpl.class);

    private final Map<ObjectId, Map<K, Integer>> keysByUpper =
            new HashMap<ObjectId, Map<K, Integer>>();
    protected final Map<K, Integer> keyRegister = new ConcurrentHashMap<K, Integer>();
    protected volatile boolean isJoined = false;
    final DCLTranslator parser = new DCLTranslator();

    /**
     * @param peer the peer object.
     * @param transId the id of the transport.
     * @param lowerTrans the lower transport.
     * @throws IdConflictException thrown when the id is conflicted.
     */
    public OverlayImpl(Peer peer, TransportId transId, Transport<?> lowerTrans)
            throws IdConflictException {
        super(peer, transId, lowerTrans);
    }

    public Class<?> getAvailableKeyType() {
        /*
         * TODO think!
         * 本来なら、Key型にしたいところであるが、今はAttributeでの型チェックでKey型にしておくと、
         * ComparableなKey型との比較ができなくなる。従来のObject型でここは考えておく
         */
        return Object.class;
    }

    public void setListener(ObjectId upper, OverlayListener<D, K> listener) {
        super.setListener(upper, listener);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public OverlayListener<D, K> getListener(ObjectId upper) {
        return (OverlayListener<D, K>) super.getListener(upper);
    }
    
    public void send(ObjectId sender, ObjectId receiver, String dstExp,
            Object msg) throws ParseException, ProtocolUnsupportedException,
            IOException {
        @SuppressWarnings("unchecked")
        D dst = (D) parser.parseDestination(dstExp);
        send(sender, receiver, dst, msg);
    }

    public void send(TransportId transId, String dstExp,
            Object msg) throws ParseException,
            ProtocolUnsupportedException, IOException {
        send(transId, transId, dstExp, msg);
    }
    
    // Utility functions by teranisi
    public <E> FutureQueue<E> singletonFutureQueue(E value) {
        FutureQueue<E> fq = new FutureQueue<E>();
        fq.add(new RemoteValue<E>(peerId, value));
        fq.setEOFuture();
        return fq;
    }
    
    public <E> FutureQueue<E> singletonFutureQueue(E value, Throwable t) {
        FutureQueue<E> fq = new FutureQueue<E>();
        fq.add(new RemoteValue<E>(peerId, value, t));
        fq.setEOFuture();
        return fq;
    }
    
    public void send(ObjectId appId, String dstExp,
            Object msg) throws ParseException, ProtocolUnsupportedException, IOException {
    		send(appId, appId, dstExp, msg);
    }
    
    public void send(String dstExp,
            Object msg) throws ParseException, ProtocolUnsupportedException, IOException {
    		send(getDefaultAppId(), dstExp, msg);
    }
    
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            String dstExp, Object msg)
            throws ParseException, ProtocolUnsupportedException, IOException {
        return request(sender, receiver, dstExp, msg, null);
    }

    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            String dstExp, Object msg, TransOptions opts)
            throws ParseException, ProtocolUnsupportedException, IOException {
    		try {
    			@SuppressWarnings("unchecked")
    			D dst = (D) parser.parseDestination(dstExp);
    			return request(sender, receiver, dst, msg, opts);
    		}
    		catch (ParseException e) {
    			/* XXX Try parse as DCL */
    			@SuppressWarnings("unchecked")
    	        D dc = (D) parser.parseDCL(dstExp);
    	        return request(sender, receiver, dc, msg, opts);
    		}
    }
    
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            String dstExp, Object msg, int timeout)
            throws ParseException, ProtocolUnsupportedException, IOException {
        return request(sender, receiver, dstExp, msg, new TransOptions(timeout));
    }

    public FutureQueue<?> request(TransportId transId, String dstExp,
            Object msg, int timeout) throws ParseException,
            ProtocolUnsupportedException, IOException {
        return request(transId, transId, dstExp, msg, timeout);
    }
    
    public FutureQueue<?> request(ObjectId appId, String dstExp,
            Object msg, TransOptions opts) throws ParseException,
            ProtocolUnsupportedException, IOException {
    		return request(appId, appId, dstExp, msg, opts);
    }
    
    public FutureQueue<?> request(ObjectId appId, String dstExp,
            Object msg, int timeout) throws ParseException,
            ProtocolUnsupportedException, IOException {
        return request(appId, appId, dstExp, msg, timeout);
    }
    
    public FutureQueue<?> request(ObjectId appId, String dstExp, Object msg) throws ParseException,
    			ProtocolUnsupportedException, IOException {
    		return request(appId, appId, dstExp, msg);
    }
    
    public FutureQueue<?> request(String dstExp, Object msg) throws ParseException,
    			ProtocolUnsupportedException, IOException {
    		return request(getDefaultAppId(), dstExp, msg);
    }

    public FutureQueue<?> request(String dstExp, Object msg, TransOptions opts)
    			throws ParseException, ProtocolUnsupportedException, IOException {
    		return request(getDefaultAppId(), dstExp, msg, opts);
    }
    
    public FutureQueue<?> request(String dstExp, Object msg, int timeout)
			throws ParseException, ProtocolUnsupportedException, IOException {
		return request(getDefaultAppId(), dstExp, msg, timeout);
    }
    
    // async request interface
    public void requestAsync(ObjectId sender, ObjectId receiver,
            String dstExp, Object msg,
            BiConsumer<Object, Exception> responseReceiver,
            TransOptions opts) throws ParseException, ProtocolUnsupportedException, IOException {
        try {
            @SuppressWarnings("unchecked")
            D dst = (D) parser.parseDestination(dstExp);
            requestAsync(sender, receiver, dst, msg, responseReceiver, opts);
        }
        catch (ParseException e) {
            /* XXX Try parse as DCL */
            @SuppressWarnings("unchecked")
            D dc = (D) parser.parseDCL(dstExp);
            requestAsync(sender, receiver, dc, msg, responseReceiver, opts);
        }
    }

    protected Object selectOnReceive(OverlayListener<D, K> listener,
            Overlay<D, K> trans, OverlayReceivedMessage<K> rmsg) {
        logger.trace("ENTRY:");
        Object msg = rmsg.getMessage();
        logger.debug("msg {}", msg);
        Object inn = checkAndClearIsEasySend(msg);
        if (NON != inn) {
            rmsg.setMessage(inn);
            logger.debug("select onReceive: trans:{}", trans.getTransportId());
            listener.onReceive(trans, rmsg);
            return null;//FutureQueue.emptyQueue();
        } else {
            return listener.onReceiveRequest(trans, rmsg);
        }
    }

    protected int numOfRegisteredKey(K key) {
        Integer count = keyRegister.get(key);
        if (count == null) {
            return 0;
        } else {
            return count;
        }
    }

    protected void registerKey(K key) {
        synchronized(keyRegister) {
            Integer count = keyRegister.get(key);
            if (count == null) {
                keyRegister.put(key, 1);
            } else {
                keyRegister.put(key, count + 1);
            }
        }
    }
    
    protected void registerKey(ObjectId upper, K key) {
        Map<K, Integer> keyCounts = keysByUpper.get(upper);
        if (keyCounts == null) {
            keyCounts = new HashMap<K, Integer>();
            keysByUpper.put(upper, keyCounts);
        }
        Integer count = keyCounts.get(key);
        if (count == null) {
            keyCounts.put(key, 1);
        } else {
            keyCounts.put(key, count + 1);
        }
        registerKey(key);
    }
    
    protected boolean unregisterKey(K key) {
        synchronized(keyRegister) {
            Integer count = keyRegister.get(key);
            if (count == null) return false;
            if (count == 1) {
                keyRegister.remove(key);
            } else {
                keyRegister.put(key, count - 1);
            }
        }
        return true;
    }
    
    protected boolean unregisterKey(ObjectId upper, K key) {
        Map<K, Integer> keyCounts = keysByUpper.get(upper);
        if (keyCounts == null) return false;
        Integer count = keyCounts.get(key);
        if (count == null) return false;
        if (count == 1) {
            keyCounts.remove(key);
        } else {
            keyCounts.put(key, count - 1);
        }
        if (!unregisterKey(key)) {
            logger.error("keyRegister should have specified key");
            return false;
        }
        return true;
    }

    protected void lowerAddKey(K key) throws IOException {
    }
    
    public boolean addKey(ObjectId upper, K key) throws IOException {
        this.checkActive();
        synchronized (keyRegister) {
            // if this key not exists, add to overlay
            if (!keyRegister.containsKey(key)) {
                lowerAddKey(key);
            }
            registerKey(upper, key);
            return true;
        }
    }
    
    public boolean addKey(K key) throws IOException {
    		return addKey(getDefaultAppId(), key);
    }

    protected CompletableFuture<Boolean> lowerAddKeyAsync(K key) {
        return CompletableFuture.completedFuture(true);
    }

    public CompletableFuture<Boolean> addKeyAsync(ObjectId upper, K key) {
        this.checkActive();
        boolean exists = false;
        CompletableFuture<Boolean> ret = CompletableFuture.completedFuture(true);
        synchronized (keyRegister) {
            // if this key not exists, add to overlay
            exists = keyRegister.containsKey(key);
        }
        if (!exists) {
            ret = lowerAddKeyAsync(key);
            ret = ret.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.warn("addKeyAsync: {}", ex);
                }
                if (result) {
                    synchronized (keyRegister) {
                        registerKey(upper, key);
                    }
                }
            });
        } else {
            synchronized (keyRegister) {
                registerKey(upper, key);
            }
        }
        return ret;
    }

    public CompletableFuture<Boolean> addKeyAsync(K key) {
        return addKeyAsync(getDefaultAppId(), key);
    }

//    public boolean addKey(ObjectId upper, String key) throws IOException {
//        return addKey(upper, (Key) new WrappedComparableKey<String>(key));
//    }
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public boolean addKey(ObjectId upper, Number key) throws IOException {
//        return addKey(upper, (Key) new WrappedComparableKey((Comparable<?>) key));
//    }

    protected void lowerRemoveKey(K key) throws IOException {
    }

    public boolean removeKey(ObjectId upper, K key) throws IOException {
        this.checkActive();
        if (key instanceof PrimaryKey || key instanceof PeerId) {
            throw new IllegalArgumentException("Primary key or Peer Id cannot be removed (leave instead)");
        }
        synchronized (keyRegister) {
            if (!keyRegister.containsKey(key)) {
                return false;
            }
            // if this key is single, do remove from overlay
            if (numOfRegisteredKey(key) == 1) {
                lowerRemoveKey(key);
            }
            return unregisterKey(upper, key);
        }
    }
    public boolean removeKey(K key) throws IOException {
    		return removeKey(getDefaultAppId(), key);
    }

    protected CompletableFuture<Boolean> lowerRemoveKeyAsync(K key) {
        return CompletableFuture.completedFuture(true);
    }

    public CompletableFuture<Boolean> removeKeyAsync(ObjectId upper, K key) {
        this.checkActive();
        int num;
        CompletableFuture<Boolean> ret = CompletableFuture.completedFuture(false);
        if (key instanceof PrimaryKey || key instanceof PeerId) {
            throw new IllegalArgumentException("Primary key or Peer Id cannot be removed (leave instead)");
        }
        synchronized (keyRegister) {
            if (!keyRegister.containsKey(key)) {
                return ret;
            }
            num = numOfRegisteredKey(key);
        }
        // if this key is single, do remove from overlay
        if (num == 1) {
            ret = lowerRemoveKeyAsync(key);
            ret = ret.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.warn("removeKeyAsync: {}, {}", key, ex);
                }
                if (result) {
                    synchronized (keyRegister) {
                        unregisterKey(upper, key);
                    }
                }
            });
        } else {
            synchronized (keyRegister) {
                unregisterKey(upper, key);
            }
        }
        return ret;
    }

    public CompletableFuture<Boolean> removeKeyAsync(K key) {
        return removeKeyAsync(getDefaultAppId(), key);
    }

//    public boolean removeKey(ObjectId upper, String key) throws IOException {
//        return removeKey(upper, (Key) new WrappedComparableKey<String>(key));
//    }
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public boolean removeKey(ObjectId upper, Number key) throws IOException {
//        return removeKey(upper, (Key) new WrappedComparableKey((Comparable<?>) key));
//    }

    public Set<K> getKeys(ObjectId upper) {
//        synchronized (keyRegister) {
            Map<K, Integer> keyCounts = keysByUpper.get(upper);
            if (keyCounts == null) {
//                return Collections.emptySet();
                return new HashSet<K>();
            }
            return new HashSet<K>(keyCounts.keySet());
//        }
    }
    
    public Set<K> getKeys() {
        synchronized (keyRegister) {
            return new HashSet<K>(keyRegister.keySet());
        }
    }
    
    public boolean join() throws ProtocolUnsupportedException, IOException {
        return join(lowerTrans.getEndpoint().newSameTypeEndpoint(Overlay.DEFAULT_SEED.value()));
    }
    
    public boolean join(String spec) throws ProtocolUnsupportedException, IOException {
        return join(lowerTrans.getEndpoint().newSameTypeEndpoint(spec));
    }

    public boolean join(Endpoint seed) throws IOException {
        return join(Collections.singleton(seed));
    }
    
    public abstract boolean join(Collection<? extends Endpoint> seeds) throws IOException;

    public abstract boolean leave() throws IOException;

    public boolean isJoined() {
        return isJoined;
    }

    @Override
    public String toString0() {
        return super.toString0()
                + ", keys=" + keysByUpper;
    }
}
