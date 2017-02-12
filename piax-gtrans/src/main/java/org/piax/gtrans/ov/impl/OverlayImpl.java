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

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.dcl.DCLTranslator;
import org.piax.common.dcl.parser.ParseException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.RequestTransportImpl;
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
    protected final Map<K, Integer> keyRegister = new HashMap<K, Integer>();
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
        return request(transId, transId, dstExp, msg, timeout);
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

    protected FutureQueue<?> selectOnReceive(OverlayListener<D, K> listener,
            Overlay<D, K> trans, OverlayReceivedMessage<K> rmsg) {
        logger.trace("ENTRY:");
        Object msg = rmsg.getMessage();
        logger.debug("msg {}", msg);
        Object inn = checkAndClearIsEasySend(msg);
        if (NON != inn) {
            rmsg.setMessage(inn);
            logger.debug("select onReceive: trans:{}", trans.getTransportId());
            listener.onReceive(trans, rmsg);
            return FutureQueue.emptyQueue();
        } else {
            return listener.onReceiveRequest(trans, rmsg);
        }
    }

    private int numOfRegisteredKey(K key) {
        Integer count = keyRegister.get(key);
        if (count == null) {
            return 0;
        } else {
            return count;
        }
    }

    protected void registerKey(K key) {
        Integer count = keyRegister.get(key);
        if (count == null) {
            keyRegister.put(key, 1);
        } else {
            keyRegister.put(key, count + 1);
        }
    }
    
    private void registerKey(ObjectId upper, K key) {
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
        Integer count = keyRegister.get(key);
        if (count == null) return false;
        if (count == 1) {
            keyRegister.remove(key);
        } else {
            keyRegister.put(key, count - 1);
        }
        return true;
    }
    
    private boolean unregisterKey(ObjectId upper, K key) {
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
            // if this key not exists, do add to overlay
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

//    public boolean removeKey(ObjectId upper, String key) throws IOException {
//        return removeKey(upper, (Key) new WrappedComparableKey<String>(key));
//    }
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public boolean removeKey(ObjectId upper, Number key) throws IOException {
//        return removeKey(upper, (Key) new WrappedComparableKey((Comparable<?>) key));
//    }

    public Set<K> getKeys(ObjectId upper) {
        synchronized (keyRegister) {
            Map<K, Integer> keyCounts = keysByUpper.get(upper);
            if (keyCounts == null) {
//                return Collections.emptySet();
                return new HashSet<K>();
            }
            return new HashSet<K>(keyCounts.keySet());
        }
    }
    
    public Set<K> getKeys() {
        synchronized (keyRegister) {
            return new HashSet<K>(keyRegister.keySet());
        }
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
