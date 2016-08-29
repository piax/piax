/*
 * TransportImpl.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: TransportImpl.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.Destination;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.raw.RawTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transportオブジェクトを実装するための部品として使用するabstractクラス
 * 
 * 
 */
public abstract class TransportImpl<D extends Destination> implements Transport<D> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(TransportImpl.class);

    protected final Peer peer;
    protected final PeerId peerId;
    protected final TransportId transId;
    protected final Transport<?> lowerTrans;
    protected final List<TransportImpl<?>> uppers = new ArrayList<TransportImpl<?>>();
    protected final TransportIdPath transIdPath;
    
    protected ObjectId defaultAppId = null;
    
    public void setDefaultAppId(ObjectId appId) {
		defaultAppId = appId;
    }
    
    public ObjectId getDefaultAppId() {
		return defaultAppId == null ? DEFAULT_APP_ID : defaultAppId; 
    }
    
    /**
     * base transportかどうかを識別するフラグ。
     * Peer.newBase(Channel)Transport() メソッドの中でセットされる。 
     */
    protected volatile boolean isBaseTransport = false;

    protected final Map<ObjectId, TransportListener<D>> listenersByUpper =
            new ConcurrentHashMap<ObjectId, TransportListener<D>>();

    /**
     * Transportオブジェクトがアクティブな状態であることを示す。
     * fin() が呼ばれた場合はfalseとなる。
     */
    protected volatile boolean isActive = true;

    /**
     * 指定されたtransport IDを持つTransportオブジェクトを生成する。
     * 
     * @param peerId このTransportオブジェクトを保持するpeerのpeer ID
     * @param transId transport ID
     * @throws IdConflictException transport IDがすでに存在する場合
     */
    protected TransportImpl(Peer peer, TransportId transId, Transport<?> lowerTrans)
            throws IdConflictException {
        if (transId == null)
            throw new IllegalArgumentException("transId should not be null");
        this.peer = peer;
        peerId = peer.getPeerId();
        this.transId = transId;
        this.lowerTrans = lowerTrans;
        // TransportIdPathを生成する
        TransportIdPath lowerIdPath = (lowerTrans == null) ? null : 
            lowerTrans.getTransportIdPath(); 
        transIdPath = new TransportIdPath(lowerIdPath, transId);
        // lowerTransにupperとしての自分をaddする
        if (lowerTrans != null && lowerTrans instanceof TransportImpl<?>) {
            ((TransportImpl<?>) lowerTrans).addUpper(this);
        }
        logger.debug("transId:{} path:{}", transId, transIdPath);
        peer.registerTransport(transIdPath, this);
    }

    /**
     * RawTransportのように、transport IDを持たないTransportオブジェクトを生成する。
     * 
     * @param peerId このTransportオブジェクトを保持するpeerのpeer ID
     */
    protected TransportImpl(Peer peer) {
        this.peer = peer;
        peerId = peer.getPeerId();
        transId = null;
        lowerTrans = null;
        transIdPath = null;
    }

    public void fin() {
        synchronized (this) {
            isActive = false;
            listenersByUpper.clear();
            if (transId != null) {
                peer.unregisterTransport(transIdPath, this);
            }
            // lowerTransからupperとしての自分をremoveする
            if (lowerTrans != null && lowerTrans instanceof TransportImpl<?>) {
                ((TransportImpl<?>) lowerTrans).removeUpper(this);
            }
        }
    }

    synchronized void addUpper(TransportImpl<?> upper) {
        uppers.add(upper);
    }

    synchronized void removeUpper(TransportImpl<?> upper) {
        uppers.remove(upper);
    }
    
    public synchronized List<TransportImpl<?>> getUppers() {
        return new ArrayList<TransportImpl<?>>(uppers);
    }
    
    /**
     * Transportオブジェクトがアクティブな状態であるかどうかをチェックする。
     * fin() が呼ばれてインアクティブな状態である場合は、IllegalStateExceptionがthrowされる。
     * サブクラスの場合も含め、fin() の後に呼び出されては困る場合のメソッド呼び出しの際のチェックに用いる。
     * 
     * @throws IllegalStateException Transportオブジェクトがインアクティブな状態である場合
     */
    protected void checkActive() throws IllegalStateException {
        if (!isActive)
            throw new IllegalStateException("this transport " + transId 
                    + " is already finalized");
    }

    public Peer getPeer() {
        return peer;
    }

    public PeerId getPeerId() {
        return peer.getPeerId();
    }

    public TransportId getTransportId() {
        return transId;
    }

    public TransportIdPath getTransportIdPath() {
        return transIdPath;
    }

    public int getMTU() {
        // デフォルト値として、MAX_MSG_SIZEを返す
        return GTransConfigValues.MAX_MSG_SIZE;
    }
    
    public Transport<?> getLowerTransport() {
        return lowerTrans;
    }

    public List<Transport<?>> getLowerTransports() {
        List<Transport<?>> trans;
        if (lowerTrans == null || lowerTrans instanceof RawTransport) {
            trans = new ArrayList<Transport<?>>();
        } else {
            trans = lowerTrans.getLowerTransports();
            if (lowerTrans.getTransportId() != null) {
                trans.add(lowerTrans);
            }
        }
        return trans;
    }

    public void setBaseTransport() {
        isBaseTransport = true;
    }

    public Transport<?> getBaseTransport() {
        if (isBaseTransport) return this;
        return (lowerTrans == null) ? null : lowerTrans.getBaseTransport();
    }

    public boolean isUp() {
        if (lowerTrans != null) {
            return lowerTrans.isUp();
        }
        return true;
    }
    
    public boolean hasStableLocator() {
        if (lowerTrans != null) {
            return lowerTrans.hasStableLocator();
        }
        return true;
    }

    public void setListener(ObjectId upper, TransportListener<D> listener) {
        logger.trace("ENTRY:");
        logger.debug("transId:{}, upper:{}", transId, upper);
        if (listener == null) {
            listenersByUpper.remove(upper);
        } else {
            checkActive();
            listenersByUpper.put(upper, listener);
        }
    }
    
    public void setListener(TransportListener<D> listener) {
    		setListener(getDefaultAppId(), listener);
    }

    public TransportListener<D> getListener(ObjectId upper) {
        return listenersByUpper.get(upper);
    }
    
    public TransportListener<D> getListener() {
        return listenersByUpper.get(getDefaultAppId());
    }
    
    public void send(ObjectId sender, ObjectId receiver, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
    		// XXX ignore opts by default.
    		send(sender, receiver, dst, msg);
    }
    
    public void send(D dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
    		send(getDefaultAppId(), dst, msg);
    }
    
    public void send(D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
    		// XXX ignore opts by default.
    		send(getDefaultAppId(), dst, msg);
    }
    
    public void send(ObjectId appId, D dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
    		send(appId, appId, dst, msg);
    }
    
    public void send(ObjectId appId, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
    		// XXX ignore opts by default.
    		send(appId, appId, dst, msg);
    }

    public void send(TransportId upperTrans, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
    		// Default behavior is ignoring the options.
        send(upperTrans, upperTrans, dst, msg);
    }
    
    public void send(TransportId upperTrans, D dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
        send(upperTrans, upperTrans, dst, msg);
    }

    public String toString0() {
        return this.getClass().getSimpleName() + "{peerId=" + getPeerId()
                + ", transIdPath=" + transIdPath 
                + ", listeners=" + listenersByUpper.keySet();
    }
    
    @Override
    public String toString() {
        return toString0() + "}";
    }
}
