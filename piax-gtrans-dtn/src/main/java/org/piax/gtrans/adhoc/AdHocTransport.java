/*
 * AdHocTransport.java - An ad-hoc transport.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: AdHocTransport.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.adhoc;

import java.io.IOException;
import java.util.List;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Discoverable;
import org.piax.gtrans.DiscoveryListener;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.PeerInfo;
import org.piax.gtrans.base.BaseChannelTransportImpl;
import org.piax.gtrans.impl.OneToOneMappingTransport;
import org.piax.gtrans.raw.bluetooth.BluetoothDiscoverer;
import org.piax.gtrans.raw.bluetooth.BluetoothLocator;
import org.piax.gtrans.tsd.TSDDiscoverable;

/**
 * An ad-hoc transport.
 */
public class AdHocTransport<E extends Endpoint> extends
        OneToOneMappingTransport<E> implements Discoverable<E> {

    final Discoverable<E> rawDiscoverable;
    final boolean isBluetooth;
    
    public AdHocTransport(TransportId transId, ChannelTransport<E> lowerTrans,
            TSDDiscoverable.Type type) throws IdConflictException, IOException {
        super(transId, lowerTrans);
        rawDiscoverable = new TSDDiscoverable<E>(lowerTrans.getPeerId(), type,
                transId);
        PeerInfo<E> info = new PeerInfo<E>(lowerTrans.getPeerId(),
                lowerTrans.getEndpoint());
        rawDiscoverable.register(info);
        isBluetooth = false;
    }

    /**
     * Bluetoothを用いたAdHocTransportを生成する。
     * 
     * @param me
     * @param transId
     * @throws IdConflictException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public AdHocTransport(Peer peer, TransportId transId)
            throws IdConflictException, IOException {
        super(transId, (ChannelTransport<E>) 
                new BaseChannelTransportImpl<BluetoothLocator>(peer, new TransportId(
                transId.toString() + "$bt"), BluetoothLocator.getLocal()));
        rawDiscoverable = (Discoverable<E>) BluetoothDiscoverer.getInstance();
        isBluetooth = true;
    }

    @Override
    public synchronized void fin() {
        super.fin();
        lowerTrans.setListener(transId, null);
        if (isBluetooth)
            lowerTrans.fin();
        rawDiscoverable.fin();
    }
    
    public void setExpireTime(long period) {
        this.checkActive();
        rawDiscoverable.setExpireTime(period);
    }

    @Override
    public boolean addDiscoveryListener(DiscoveryListener<E> listener) {
        this.checkActive();
        return rawDiscoverable.addDiscoveryListener(listener);
    }

    @Override
    public boolean removeDiscoveryListener(DiscoveryListener<E> listener) {
        this.checkActive();
        return rawDiscoverable.removeDiscoveryListener(listener);
    }

    @Override
    public List<PeerInfo<E>> getAvailablePeerInfos() {
        return rawDiscoverable.getAvailablePeerInfos();
    }

    @Override
    public void scheduleDiscovery(long delay, long period) {
        this.checkActive();
        rawDiscoverable.scheduleDiscovery(delay, period);
    }

    @Override
    public void cancelDiscovery() {
        this.checkActive();
        rawDiscoverable.cancelDiscovery();
    }

    public void register(PeerInfo<E> info) {
        throw new UnsupportedOperationException();
    }

    public void unregister(PeerInfo<E> info) {
        throw new UnsupportedOperationException();
    }
}
