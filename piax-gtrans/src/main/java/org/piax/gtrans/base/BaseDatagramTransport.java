/*
 * BaseDatagramTransport.java - A datagram transport.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: BaseDatagramTransport.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.base;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.impl.DatagramBasedTransport;
import org.piax.gtrans.impl.ExceededSizeException;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.raw.RawTransport;
import org.piax.util.BinaryJsonabilityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A datagram transport.
 */
public class BaseDatagramTransport<E extends PeerLocator> extends
        DatagramBasedTransport<E, E> {
    /*--- logger ---*/
    private static final Logger logger = 
            LoggerFactory.getLogger(BaseDatagramTransport.class);

    public static final int MAX_HEADER_SIZE = 100;
    final E locator;

    public BaseDatagramTransport(Peer peer, TransportId transId,
            E locator) throws IdConflictException, IOException {
        super(peer, transId, locator.newRawTransport(peer.getPeerId()), true);
        this.locator = locator;
        getLowerTransport().setListener(this);
    }

    @Override
    public synchronized void fin() {
        super.fin();
        lowerTrans.fin();
    }

    @Override
    public E getEndpoint() {
        return locator;
    }

    @Override
    public int getMTU() {
        return lowerTrans.getMTU() - MAX_HEADER_SIZE;
    }

    @Override
    public RawTransport<E> getLowerTransport() {
        @SuppressWarnings("unchecked")
        RawTransport<E> lower = (RawTransport<E>) lowerTrans;
        return lower;
    }
    
    @Override
    protected boolean useReceiverThread(int numProc) {
        return true;
    }

    @Override
    protected void lowerSend(E dst, NestedMessage nmsg)
            throws IOException {
        if (GTransConfigValues.ALLOW_REF_SEND_IN_BASE_TRANSPORT
                && getLowerTransport().canSendNormalObject()) {
            getLowerTransport().send(dst, nmsg);
        } else {
            try {
                getLowerTransport().send(dst, nmsg.serialize());
            } catch (BinaryJsonabilityException e) {
                logger.error("", e);
            } catch (ExceededSizeException e) {
                throw new IOException(e);
            }
        }
    }
    
    @Override
    protected NestedMessage _preReceive(ReceivedMessage rmsg) {
        if (GTransConfigValues.ALLOW_REF_SEND_IN_BASE_TRANSPORT
                && getLowerTransport().canSendNormalObject()) {
            return (NestedMessage) rmsg.getMessage();
        } else {
        try {
            ByteBuffer bb = (ByteBuffer) rmsg.getMessage();
            NestedMessage nmsg = NestedMessage.deserialize(bb);
            return nmsg;
        } catch (BinaryJsonabilityException e) {
            logger.error("", e);
            return null;
        }
    }}
}
