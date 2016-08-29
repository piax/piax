/*
 * ChannelAddOnTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ChannelAddOnTransport.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.util;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.DatagramBasedTransport;
import org.piax.gtrans.impl.NestedMessage;

/**
 */
public class ChannelAddOnTransport<E extends Endpoint> extends
        DatagramBasedTransport<E, E> {

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("chOn");

    public ChannelAddOnTransport(
            Transport<? super E> lowerTrans) throws IdConflictException {
        this(DEFAULT_TRANSPORT_ID, lowerTrans);
    }

    public ChannelAddOnTransport(TransportId transId,
            Transport<? super E> lowerTrans) throws IdConflictException {
        super(lowerTrans.getPeer(), transId, lowerTrans, true);
        getLowerTransport().setListener(transId, this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public E getEndpoint() {
        return (E) lowerTrans.getEndpoint();
    }

    @Override
    public int getMTU() {
        return lowerTrans.getMTU();
    }

    /*
     * 下位層にセットするlowerTransは、Transport<E> を前提にしていることに注意
     */
    @Override
    public Transport<E> getLowerTransport() {
        @SuppressWarnings("unchecked")
        Transport<E> lower = (Transport<E>) lowerTrans;
        return lower;
    }

    @Override
    protected void lowerSend(E dst, NestedMessage nmsg)
            throws ProtocolUnsupportedException, IOException {
        getLowerTransport().send(transId, dst, nmsg);
    }

    @Override
    protected NestedMessage _preReceive(ReceivedMessage rmsg) {
        return (NestedMessage) rmsg.getMessage();
    }

    @Override
    protected boolean useReceiverThread(int numProc) {
        return false;
    }
}
