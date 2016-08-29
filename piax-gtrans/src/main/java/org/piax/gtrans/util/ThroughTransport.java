/*
 * ThroughTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ThroughTransport.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.util;

import java.io.IOException;

import org.piax.common.ObjectId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.impl.OneToOneMappingTransport;

/**
 * OneToOneMappingTransportを使って作成した sendとonReceiveをフックするだけの Transportクラス
 */
public class ThroughTransport<E extends PeerLocator> extends
        OneToOneMappingTransport<E> {

	public ThroughTransport(ChannelTransport<E> trans)
            throws IdConflictException {
        super(new TransportId("through"), trans);
    }
	
    public ThroughTransport(TransportId transId, ChannelTransport<E> trans)
            throws IdConflictException {
        super(transId, trans);
    }

    @Override
    public void fin() {
        super.fin();
        lowerTrans.fin();
    }

    @Override
    protected Object _preSend(ObjectId sender, ObjectId receiver,
            E dst, Object msg) throws IOException {
        System.out.print("*");
        return msg;
    }

    @Override
    protected Object _postReceive(ObjectId sender, ObjectId receiver,
            E src, Object msg) {
        System.out.print("+");
        return msg;
    }
}
