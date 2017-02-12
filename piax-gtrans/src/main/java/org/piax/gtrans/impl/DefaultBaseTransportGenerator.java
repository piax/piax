/*
 * DefaultBaseTransportGenerator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: DefaultBaseTransportGenerator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.impl;

import java.io.IOException;

import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.base.BaseChannelTransportImpl;
import org.piax.gtrans.base.BaseDatagramTransport;
import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.lwtcp.LWTcpTransport;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.gtrans.util.FragmentationTransport;

/**
 * 
 */
public class DefaultBaseTransportGenerator extends BaseTransportGenerator {

    DefaultBaseTransportGenerator(Peer peer) {
        super(peer);
    }
    
    private TransportId newDefaultId(PeerLocator loc) {
        String type;
        if (loc instanceof EmuLocator) {
            type = "emu"; 
        } else if (loc instanceof UdpLocator) {
            type = "udp"; 
        } else if (loc instanceof TcpLocator) {
            type = "tcp";
        } else if (loc instanceof NettyLocator) {
            type = "netty";
        } else {
            return null;
        }
        /*
         * TODO think!
         * base transportについてもユニークでなければ、transportIdPathが違うpeerで異なってしまう。
         * 同じpeerで違うportのbase transportを作る場合は、transIdを明示的に変えなければいけない。
         * 問題は、同じUDPなら相互に通信できてしまうところ
         */
        return new TransportId(type);
    }

    @Override
    public <E extends PeerLocator> ChannelTransport<E> _newBaseChannelTransport(
            String desc, TransportId transId, E loc)
            throws IdConflictException, IOException {
        if (transId == null) {
            transId = newDefaultId(loc);
        }
        if (transId == null) {
            return null;
        }
        ChannelTransport<E> trans = null;
        if (loc instanceof EmuLocator || loc instanceof UdpLocator) {
            trans = new BaseDatagramTransport<E>(peer, transId, loc);
        } else if (loc instanceof TcpLocator){
            boolean linger0Option = false;
            if (desc != null && desc.equals("LINGER0")) {
                linger0Option = true;
            }
            trans = new BaseChannelTransportImpl<E>(peer, transId,
                    new LWTcpTransport(peer.getPeerId(), (TcpLocator) loc,
                            linger0Option));
        } else if (loc instanceof NettyLocator){
            trans = (ChannelTransport<E>)new NettyChannelTransport(peer, transId, peer.getPeerId(), (NettyLocator)loc);
        } else {
            return null;
        }
        if (!Peer.RAW.equals(desc)) {
            if (loc instanceof UdpLocator) {
                trans = new FragmentationTransport<E>(TransportId.NULL_ID, trans);
            }
        }
        ((TransportImpl<?>) trans).setBaseTransport();
        return trans;
    }

    @Override
    public <E extends PeerLocator> Transport<E> _newBaseTransport(
            String desc, TransportId transId, E loc)
            throws IdConflictException, IOException {
        return newBaseChannelTransport(desc, transId, loc);
    }
}
