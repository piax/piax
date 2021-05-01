/*
 * UdpPrimaryKey.java - A PrimaryKey implementaion for UDP
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.udp;

import java.util.List;

import org.piax.common.ComparableKey;
import org.piax.common.Option.EnumOption;
import org.piax.common.Option.IntegerOption;
import org.piax.gtrans.PeerLocator;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyLocator.TYPE;
import org.piax.gtrans.netty.idtrans.PrimaryKey;

public class UdpPrimaryKey extends PrimaryKey {
    static public IntegerOption DEFAULT_UDP_PORT = new IntegerOption(12367, "-default-udp-port");
    static public enum SIGTYPE {
        DIRECT, RELAY//, RELAY_PUNCHING
    };
    SIGTYPE sigType;
    NettyLocator[] candidates;
    transient UdpLocatorManager mgr;
    
    static public EnumOption<SIGTYPE> DEFAULT_SIG_TYPE = new EnumOption<SIGTYPE>(SIGTYPE.class, SIGTYPE.DIRECT, "-default-signaling-type");
    
    static public SIGTYPE parseSigType(String spec) {
        if (spec.equals("udp")) {
            return SIGTYPE.DIRECT;
        }
        else if (spec.equals("udpr")) {
            return SIGTYPE.RELAY;
        }
        return DEFAULT_SIG_TYPE.value();
    }

    static public PrimaryKey parse(String spec) {
        List<String> specs = NettyEndpoint.parse(spec);
        if (specs.size() == 1) {
            return new UdpPrimaryKey(null, DEFAULT_UDP_PORT.value());
        }
        // udp:key
        else if (specs.size() == 2) {
            return new UdpPrimaryKey(NettyEndpoint.parseKey(specs.get(1)), DEFAULT_UDP_PORT.value());
        }
        // udp:key:12367
        else if (specs.size() == 3) {//
            return new UdpPrimaryKey(NettyEndpoint.parseKey(specs.get(1)), Integer.parseInt(specs.get(2)));
        }
        // udp:*:localhost:12367
        else if (specs.size() == 4) {
            return new UdpPrimaryKey(NettyEndpoint.parseKey(specs.get(1)), 
                    new NettyLocator(TYPE.UDP, specs.get(2), Integer.parseInt(specs.get(3))));
        }
        return null; 
    }
    
    public void setLocatorManager(UdpLocatorManager mgr) {
        this.mgr = mgr;
    }
    
    public UdpPrimaryKey(ComparableKey<?> key) { // key only
        this(DEFAULT_SIG_TYPE.value(), key, null);
    }

    public UdpPrimaryKey(NettyLocator locator) { // locator only
        this(DEFAULT_SIG_TYPE.value(), null, locator);
    }

    public UdpPrimaryKey(ComparableKey<?> key, NettyLocator locator) { // represents myself
        this(DEFAULT_SIG_TYPE.value(), key, locator);
    }
    
    public UdpPrimaryKey(ComparableKey<?> key, int port) { // represents myself
        this(DEFAULT_SIG_TYPE.value(), key, port);
    }
        
    public UdpPrimaryKey(SIGTYPE sigType, ComparableKey<?> key, NettyLocator locator) { // represents myself
        super(key, locator);
        this.sigType = sigType;
    }

    public UdpPrimaryKey(SIGTYPE sigType, ComparableKey<?> key, int port) { // represents myself
        super(key);
        locator = new NettyLocator(TYPE.UDP, port); // address is wildcard in UdpPrimaryKey.
        this.sigType = sigType;
    }

    public UdpPrimaryKey(SIGTYPE sigType, ComparableKey<?> key, NettyLocator locator, NettyLocator[] entries) {
        super(key);
        this.sigType = sigType;
        this.locator = locator; // primary candidate;
        this.candidates = entries;
    }
    
    public NettyLocator getLocator() {
        if (mgr == null) { // specified locator(first time)
            return locator;
        }
        return mgr.getPrimaryLocator(rawKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if ((rawKey == null) && o instanceof PeerLocator) { // WILDCARD
            return locator.equals(o);
        }
        if (getClass() != o.getClass())
            return false;
        if ((rawKey == null) || (((PrimaryKey)o).getRawKey() == null)) { // WILDCARD
            return ((PrimaryKey)o).getLocator().equals(locator);
        }
        return rawKey.equals(((PrimaryKey) o).getRawKey());
    }
}
