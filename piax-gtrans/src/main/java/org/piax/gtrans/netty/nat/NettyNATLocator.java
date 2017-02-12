package org.piax.gtrans.netty.nat;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.piax.common.Id;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyLocator;

public class NettyNATLocator extends NettyLocator {
    private static final long serialVersionUID = -5284816579679286212L;
    public static final int NAT_NODE_ID_LENGTH = 24; 
    Id id;
    List<NettyLocator> rawChannelLocators;

    private void init() {
        id = Id.newId(NAT_NODE_ID_LENGTH); 
        rawChannelLocators = new ArrayList<NettyLocator>();
    }
    
    public NettyNATLocator(InetSocketAddress addr) {
        super(addr);
        init();
    }

    public NettyNATLocator(String host, int port) {
        super(host, port);
        init();
    }

    public NettyNATLocator(String spec) throws ProtocolUnsupportedException {
        super(spec);
        init();
    }

    public void updateRawChannelLocators(NettyChannelTransport trans) {
        rawChannelLocators = trans.getRawChannelLocators();
    }

    public void updateRawChannelLocators(NettyNATLocator locator) {
        rawChannelLocators = locator.getRawChannelLocators();
    }

    public List<NettyLocator> getRawChannelLocators() {
        return rawChannelLocators;
    }
    
    public TYPE getType() {
        return TYPE.TCP;
    }

    public String getKeyString() {
        return "nat:" + id.toHexString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NettyNATLocator) {
            NettyNATLocator l = (NettyNATLocator)o;
            return id.equals(l.id);
        }
        return false;
    }

    @Override
    public String toString() {
        String ret = getKeyString() + ":";
        for (NettyLocator loc : rawChannelLocators) {
            ret += loc + ";";
        }
        return ret;
    }
}
