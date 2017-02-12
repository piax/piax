package org.piax.gtrans.netty;

import java.io.Serializable;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;

public class NettyMessage implements Serializable {
    private static final long serialVersionUID = -5499407890598183414L;
    private NettyLocator srcLocator;
    private final NettyLocator dstLocator;
    private final NettyLocator channelInitiator;
    private final ObjectId sender;
    private final PeerId peerId;
    private final Object msg;
    private final boolean isChannelSend; // true if it is channel
    private final int channelNo; // the channel number
    private int hops;

    public NettyMessage(ObjectId upper, NettyLocator src, NettyLocator dst, NettyLocator channelInitiator,
            PeerId peerId, Object msg, 
            boolean isChannelSend, int channelNo) {
        this.sender = upper;
        this.srcLocator = src;
        this.dstLocator = dst;
        this.channelInitiator = channelInitiator;
        this.peerId = peerId;
        this.msg = msg;
        this.isChannelSend = isChannelSend;
        this.channelNo = channelNo;
        hops = 0;
    }

    public ObjectId getObjectId() {
        return sender;
    }

    public NettyLocator getSourceLocator() {
        return srcLocator;
    }
    
    public void setSourceLocator(NettyLocator locator) {
        this.srcLocator = locator;
    }
    
    public NettyLocator getDestinationLocator() {
        return dstLocator;
    }

    public NettyLocator getChannelInitiator() {
        return channelInitiator;
    }

    public PeerId getPeerId() {
        return peerId;
    }

    public Object getMsg() {
        return msg;
    }

    public boolean isChannelSend() {
        return isChannelSend;
    }

    public int channelNo() {
        return channelNo;
    }
    
    public void incrementHops() {
        hops++;
    }
    
    public int getHops() {
        return hops;
    }

    public String toString() {
        return "locator=" + srcLocator + ",objectId=" + sender + "msg=" + msg;
    }
}
