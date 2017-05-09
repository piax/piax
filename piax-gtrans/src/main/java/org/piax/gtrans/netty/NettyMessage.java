package org.piax.gtrans.netty;

import java.io.Serializable;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;

public class NettyMessage<E extends NettyEndpoint> implements Serializable {
    private static final long serialVersionUID = -5499407890598183414L;
    private E src;
    private final E dst;
    private final E channelInitiator;
    private final ObjectId sender;
    private final PeerId peerId;
    private final Object msg;
    private final boolean isChannelSend; // true if it is channel
    private final int channelNo; // the channel number
    private int hops;

    public NettyMessage(ObjectId upper, E src, E dst, E channelInitiator,
            PeerId peerId, Object msg, 
            boolean isChannelSend, int channelNo) {
        this.sender = upper;
        this.src = src;
        this.dst = dst;
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

    public E getSource() {
        return src;
    }
    
    public void setSource(E locator) {
        this.src = locator;
    }
    
    public E getDestination() {
        return dst;
    }

    public E getChannelInitiator() {
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
        return "[src=" + src + ",dst=" + dst + ",ischannel=" + isChannelSend + ",msg=" + msg + "]";
    }
}
