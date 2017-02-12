package org.piax.gtrans.netty;

import java.io.Serializable;

import org.piax.gtrans.netty.NettyChannelTransport.AttemptType;

class AttemptMessage implements Serializable {
    private static final long serialVersionUID = 4729231253864270776L;
    final AttemptType type;
    final NettyLocator source;
    final Object arg;
    public AttemptMessage(AttemptType type, NettyLocator source, Object arg) {
        this.type = type;
        this.source = source;
        this.arg = arg;
    }

    public AttemptType getType() {
        return type;
    }

    public Object getArg() {
        return arg;
    }

    public NettyLocator getSource() {
        return source;
    }
}
