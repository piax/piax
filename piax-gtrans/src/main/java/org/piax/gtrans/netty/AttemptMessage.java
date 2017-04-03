package org.piax.gtrans.netty;

import java.io.Serializable;

import org.piax.common.Endpoint;
import org.piax.gtrans.netty.NettyChannelTransport.AttemptType;

class AttemptMessage<E extends NettyEndpoint> implements Serializable {
    private static final long serialVersionUID = 4729231253864270776L;
    final AttemptType type;
    final E source;
    final Object arg;
    public AttemptMessage(AttemptType type, E source, Object arg) {
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

    public E getSource() {
        return source;
    }
}
