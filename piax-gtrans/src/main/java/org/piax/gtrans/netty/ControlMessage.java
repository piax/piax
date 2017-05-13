package org.piax.gtrans.netty;

import java.io.Serializable;

public class ControlMessage<E extends NettyEndpoint> implements Serializable {
    private static final long serialVersionUID = 4729231253864270776L;
    public final ControlType type;
    final E source;
    final Object arg;
    
    public enum ControlType {
        ATTEMPT, ACK, NACK, // Locator
        UPDATE, INIT, WAIT,
        CLOSE // for upper layer
    }
    public ControlMessage(ControlType type, E source, Object arg) {
        this.type = type;
        this.source = source;
        this.arg = arg;
    }

    public ControlType getType() {
        return type;
    }

    public Object getArg() {
        return arg;
    }

    public E getSource() {
        return source;
    }
}
