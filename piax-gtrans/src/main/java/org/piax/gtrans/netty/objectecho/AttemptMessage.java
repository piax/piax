package org.piax.gtrans.netty.objectecho;

import java.io.Serializable;

public class AttemptMessage implements Serializable {
    private static final long serialVersionUID = 4729231253864270776L;
    final int type;
    final int source;
    public AttemptMessage(int type, int source) {
        this.type = type;
        this.source = source;
    }
    public int getType() {
        return type;
    }
    public int getSource() {
        return source;
    }
    public String toString() {
        return type + ":" + source;
    }
}