package org.piax.gtrans.netty;

import org.piax.common.Endpoint;

public interface NettyEndpoint extends Endpoint {
    public int getPort();
    public String getHost();
    public String getKeyString();
    static public NettyEndpoint createEndpoint(String host, int port) {
        return null; // default is null;
    };
}
