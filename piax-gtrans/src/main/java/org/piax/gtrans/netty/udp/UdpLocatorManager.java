package org.piax.gtrans.netty.udp;

import org.piax.gtrans.netty.NettyLocator;

public interface UdpLocatorManager {
    public NettyLocator getPrimaryLocator(Comparable<?> key);
    public NettyLocator[] locatorCandidates(Comparable<?> key);
    public String dump();
}
