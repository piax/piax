package org.piax.gtrans.async;

import java.io.IOException;

import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ov.ddll.DdllKey;

public abstract class NodeFactory {
    public abstract LocalNode createNode(ChannelTransport<?> trans, DdllKey key,
            int latency) throws IOException, IdConflictException;

    public abstract String name();
}
