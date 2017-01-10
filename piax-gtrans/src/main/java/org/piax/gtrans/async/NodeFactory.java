package org.piax.gtrans.async;

import java.io.IOException;

import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.util.UniqId;

public abstract class NodeFactory {
    public abstract LocalNode createNode(TransportId transId,
            ChannelTransport<?> trans, DdllKey key, int latency)
                    throws IOException, IdConflictException;

    public LocalNode createNode(TransportId transId,
            ChannelTransport<?> trans, Comparable<?> rawkey, int latency)
                    throws IOException, IdConflictException {
        DdllKey ddllkey = new DdllKey(rawkey, new UniqId(trans.getPeerId()));
        return createNode(transId, trans, ddllkey, latency);
    }


    public abstract String name();
}
