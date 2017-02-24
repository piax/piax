package org.piax.gtrans.async;

import java.io.IOException;

import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.util.UniqId;

public abstract class NodeFactory {
    public abstract LocalNode createNode(TransportId transId,
            ChannelTransport<?> trans, DdllKey key)
                    throws IOException, IdConflictException;

    public LocalNode createNode(TransportId transId,
            ChannelTransport<?> trans, Comparable<?> rawkey)
                    throws IOException, IdConflictException {
        DdllKey ddllkey = new DdllKey(rawkey, new UniqId(trans.getPeerId()));
        return createNode(transId, trans, ddllkey);
    }

    public abstract String name();
}
