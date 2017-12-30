package org.piax.gtrans.ov.suzaku;

import java.io.IOException;
import java.util.TreeSet;

import org.piax.ayame.EventSender.EventSenderSim;
import org.piax.ayame.LocalNode;
import org.piax.ayame.ov.ddll.DdllKey;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;

public class NetLocalNode extends LocalNode {
    public NetLocalNode(TransportId transId, ChannelTransport<?> trans,
            DdllKey ddllkey)
            throws IdConflictException, IOException {
        super(ddllkey, trans == null ? null : trans.getEndpoint());
        assert getInstance(ddllkey) == this; 
        assert trans == null || this.peerId.equals(trans.getPeerId());

        // to support multi-keys
        localNodeMap.computeIfAbsent(peerId, k -> new TreeSet<>())
            .add(this);

        if (trans == null) {
            this.sender = EventSenderSim.getInstance();
        } else {
            this.sender = new NetEventSender(transId, trans);
        }
    }
}
