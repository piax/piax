/*
 * LWTcpTransportGenerator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.piax.gtrans.raw.lwtcp;

import java.io.IOException;

import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.base.BaseChannelTransportImpl;
import org.piax.gtrans.impl.BaseTransportGenerator;
import org.piax.gtrans.impl.TransportImpl;
import org.piax.gtrans.raw.RawTransport;
import org.piax.gtrans.raw.tcp.TcpLocator;

/**
 * 
 */
public class LWTcpTransportGenerator extends BaseTransportGenerator {

    public LWTcpTransportGenerator(Peer peer) {
        super(peer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends PeerLocator> ChannelTransport<E> _newBaseChannelTransport(
            String desc, TransportId transId, E loc)
            throws IdConflictException, IOException {
        if (!(loc instanceof TcpLocator))
            return null;
        if (transId == null)
            transId = new TransportId("lwtcp");
        boolean linger0Option = false;
        if (desc != null && desc.equals("LINGER0")) {
            linger0Option = true;
        }
        ChannelTransport<E> trans = new BaseChannelTransportImpl<E>(peer,
                transId, (RawTransport<E>) new LWTcpTransport(peer.getPeerId(),
                        (TcpLocator) loc, linger0Option));
        ((TransportImpl<?>) trans).setBaseTransport();
        return trans;
    }

    @Override
    public <E extends PeerLocator> Transport<E> _newBaseTransport(String desc,
            TransportId transId, E loc) throws IdConflictException, IOException {
        return null;
    }
}
