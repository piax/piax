/*
 * SimpleDTN.java - A simple DTN implementation
 * 
 * Copyright (c) 2015 PIAX development team
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
 *
 * $Id: SimpleDTN.java 1194 2015-06-08 03:29:48Z teranisi $
 */

package org.piax.gtrans.dtn;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.PeerInfo;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.adhoc.AdHocTransport;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 単純なDTNを実現するOverlayクラス。
 * 他のOverlayと違い、sendとonReceiveを使った一方向の通信を行う。
 * 実際の通信はDTNNodeがRPCを使って行うため、このクラスはテンプレートとしての機能しか持たない。
 */
public class SimpleDTN<D extends Destination, K extends Key> extends
        OverlayImpl<D, K> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(SimpleDTN.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("dtn");

    public final DTNNode<D, K> node;

    public SimpleDTN(AdHocTransport<?> trans) throws IdConflictException, IOException {
        this(DEFAULT_TRANSPORT_ID, trans);
    }

    public SimpleDTN(TransportId transId, AdHocTransport<?> trans)
            throws IdConflictException, IOException {
        super(trans.getPeer(), transId, trans);
        Peer.getInstance(peerId).registerBaseOverlay(transIdPath);
        node = new DTNNode<D, K>(this, new TransportId(transId + "$n"), trans);
    }

    @Override
    public synchronized void fin() {
        try {
            leave();
        } catch (IOException ignore) {
        }
        node.fin();
        super.fin();
    }

    public Endpoint getEndpoint() {
        return lowerTrans.getEndpoint();
    }
    
    @Override
    public AdHocTransport<?> getLowerTransport() {
        return (AdHocTransport<?>) lowerTrans;
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver, Destination dst,
            Object msg) throws ProtocolUnsupportedException, IOException {
        if (!(dst instanceof GeoRegion || dst instanceof KeyRanges
                || dst instanceof KeyRange || dst instanceof Key)) {
            throw new ProtocolUnsupportedException(
                    "DTN only supports region, key, range or ranges destination");
        }
        NestedMessage nmsg = new NestedMessage(sender, receiver, null,
                getEndpoint(), msg);
        node.firstSend(dst, nmsg);
    }

    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            Destination dst, Object msg, int timeout)
            throws ProtocolUnsupportedException, IOException {
        throw new UnsupportedOperationException();
    }

    public void onReceive(Collection<K> matchedKeys, NestedMessage nmsg) {
        OverlayListener<D, K> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("this onReceive method purged data as no such listener");
            return;
        }
        OverlayReceivedMessage<K> rcvMsg = new OverlayReceivedMessage<K>(
                nmsg.sender, nmsg.src, matchedKeys, nmsg.getInner());
        ovl.onReceive(this, rcvMsg);
    }
    
    @Override
    public synchronized boolean join(Collection<? extends Endpoint> seeds)
            throws IOException {
        if (isJoined()) {
            return false;
        }
        isJoined = true;
        return true;
    }

    @Override
    public boolean leave() throws IOException {
        if (!isJoined()) {
            return false;
        }
        isJoined = false;
        return true;
    }

    @SuppressWarnings("unchecked")
    public List<PeerInfo<?>> getAvailablePeerInfos() {
        List<?> infos = getLowerTransport().getAvailablePeerInfos();
        logger.debug("availNbrs {}", infos);
        return (List<PeerInfo<?>>) infos;
    }

    public String showTable() {
        StringBuilder sb = new StringBuilder();
        for (PeerInfo<?> info : getAvailablePeerInfos()) {
            sb.append("   " + info + "\n");
        }
        return sb.toString();
    }

    public String dumpStoredMsgs() {
        return node.msgMgr.toString();
    }

	@Override
	public FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
			Object msg, TransOptions opts) throws ProtocolUnsupportedException,
			IOException {
		return request(sender, receiver, dst, msg, (int)TransOptions.timeout(opts));
	}
}
