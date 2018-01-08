/*
 * DefaultAgentTransportManager.java - A default implementation of transport manager.
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
 * $Id: AgentHomeImpl.java 1064 2014-07-02 05:31:54Z ishi $
 */

package org.piax.agent;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Key;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.dolr.DOLR;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.szk.Suzaku;
import org.piax.gtrans.util.ChannelAddOnTransport;

/**
 * デフォルトのAgentTransportmanagerである。
 * トランスポートの作成は、getRPCTransportが呼ばれるまで行わない。
 */
public class DefaultAgentTransportManager extends AgentTransportManager {
    private ChannelTransport<PeerLocator> baseTransport = null;
    private PeerLocator peerLocator;
    private PeerLocator seedLocator;
    private PeerId peerId;
    private String name;
    
    /**
     * コンストラクタ
     * 
     * ベース・トランスポートは、Peer#newBaseChannelTransportで作成するので
     * ロケータは、このメソッドに渡せるものでなければいけない。
     * 2つのロケータpeerLocator, seedLocatorは同じ型でなければいけない。
     * 
     * @param peerName ピア名
     * @param peerLocator 自身のピアのロケータ
     * @param seedLocator シードのロケータ
     */
    public DefaultAgentTransportManager(String peerName,PeerLocator peerLocator,
            PeerLocator seedLocator) {
    		this.name = peerName;
        this.peerId = new PeerId(peerName);
        this.peerLocator = peerLocator;
        this.seedLocator = seedLocator;
    }
    
    /**
     * Return peerId 
     */
    public PeerId getPeerId() {
        return peerId;
    }
    
    /**
     * Return peerLocator
     */
    public PeerLocator getPeerLocator() {
        return peerLocator;
    }
    
    /**
     * Return peer name
     */
    public String getName() {
    		return name;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setup(AgentHome home) throws Exception {
        defineTransport("BASE", () -> {
            return Peer.getInstance(peerId).newBaseChannelTransport(peerLocator);
        });

        // If not defined, set range query overlay as Multi-key Skip Graph.
        defineOverlay("RQ", () -> {
            return getOverlay("MSG");
        }, seedLocator);

        defineTransport("RPC", () -> {
            return new ChannelAddOnTransport<PeerId>((Transport<? super PeerId>) getOverlay("RQ"));
        });

        // LLNET
        defineOverlay("LLNET", () -> {
            return new LLNet((Overlay<Destination,ComparableKey<?>>)getOverlay("RQ"));
        }, seedLocator);

        // DOLR
        defineOverlay("DOLR", () -> {
            return new DOLR<Key>((Overlay<Destination,ComparableKey<?>>)getOverlay("RQ"));
        }, seedLocator);

        // Suzaku factories
        defineOverlay("SZK", () -> {
            return new Suzaku<Destination,ComparableKey<?>>((ChannelTransport<?>)getTransport("BASE"));
        }, seedLocator);

        defineOverlay("MSG", () -> {
            return new MSkipGraph<Destination,ComparableKey<?>>((ChannelTransport<?>)getTransport("BASE"));
        }, seedLocator);
    }
}
