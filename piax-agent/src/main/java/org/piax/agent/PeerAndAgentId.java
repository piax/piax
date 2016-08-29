/*
 * PeerAndAgentId.java - A pair of PeerId and AgentId
 * 
 * Copyright (c) 2009-2015 PIAX development team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
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
 * $Id: PeerAndAgentId.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent;

import java.io.Serializable;

import org.piax.common.PeerId;

/**
 * A pair of PeerId and AgentId
 */
public class PeerAndAgentId implements Serializable {
    private static final long serialVersionUID = 1L;

    public final PeerId peerId;
    public final AgentId agentId;

    /** Cache the hash code */
    transient private int hash; // default to 0
    
    public PeerAndAgentId(PeerId peerId, AgentId agentId) {
        if (peerId == null || agentId == null) {
            throw new IllegalArgumentException(
                    "peerId and agentId should not be null");
        }
        this.peerId = peerId;
        this.agentId = agentId;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || !(o instanceof PeerAndAgentId)) return false;
        if (hashCode() != o.hashCode()) return false;
        PeerAndAgentId obj = (PeerAndAgentId) o;
        return peerId.equals(obj.peerId) && agentId.equals(obj.agentId);
    }
    
    @Override
    public int hashCode() {
        int h = hash; // hashCode()が並行でcallされてもよいように、一時変数を使う
        if (h == 0) {
            h = peerId.hashCode() * 97 + agentId.hashCode(); 
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return peerId + "|" + agentId;
    }
}
