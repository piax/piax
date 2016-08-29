/*
 * MobileAgent.java - An mobile agent class
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
 * $Id: MobileAgent.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.piax.agent.impl.AgentContainer;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.agent.impl.InitialAgentMode;
import org.piax.common.PeerId;

/**
 * An mobile agent class
 */
public abstract class MobileAgent extends PersistentAgent 
        implements MobileAgentIf {
    private static final long serialVersionUID = -2227756492286070065L;
    
    private List<PeerId> travelList;

    @Override
    public void _$init(AgentHomeImpl home, String name) {
        super._$init(home, name);
        travelList = new ArrayList<PeerId>();
        travelList.add(home.getPeerId());
    }

    @Override
    public void _$bindAndActivate(AgentContainer agContainer, InitialAgentMode mode) {
        super._$bindAndActivate(agContainer, mode);
        if (mode == InitialAgentMode.ARRIVAL) {
            onArrival();
        }
    }

    public void _$addPeer(PeerId peerId) {
        travelList.add(peerId);
    }
    
    public final PeerId getPreviousPeerId() {
        int ix = travelList.size() - 2;
        if (ix == -1) {
            return null;
        }
        return travelList.get(ix);
    }
    
    public final List<PeerId> getTraveledPeerIds() {
        return Collections.unmodifiableList(travelList);
    }
    
    public void travel(PeerId peerId) throws ClassNotFoundException,
            ObjectStreamException, IOException, AgentAccessDeniedException {
        // TODO loc
        AgentContainer agContainer = _$getContainer();
        if (agContainer != null && !agContainer.isSleeping()) {
//            onDeparture();
            try {
                agContainer.travel(peerId);
            } catch (IllegalAgentModeException e) {
                // never occur
            }
        }
    }

    public void onArrival() {
    }

    public void onDeparture() {
    }

}
