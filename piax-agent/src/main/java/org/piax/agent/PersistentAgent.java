/*
 * PersistentAgent.java - An agent with persistency.
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
 * $Id: PersistentAgent.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectStreamException;

import org.piax.agent.impl.AgentAttribs;
import org.piax.agent.impl.AgentContainer;
import org.piax.agent.impl.InitialAgentMode;
import org.piax.common.PeerId;

/**
 * An agent with persistency.
 */
public abstract class PersistentAgent extends Agent 
        implements PersistentAgentIf {
    private static final long serialVersionUID = 344332485179705343L;

    @Override
    public void _$bindAndActivate(AgentContainer agContainer, InitialAgentMode mode) {
        super._$bindAndActivate(agContainer, mode);
        if (mode == InitialAgentMode.WAKEUP) {
            onWakeup();
        } else if (mode == InitialAgentMode.DUPLICATION) {
            onDuplication();
        } else if (mode == InitialAgentMode.RESTORE) {
            onRestore();
        }
    }
    
    public final void sleep() 
            throws ObjectStreamException, IOException {
        // TODO loc
        AgentContainer agContainer = _$getContainer();
        if (agContainer != null && !agContainer.isSleeping()) {
//            onSleeping();
            try {
                agContainer.sleep();
            } catch (IllegalAgentModeException e) {
                // never occur
            }
        }
    }
    
    public final AgentId duplicate() 
            throws ObjectStreamException {
        // TODO loc
        AgentContainer agContainer = _$getContainer();
        AgentId id = null;
        if (agContainer != null && !agContainer.isSleeping()) {
//            onDuplicating();
            try {
                id = agContainer.duplicate();
            } catch (IllegalAgentModeException e) {
                // never occur
            }
            return id;
        }
        return null;
    }
    
    public final void save() 
            throws ObjectStreamException, IOException {
        save(null);
    }
    
    public final void save(File file) 
            throws FileNotFoundException, 
            ObjectStreamException, IOException {
        // TODO loc
        AgentContainer agContainer = _$getContainer();
        if (agContainer != null && !agContainer.isSleeping()) {
//            onSaving();
            try {
                agContainer.save(file);
            } catch (IllegalAgentModeException e) {
                // never occur
            }
        }
    }
    
    public void onSleeping() {
    }

    public void onWakeup() {
    }

    public void onDuplication() {
    }
    public void onDuplicating() {
    }
    public void onSaving() {
    }
    public void onRestore() {
    }

    private void writeObject(java.io.ObjectOutputStream s)
    throws java.io.IOException {
        s.defaultWriteObject();

        s.writeObject(id);
        s.writeObject(name);
        s.writeLong(createTime);
        s.writeObject(motherPeerId);
        s.writeObject(attribs);
    }

    private void readObject(java.io.ObjectInputStream s)
    throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        
        id = (AgentId) s.readObject();
        name = (String) s.readObject();
        createTime = s.readLong();
        motherPeerId = (PeerId) s.readObject();
        attribs = (AgentAttribs) s.readObject();
    }
}
