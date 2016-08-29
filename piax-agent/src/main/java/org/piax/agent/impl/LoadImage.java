/*
 * LoadImage.java - A class for agent serialization image
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
 */
/*
 * Revision History:
 * ---
 * 
 * $Id: LoadImage.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent.impl;

import java.io.ObjectStreamException;
import java.util.Map;

import org.piax.agent.Agent;
import org.piax.agent.InvalidAgentJarException;
import org.piax.agent.impl.asm.ByteCodes;
import org.piax.agent.impl.loader.AgentLoader;
import org.piax.agent.impl.loader.AgentLoaderPool;
import org.piax.util.JarUtil;
import org.piax.util.JarUtil.Parcel;
import org.piax.util.SerializingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for agent serialization image
 * Agentを serialize するための元となる情報を持つクラス。
 * Agentを継承したクラスでも使用する部品であることを考慮して、
 * publicなクラスとして定義している。
 */
public class LoadImage {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(LoadImage.class);
    
    /*--- constants ---*/
    static final String AGENT_INSTANCE = "AGENT-INSTANCE.ser";
    static final String AGENT_CLASS_NAME = "Agent-Class-Name";
    static final String AGENT_ID = "Agent-ID";
    static final String AGENT_NAME = "Agent-Name";

    Agent agent;
    ByteCodes codes;
    AgentLoader loader;
    
    Parcel par = null;
    String agentClassName = null;
    String agentName = null;
    String agentId = null;
    
    public LoadImage(Agent agent, ByteCodes codes, AgentLoader loader) {
        this.agent = agent;
        this.codes = codes;
        this.loader = loader;
    }
    
    LoadImage(byte[] jar) {
        par = JarUtil.unwrap(jar);

        agentClassName = par.attribs.getValue(AGENT_CLASS_NAME);
        agentName = par.attribs.getValue(AGENT_NAME);
        agentId = par.attribs.getValue(AGENT_ID);
    }
    
    void loadLoadImage(AgentLoaderPool loaderPool)
            throws InvalidAgentJarException, 
            ObjectStreamException, ClassNotFoundException {
        byte[] ag = par.others.get(AGENT_INSTANCE);
        if (ag == null) {
            throw new InvalidAgentJarException();
        }
        codes = new ByteCodes();
        for (Map.Entry<String, byte[]> ent : par.classes.entrySet()) {
            codes.put(ent.getKey(), ent.getValue());
        }
        loader = 
            loaderPool.getAccecptableLoaderAsAdded(agentClassName, codes);
        agent = (Agent) SerializingUtil.deserialize(ag, loader);
        logger.debug("new LoadImage");
    }

    public byte[] makeJar() throws ObjectStreamException {
        logger.debug("makeJar called");
        byte[] agSer = SerializingUtil.serialize(agent);
        
        par = new Parcel();
        agentClassName = agent.getClass().getName();
        agentId = agent.getId().toString();
        agentName = agent.getName();
        par.attribs.putValue(AGENT_CLASS_NAME, agentClassName);
        par.attribs.putValue(AGENT_ID, agentId);
        par.attribs.putValue(AGENT_NAME, agentName);
        for (String cname : codes.getClassNames()) {
            par.classes.put(cname, codes.get(cname));
        }
        par.others.put(AGENT_INSTANCE, agSer);
        return JarUtil.wrap(par);
    }
}
