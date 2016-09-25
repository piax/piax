/*
 * HelloAgent.java - A class that provides rapidly experience the PIAX behavior
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
 * $Id: HelloAgent.java 1241 2015-07-22 11:29:41Z shibajun $
 */

package agent;

import java.util.Date;
import java.util.logging.Logger;

import org.piax.agent.Agent;
import org.piax.agent.AgentId;

public class HelloAgent extends Agent implements HelloAgentIf {
    @Override
    public void onCreation() {
        System.out.println("Hello, here's my information.");

        System.out.println("CalleeId:\t" + getCalleeId());
        System.out.println("AgentId:\t" + getId());
        System.out.println("AgentName:\t" + getName());
        System.out.println("AgentFullName:\t" + getFullName());
        System.out.println("CreateTime:\t" + new Date(getCreationTime()));
        System.out.println("MotherPeerId: \t" + getMotherPeerId());
//        System.out.println("MotherPeerName:\t" + getMotherPeerName());
    }

    @Override
    public void onDestruction() {
        System.out.println("See you.");
    }

    public AgentId getAgentId() {
    	return getId();
    }
    
    public String hello() {
    	return "hello, my name is " + getName();
    	
    }

}
