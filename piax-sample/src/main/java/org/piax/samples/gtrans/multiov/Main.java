/* Main - The main program of multiple overlay sample.
 *
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.samples.gtrans.multiov;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.piax.agent.AgentId;
import org.piax.agent.AgentPeer;
import org.piax.agent.AgentTransportManager;
import org.piax.agent.DefaultAgentTransportManager;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.raw.tcp.TcpLocator;

public class Main {
	static public void main(String args[]) {
		PeerLocator rootLocator = new TcpLocator(new InetSocketAddress("localhost", 10000));
		int NUM_PARENTS = 5;
		PeerLocator parentLocators[] = new TcpLocator[NUM_PARENTS];
		int NUM_CHILDREN = 25;
		AgentId rootAgentId;
		List<Integer>pnumber = new ArrayList<Integer>();
		List<Integer>cnumber = new ArrayList<Integer>();
		AgentPeer peers[] = new AgentPeer[NUM_CHILDREN+NUM_PARENTS];
		
		try {
			AgentTransportManager atm = new DefaultAgentTransportManager("root",
					rootLocator, rootLocator);
			AgentPeer rootPeer = new AgentPeer(atm, new File("./"));
			rootPeer.declareAttrib("value", Double.class);
			rootPeer.bindOverlay("value", "MSG");
			rootPeer.join();
			rootAgentId = rootPeer.createAgent(RootAgent.class, "root@" + rootPeer.getName());
			for (int i = 0; i < NUM_PARENTS; i++) {
				pnumber.add(i);
			}
			pnumber.forEach(i -> {
				System.out.println("creating parent-" + i);
				parentLocators[i] = new TcpLocator(new InetSocketAddress("localhost", 10001 + i));
				AgentTransportManager patm = new DefaultAgentTransportManager("parent-" + i,
						parentLocators[i], rootLocator);
				try {
					patm.setOverlay("MSG2", ()->{
						ChannelTransport<?> base = (ChannelTransport<?>)patm.getTransport("BASE");
						return new MSkipGraph<Destination,ComparableKey<?>>(new TransportId("sg2"), base);
					},parentLocators[i]);
					peers[i] = new AgentPeer(patm, new File("./"));
					peers[i].declareAttrib("value", Double.class);
					peers[i].bindOverlay("value", "MSG");
					peers[i].declareAttrib("value2", Double.class);
					peers[i].bindOverlay("value2", "MSG2");
					peers[i].createAgent(ParentAgent.class, peers[i].getName());
					peers[i].join();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			for (int i = 0; i < NUM_CHILDREN; i++) {
				cnumber.add(i);
			}
			cnumber.forEach(i -> {
				System.out.println("creating children-" + i + " under parent-" + i / 5);
				PeerLocator childrenLocator = new TcpLocator(new InetSocketAddress("localhost", 11000 + i));
				AgentTransportManager catm = new DefaultAgentTransportManager("children-" + i,
						childrenLocator, parentLocators[i / 5]);
				try {
					catm.setOverlay("MSG2", ()->{
						ChannelTransport<?> base = (ChannelTransport<?>)catm.getTransport("BASE");
						return new MSkipGraph<Destination,ComparableKey<?>>(new TransportId("sg2"), base);
					}, parentLocators[i / 5]);
					peers[i+NUM_PARENTS]= new AgentPeer(catm, new File("./"));
					peers[i+NUM_PARENTS].declareAttrib("value2", Double.class);
					peers[i+NUM_PARENTS].bindOverlay("value2", "MSG2");
					peers[i+NUM_PARENTS].createAgent(ChildrenAgent.class, peers[i+NUM_PARENTS].getName());
					peers[i+NUM_PARENTS].join();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			System.out.println("sleeping 3 sec...");
			Thread.sleep(3000);
			rootPeer.getHome().getStub(RootAgentIf.class, rootAgentId).collectChildrenFromParents();
			rootPeer.leave();
			for (int i = 0; i < NUM_PARENTS+NUM_CHILDREN; i++) {
				peers[i].leave();
			}
			rootPeer.fin();
			for (int i = 0; i < NUM_PARENTS+NUM_CHILDREN; i++) {
				peers[i].fin();
			}
			System.out.println("finished");
		}
		catch (Exception e) {
			e.printStackTrace();
		}		
	}
}
