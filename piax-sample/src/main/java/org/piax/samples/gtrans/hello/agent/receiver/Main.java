/* Receiver - A receiver process implementation of Hello World on overlay.
 *
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.samples.gtrans.hello.agent.receiver;

import java.io.File;
import java.net.InetSocketAddress;

import org.piax.agent.AgentId;
import org.piax.agent.AgentPeer;
import org.piax.agent.AgentTransportManager;
import org.piax.agent.DefaultAgentTransportManager;
import org.piax.common.Location;
import org.piax.gtrans.ov.ddll.NodeMonitor;
import org.piax.gtrans.raw.tcp.TcpLocator;

/* 
 * To run on the localhost, type as follows on the shell:
 *  $ java -jar Receiver.jar localhost 10000 &
 *  $ java -jar Receiver.jar localhost 10001 localhost 10000 &
 *  $ java -jar Receiver.jar localhost 10002 localhost 10001 &
 *  $ ... (as many as you want)
 *  $ java -jar Sender.jar localhost 20001 localhost 10001
 *  
 */
public class Main {
	static public void main(String args[]) throws Exception {
		boolean USE_SZK = false;
		String selfHostName = null;
		int selfPort = -1;
		String seedHostName = null;
		int seedPort = -1;
		TcpLocator seedLocator = null;
		TcpLocator selfLocator = null;
		
		if (!(args.length == 2 || args.length == 4)) {
			System.out.println("Usage: Receiver self-hostname self-port [introducer-hostname introducer-port]");
			System.exit(1);
		}
		selfHostName = args[0];
		selfPort = Integer.parseInt(args[1]);
		if (args.length == 4) { // not seed
			seedHostName = args[2];
			seedPort = Integer.parseInt(args[3]);
			seedLocator = new TcpLocator(new InetSocketAddress(seedHostName, seedPort));
		}
		selfLocator = new TcpLocator(new InetSocketAddress(selfHostName, selfPort));
		if (seedLocator == null) {
			seedLocator = selfLocator;
		}
		
		AgentTransportManager atm = new DefaultAgentTransportManager(selfHostName + ":" + selfPort,
				selfLocator, seedLocator);

		if (USE_SZK) {
			atm.setOverlay("RQ", ()->{
				return atm.getOverlay("SZK");
			}, seedLocator);
		}
		
		AgentPeer p = new AgentPeer(atm, new File("./"));
		
		p.declareAttrib("name", String.class);
		p.bindOverlay("name", "DOLR");
		p.declareAttrib("location", Location.class);
		p.bindOverlay("location", "LLNET");
		p.join();
		Thread.sleep(100);;

		AgentId id = p.createAgent(ReceiverAgent.class, "agent@" + p.getName());
		
        ReceiverAgentIf stub = p.getHome().getStub(ReceiverAgentIf.class, id);
        stub.setup();
		Thread.sleep(100);;
        System.out.println(p.getName() + " is ready.");
		Thread.sleep(60000);
        p.leave();
		p.fin();
		System.out.println(p.getName() + " finished running.");
	}
}
