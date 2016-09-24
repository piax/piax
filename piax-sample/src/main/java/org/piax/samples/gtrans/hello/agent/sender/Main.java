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
package org.piax.samples.gtrans.hello.agent.sender;

import java.io.File;
import java.net.InetSocketAddress;

import org.piax.agent.AgentId;
import org.piax.agent.AgentPeer;
import org.piax.agent.AgentTransportManager;
import org.piax.agent.DefaultAgentTransportManager;
import org.piax.common.Location;
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
		
		if (!(args.length == 4)) {
			System.out.println("Usage: Sender self-hostname self-port introducer-hostname introducer-port");
			System.exit(1);
		}
		selfHostName = args[0];
		selfPort = Integer.parseInt(args[1]);
		seedHostName = args[2];
		seedPort = Integer.parseInt(args[3]);
		seedLocator = new TcpLocator(new InetSocketAddress(seedHostName, seedPort));
		selfLocator = new TcpLocator(new InetSocketAddress(selfHostName, selfPort));
		
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

		AgentId id = p.createAgent(SenderAgent.class);
		
        SenderAgentIf stub = p.getHome().getStub(SenderAgentIf.class, id);
        stub.send();
        Thread.sleep(100);;
        p.leave();
		p.fin();
		System.out.println(p.getName() + " finished running.");
	}
}
