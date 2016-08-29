/* Sender - A sender process implementation of Hello World.
 *
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */ 
package org.piax.samples.gtrans.hello.udp.sender;

import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.raw.udp.UdpLocator;

/* 
 * This sample requires at least two processes.
 * Two processes exchange Hello World message with each other via UDP.
 * To run on the localhost, type as follows on the shell:
 *  $ java -jar Receiver.jar localhost 10000
 *  $ java -jar Sender.jar localhost 10001 localhost 10000
 */
public class Main {
	static public void main(String args[]) throws Exception {
		String selfHostName = null;
		int selfPort = -1;
		String dstHostName = null;
		int dstPort = -1;
		if (!(args.length == 4)) {
			System.out.println("Usage: Sender self-hostname self-port dst-hostname dst-port");
			System.exit(1);
		}
		selfHostName = args[0];
		selfPort = Integer.parseInt(args[1]);
		dstHostName = args[2];
		dstPort = Integer.parseInt(args[3]);

		Peer p = Peer.getInstance(PeerId.newId());
		Transport<UdpLocator> t = p.newBaseTransport(new UdpLocator(new InetSocketAddress(selfHostName, selfPort)));
		t.send(new UdpLocator(new InetSocketAddress(dstHostName, dstPort)), "hello world");
		p.fin();
	}
}
