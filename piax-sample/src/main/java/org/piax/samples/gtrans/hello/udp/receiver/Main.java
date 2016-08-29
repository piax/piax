/* Receiver - A receiver process implementation of Hello World.
 *
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */ 
package org.piax.samples.gtrans.hello.udp.receiver;

import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.raw.udp.UdpLocator;

/* 
 * This sample requires at least two processes.
 * Two processes exchange Hello World message with each other via UDP.
 * To run on the localhost, type as follows on the shell:
 *  $ java -jar Receiver localhost 10000
 *  $ java -jar Sender localhost 10001 localhost 10000
 */
public class Main {
	static public void main(String args[]) throws Exception {
		String selfHostName = null;
		int selfPort = -1;

		if (!(args.length == 2)) {
			System.out.println("Usage: Receiver self-hostname self-port");
			System.exit(1);
		}
		selfHostName = args[0];
		selfPort = Integer.parseInt(args[1]);
		Peer p = Peer.getInstance(PeerId.newId());
		Transport<UdpLocator> t = p.newBaseTransport(new UdpLocator(new InetSocketAddress(selfHostName, selfPort)));

		t.setListener(new TransportListener<UdpLocator>() {
			public void onReceive(Transport<UdpLocator> trans, ReceivedMessage msg) {
				try {
					System.out.printf("Received %s @ %s\n", msg.getMessage(), trans.getEndpoint());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		System.out.println(t.getBaseTransport().getEndpoint() + " is ready.");
		Thread.sleep(60000);
		p.fin();
		System.out.println(t.getBaseTransport().getEndpoint() + " finished running.");
	}
}
