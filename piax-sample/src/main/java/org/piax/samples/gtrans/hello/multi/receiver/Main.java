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
package org.piax.samples.gtrans.hello.multi.receiver;

import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.sg.MSkipGraph;
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
		Peer p = Peer.getInstance(PeerId.newId());
		
		Overlay<StringKey, StringKey> t = 
                new MSkipGraph<StringKey, StringKey>(
                		p.newBaseChannelTransport(selfLocator));
		
		t.setListener(new TransportListener<StringKey>() {
			public void onReceive(Transport<StringKey> trans, ReceivedMessage msg) {
				try {
					System.out.printf("Received %s @ %s\n", msg.getMessage(), trans.getBaseTransport().getEndpoint());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		if (seedLocator != null) {
			t.join(seedLocator);
		}
		else {
			t.join(selfLocator);
		}
		Thread.sleep(100);;
		t.addKey(new StringKey("hello"));
		System.out.println(t.getBaseTransport().getEndpoint() + " is ready.");
		Thread.sleep(60000);
		p.fin();
		System.out.println(t.getBaseTransport().getEndpoint() + " finished running.");
	}
}
