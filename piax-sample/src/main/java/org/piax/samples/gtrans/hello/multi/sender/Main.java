/* Sender - A sender process implementation of Hello World on overlay.
 *
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.samples.gtrans.hello.multi.sender;

import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.raw.tcp.TcpLocator;

/* To run on the localhost, type as follows on the shell:
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
		
		if (!(args.length == 4)) {
			System.out.println("Usage: Sender self-hostname self-port introducer-hostname introducer-port");
			System.exit(1);
		}
		selfHostName = args[0];
		selfPort = Integer.parseInt(args[1]);
		seedHostName = args[2];
		seedPort = Integer.parseInt(args[3]);
		seedLocator = new TcpLocator(new InetSocketAddress(seedHostName, seedPort));
		
		Peer p = Peer.getInstance(PeerId.newId());
		MSkipGraph<StringKey, StringKey> t = new MSkipGraph<StringKey, StringKey>(
				p.newBaseChannelTransport(new TcpLocator(new InetSocketAddress(selfHostName, selfPort))));
		if (seedLocator != null) {
			t.join(seedLocator);
		}
		t.send(new StringKey("hello"), "world");
		t.leave();
		Thread.sleep(1000);
		p.fin();
	}
}
