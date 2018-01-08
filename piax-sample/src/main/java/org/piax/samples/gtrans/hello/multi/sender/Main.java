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

import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ov.suzaku.Suzaku;

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

        if (!(args.length == 4)) {
            System.out.println("Usage: Sender self-hostname self-port introducer-hostname introducer-port");
            System.exit(1);
        }
        String self = "tcp:" + args[0] + ":" + args[1];
        String seed = "tcp:" + args[2] + ":" + args[3];
        
        try(Suzaku<StringKey, StringKey> t = new Suzaku<>("id:" + selfHostName+selfPort + ":" + self)) {
            t.join(seed);
            t.send(new StringKey("hello"), "world");
            t.leave();
        }
		Thread.sleep(1000);
	}
}
