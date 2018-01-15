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

import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ov.suzaku.Suzaku;

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
        String self = null;
        String seed = null;

        if (!(args.length == 2 || args.length == 4)) {
            System.out
                    .println("Usage: Receiver self-hostname self-port [introducer-hostname introducer-port]");
            System.exit(1);
        }
        if (args.length == 4) { // not seed
            seed = "tcp:" + args[2] + ":" + args[3];
        }
        self = "id:" + args[0] + args[1] + ":tcp:" + args[0] + ":" + args[1];
        try(Suzaku<StringKey, StringKey> t = new Suzaku<>(self)) { 
            t.setListener((trans, msg) -> {
                System.out.printf("Received %s @ %s\n", msg.getMessage(),
                        trans.getBaseTransport().getEndpoint());
            });
            if (seed != null) {
                t.join(seed);
            } else {
                t.join(self);
            }
            t.addKey(new StringKey("hello"));
            System.out.println(t.getBaseTransport().getEndpoint() + " is ready.");
            Thread.sleep(60000);
            System.out.println(t.getBaseTransport().getEndpoint()
                    + " finished running.");
        }
    }
}
