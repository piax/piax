/*
 * EmuPool.java -- A table for server socket emulation
 * 
 * Copyright (c) 2012- National Institute of Information and 
 * Communications Technology
 * Copyright (c) 2006-2007 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: EmuPool.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.gtrans.raw.emu;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
class EmuPool {
    static Map<EmuLocator, EmuTransport> table = 
        new HashMap<EmuLocator, EmuTransport>();
    
    static synchronized void add(EmuTransport transport) {
        table.put(transport.getEndpoint(), transport);
    }

    static synchronized void remove(EmuLocator locator) {
        table.remove(locator);
    }

    static synchronized EmuTransport lookup(EmuLocator locator)
            throws IOException {
        EmuTransport emuTrans = table.get(locator);
        if (emuTrans == null) {
            throw new IOException("Cannot connect :-p");
        }
        return emuTrans;
    }

    static synchronized int size() {
        return table.size();
    }
}
