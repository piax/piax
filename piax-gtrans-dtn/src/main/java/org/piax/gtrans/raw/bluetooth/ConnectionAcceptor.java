/*
 * ConnectionAcceptor.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ConnectionAcceptor.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.bluetooth;

import java.io.IOException;

import javax.bluetooth.ServiceRecord;
import javax.microedition.io.Connector;
import javax.microedition.io.StreamConnection;
import javax.microedition.io.StreamConnectionNotifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class ConnectionAcceptor extends Thread {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(ConnectionAcceptor.class);
    
    final BluetoothTransport transport;
    final String listenerURL;
    private StreamConnectionNotifier notifier;
    private volatile boolean isTerminated;
    
    ConnectionAcceptor(BluetoothTransport transport, String listenerURL) {
        this.transport = transport;
        this.listenerURL = listenerURL;
        isTerminated = false;
    }

    synchronized void terminate() {
        logger.trace("ENTRY:");
        if (isTerminated) return;
        isTerminated = true;
        try {
            // causes StreamConnectionNotifier#acceptAndOpen exception!
            notifier.close();
        } catch (IOException ignore) {}
        logger.trace("EXIT:");
    }
    
    boolean isTerminated() {
        return isTerminated;
    }

    @Override
    public void run() {
        logger.trace("ENTRY:");
        while (!isTerminated) {
            try {
                notifier = (StreamConnectionNotifier) Connector.open(listenerURL);
                ServiceRecord sr = transport.localDev.getRecord(notifier);
                int port = BluetoothTransport.getRFCOMMPort(sr);
                StreamConnection conn = notifier.acceptAndOpen();
                BluetoothChannel ch = transport.newAcceptedChannel(conn, port);
                transport.onAccepting(ch);
                // すぐに閉じて、新しくnotifierをopenする。
                notifier.close();
            } catch (IOException e) {
                if (!isTerminated) {
                    // unexpected error
                    transport.onHangup(e);
                }
                break;
            }
        }
        logger.trace("EXIT:");
    }
}
