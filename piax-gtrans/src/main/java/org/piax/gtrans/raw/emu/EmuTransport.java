/*
 * EmuTransport.java -- Emulation version of LocatorTransportService2
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
 * $Id: EmuTransport.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.emu;

import java.io.IOException;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.raw.RawChannel;
import org.piax.gtrans.raw.RawTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 */
public class EmuTransport extends RawTransport<EmuLocator> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(EmuTransport.class);

    public static int hopDelay = 0;

    public EmuTransport(PeerId peerId, EmuLocator peerLocator) {
        super(peerId, peerLocator, true);
        EmuPool.add(this);
    }
    
    @Override
    public void fin() {
        super.fin();
        EmuPool.remove(peerLocator);
    }

    @Override
    public boolean canSendNormalObject() {
        return true;
    }
    
    public void putNewLocator(PeerLocator newLoc) {
        EmuPool.add(this);
    }
    
    public void fadeoutLocator(EmuLocator oldLoc) {
        EmuPool.remove(oldLoc);
    }

    @Override
    public void send(EmuLocator dst, Object msg) throws IOException {
        // preserve the thread name (k-abe)
        String threadName = Thread.currentThread().getName();
        try {
            EmuTransport ts = EmuPool.lookup((EmuLocator) dst);
            ts.receive(peerLocator, msg);
        } catch (IOException e) {
            /*
             * 2010/8/14 yos
             * 送信先が閉じている状態なので、
             * ここで、IOExceptionを発生させてはいけない。
             */
            logger.info("destination closed");
        } finally {
            Thread.currentThread().setName(threadName);
        }
    }

    void receive(PeerLocator src, Object bbuf) {
        // ここに1hopのディレイを挟めばよい
        if (hopDelay > 0) {
            try {
                Thread.sleep(hopDelay);
            } catch (InterruptedException ignore) {
            }
        }
        ReceivedMessage rmsg = new ReceivedMessage(null, src, bbuf);
        if (this.listener != null)
            this.listener.onReceive(this, rmsg);
    }

    @Override
    public RawChannel<EmuLocator> newChannel(EmuLocator dst, boolean isDuplex,
            int timeout) throws IOException {
        throw new UnsupportedOperationException();
    }
}
