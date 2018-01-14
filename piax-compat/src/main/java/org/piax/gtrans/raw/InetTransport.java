/*
 * InetTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: InetTransport.java 1189 2015-06-06 14:57:58Z teranisi $
 */

package org.piax.gtrans.raw;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;

import org.piax.common.PeerId;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.impl.BaseTransportMgr;

/**
 * NetworkInterfaceのenable/disableの変化をBaseTransportMgrに通知する機能を持つTransport。
 * InetLocator専用。
 * 
 * 
 */
public abstract class InetTransport<E extends InetLocator> extends RawTransport<E> {
    
    public static String TIMER_THREAD_NAME = "netIfMon";
    protected static Timer monitorTimer;
    protected final static Map<InetAddress, MonitorTask> monitorTasks = 
            new HashMap<InetAddress, MonitorTask>();
    
    static class MonitorTask extends TimerTask {
        final InetAddress addr;
        NetworkInterface netIf = null;
        boolean isUp = true;
        // for thread safe
        final Set<InetTransport<?>> itranss = new CopyOnWriteArraySet<InetTransport<?>>();
        
        MonitorTask(InetAddress addr) {
            this.addr = addr;
        }
        
        @Override
        public void run() {
            try {
                if (netIf == null) {
                    netIf = NetworkInterface.getByInetAddress(addr);
                    if (netIf == null) {
                        cancel();
                        return;
                    }
                }
                boolean currIsUp = netIf.isUp();
                if (isUp == currIsUp) return;
                isUp = currIsUp;
                for (InetTransport<?> it : itranss) {
                    it.setIsUp(currIsUp);
                }
            } catch (SocketException e) {
                for (InetTransport<?> it : itranss) {
                    it.setHangup(e);
                }
            }
        }
    }
    
    protected static MonitorTask registerTrans(InetTransport<?> inetTrans,
            long interval) throws IOException {
        InetAddress addr = inetTrans.peerLocator.getInetAddress();
        synchronized (monitorTasks) {
            MonitorTask mon = monitorTasks.get(addr);
            if (mon == null) {
                mon = new MonitorTask(addr);
                if (monitorTimer == null) {
                    monitorTimer = new Timer(TIMER_THREAD_NAME, true);
                }
                monitorTimer.schedule(mon, interval, interval);
                monitorTasks.put(addr, mon);
            }
            mon.itranss.add(inetTrans);
            return mon;
        }
    }
    
    protected static void unregisterTrans(InetTransport<?> inetTrans)
            throws IOException {
        InetAddress addr = inetTrans.peerLocator.getInetAddress();
        synchronized (monitorTasks) {
            MonitorTask mon = monitorTasks.get(addr);
            if (mon == null) {
                return;
            }
            mon.itranss.remove(inetTrans);
            if (mon.itranss.size() == 0) {
                monitorTasks.remove(addr);
                mon.cancel();
            }
        }
    }
    
    protected MonitorTask monitorTask;

    protected InetTransport(PeerId peerId, E peerLocator, boolean supportsDuplex)
            throws IOException {
        this(peerId, peerLocator, supportsDuplex,
                GTransConfigValues.INET_CHECK_INTERVAL);
    }
    
    protected InetTransport(PeerId peerId, E peerLocator,
            boolean supportsDuplex, long interval) throws IOException {
        super(peerId, peerLocator, supportsDuplex);
        if (GTransConfigValues.USE_INET_MON) {
            monitorTask = registerTrans(this, interval);
        }
    }

    @Override
    public void fin() {
        if (!isActive) return;
        super.fin();
        try {
            unregisterTrans(this);
        } catch (IOException e) {
        }
    }

    @Override
    public boolean isUp() {
        if (monitorTask == null) return true;
        return monitorTask.isUp;
    }
    
    @Override
    public boolean hasStableLocator() {
        return false;
    }

    private void setIsUp(boolean isUp) {
        BaseTransportMgr btMgr = this.peer.getBaseTransportMgr();
        if (isUp) {
            btMgr.onEnabled(peerLocator, false);
        } else {
            btMgr.onFadeout(peerLocator, false);
        }
    }

    private void setHangup(Exception cause) {
        this.peer.getBaseTransportMgr().onHangup(peerLocator, cause);
    }
}
