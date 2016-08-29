/*
 * ThreadUtil.java - A utility class for threads.
 * 
 * Copyright (c) 2009-2015 PIAX develoment team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 *
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * $Id: ThreadUtil.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.util.Timer;

import org.piax.common.PeerId;

/**
 * 
 */
public class ThreadUtil {
    private static String ID_SEPARATOR_IN_THREADNAME = ":";

    public static void concatPeerId2ThreadName(PeerId peerId) {
        String currName = Thread.currentThread().getName();
        int ix = currName.lastIndexOf(ID_SEPARATOR_IN_THREADNAME);
        if (ix != -1) {
            currName = currName.substring(0, ix);
        }
        Thread.currentThread().setName(currName + ID_SEPARATOR_IN_THREADNAME + peerId);
    }

    private static String clipPeerIdFromCurrentThreadName() {
        String currName = Thread.currentThread().getName();
        int ix = currName.lastIndexOf(ID_SEPARATOR_IN_THREADNAME);
        if (ix != -1) {
            return currName.substring(ix + 1, currName.length());
        }
        return "";
    }
    
    public static class _Runnable implements Runnable {
        private final PeerId peerId;
        private final Runnable runnable;
        
        public _Runnable(PeerId peerId, Runnable runnable) {
            this.peerId = peerId;
            this.runnable = runnable;
        }

        public void run() {
            concatPeerId2ThreadName(peerId);
            runnable.run();
        }
    }

    public static class _Timer extends Timer {
        public _Timer(String name, boolean isDaemon) {
            super(name + ":" + clipPeerIdFromCurrentThreadName(), isDaemon);
        }

        public _Timer(PeerId peerId, String name, boolean isDaemon) {
            super(name + ":" + peerId, isDaemon);
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
