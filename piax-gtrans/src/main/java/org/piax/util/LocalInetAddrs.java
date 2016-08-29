/*
 * LocalInetAddrs.java - A utility for local inet address handling
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
 * $Id: LocalInetAddrs.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.util;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ローカルに保持されるIPアドレスを外向けアドレスとして使用されているものを
 * 選び出すためのユーティリティクラス。
 * 但し、以下の制限がある。
 * <ul>
 * <li>IPv6には未対応（IPv6アドレスは除外される）
 * <li>Java6以上を前提にしている。
 * <li>2つ以上の候補が存在する場合、先に見つかったアドレスを返す。
 * </ul>
 * 
 */
public class LocalInetAddrs {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(LocalInetAddrs.class);

    /** ローカルIPアドレスのリストを頻繁に更新しないための抑止期間 */
    public static final long NO_LISTUP_PERIOD = 5000;
    
    private static long listedUpTime = 0;
    private static List<InetAddress> linkLocals = new ArrayList<InetAddress>();
    private static List<InetAddress> siteLocals = new ArrayList<InetAddress>();
    private static List<InetAddress> globals = new ArrayList<InetAddress>();

    /**
     * マシンが持つIPアドレスのうち、IPv4アドレスについて、
     * link local, site local, globalの各リストに分類する。
     * <p>
     * 但し、NO_LISTUP_PERIODで既定された期間はlistupが呼ばれても、
     * リストの更新は行わない。
     */
    private static void listup() {
        long thisTime = System.currentTimeMillis();
        if (thisTime - listedUpTime < NO_LISTUP_PERIOD) return;
        listedUpTime = thisTime;
        
        linkLocals.clear();
        siteLocals.clear();
        globals.clear();
        
        Enumeration<NetworkInterface> netIfs;
        try {
            netIfs = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            // never occurred.
            logger.error("", e);
            return;
        }
        if (netIfs == null) {
            logger.warn("there is NO network I/F");
            return;
        }
        while (netIfs.hasMoreElements()) {
            NetworkInterface netIf = netIfs.nextElement();
            /*
             * netIf.isUp()とisVirtual() がJava6のメソッドであるため、
             * Java5互換にするためには以下をはずす必要がある。
             */
            try {
                if (!netIf.isUp() || netIf.isVirtual()) {
                    continue;
                }
            } catch (SocketException e) {
                // never occurred.
                logger.error("", e);
                return;
            }
            Enumeration<InetAddress> inetAddrs = netIf.getInetAddresses();
            while (inetAddrs.hasMoreElements()) {
                InetAddress inetAddr = inetAddrs.nextElement();
                if (inetAddr instanceof Inet6Address) {
                    // IPv6 アドレスは無視する
                } else if (inetAddr.isLoopbackAddress()) {
                    // 何もしない
                } else if (inetAddr.isLinkLocalAddress()) {
                    linkLocals.add(inetAddr);
                } else if (inetAddr.isSiteLocalAddress()) {
                    siteLocals.add(inetAddr);
                } else {
                    globals.add(inetAddr);
                }
            }
        }
    }

    /**
     * 指定されたInetAddressがローカルIPアドレスの場合はtrueを返す。
     * 
     * @param target 指定されたInetAddress
     * @return targetがローカルIPアドレスの場合はtrue、それ以外はfalse
     */
    public static synchronized boolean isLocal(InetAddress target) {
        listup();
        String address = target.getHostAddress();
        
        for (InetAddress addr : globals) {
            if (address.equals(addr.getHostAddress())) {
                return true;
            }
        }
        for (InetAddress addr : siteLocals) {
            if (address.equals(addr.getHostAddress())) {
                return true;
            }
        }
        for (InetAddress addr : linkLocals) {
            if (address.equals(addr.getHostAddress())) {
                return true;
            }
        }
        if (target.isLoopbackAddress()) {
            return true;
        }
        return false;
    }

    /**
     * グローバルアドレス、サイトローカルアドレス、リンクローカルアドレスの順に、
     * ローカルIPアドレスの中から接続性の高い外部アドレスを選択して返す。
     * 外部アドレスがない場合は、ループバックアドレス（127.0.0.1）を返す。
     * <p>
     * 尚、複数のIPアドレスの候補が存在する場合は最初に見つかったアドレスが
     * 返される。このため、明示的にセットアップしたアドレスが返されないことも
     * 起こりえる。
     * 
     * @return 接続性の高い外部アドレス、ない場合は127.0.0.1
     */
    public static synchronized InetAddress choice() {
        listup();
        if (globals.size() > 0) {
            return globals.get(0);
        }
        if (siteLocals.size() > 0) {
            return siteLocals.get(0);
        }
        if (linkLocals.size() > 0) {
            return linkLocals.get(0);
        }
        try {
            return InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e) {
            // normally never occurred.
            logger.error("", e);
            return null;
        }
    }
    
    /**
     * 指定されたInetAddress targetがローカルIPアドレスの場合に限り、
     * choiceを呼び出す。
     * targetがローカルIPアドレスでない場合は、それ自身が返される。
     * 
     * @param target 指定されたInetAddress
     * @return targetがローカルIPアドレスの場合はより接続性の高い外部アドレス、
     * それ以外の場合はtarget
     */
    public static synchronized InetAddress choiceIfIsLocal(InetAddress target) {
        if (isLocal(target)) {
            return choice();
        }
        return target;
    }
}
