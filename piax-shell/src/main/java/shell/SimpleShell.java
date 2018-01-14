/*
 * SimpleShell.java - A simple command line application to demonstrate PIAX Agents.
 * 
 * Copyright (c) 2009-2015 PIAX development team
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
 * $Id: SimpleShell.java 1241 2015-07-22 11:29:41Z shibajun $
 */

package shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.TypeVariable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.piax.agent.AgentException;
import org.piax.agent.AgentId;
import org.piax.agent.AgentIf;
import org.piax.agent.AgentPeer;
import org.piax.agent.AgentTransportManager;
import org.piax.agent.DefaultAgentTransportManager;
import org.piax.common.CalleeId;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.StatusRepo;
import org.piax.common.TransportId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.PeerLocator;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.kvs.dht.DHT;
import org.piax.kvs.dht.HashId;
import org.piax.util.LocalInetAddrs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleShell {
    /*--- logger ---*/
    private static final Logger log = LoggerFactory
            .getLogger(SimpleShell.class);

    static AgentTransportManager atm;
    static AgentPeer p;
    static String peerName;
    static PeerLocator myLocator = null;
    static PeerLocator seedLocator = null;
    static String locType = "";
    static Location loc = null;
    static InetSocketAddress myAddr;
    static InetSocketAddress seedAddr;
    static DHT dht = null;

    private static void printUsage() {
        System.out
                .println("Usage: piaxshell -? [-{u,t}] [-i<me>] -s<seed> [-x<x> -y<y>] name\n"
                        + "  ex(root).  piaxshell -s12367 root\n"
                        + "  ex(root).  piaxshell -i9000 -s9000 root\n"
                        + "  ex(root).  piaxshell -i210.156.0.59:12367 -s12367 -x130 -y30 root\n"
                        + "  ex.        piaxshell -s192.168.0.7:12367 foo\n"
                        + "  ex.        piaxshell -i9000 -s192.168.0.7:12367 foo\n"
                        + "  ex.        piaxshell -i210.156.0.59:12367 -s192.168.0.7:12367 foo\n"
                        + "  ex(use UDP).   piaxshell -u -s192.168.0.7:12367 bar\n"
                        + "  ex(use TCP).   piaxshell -t -s192.168.0.7:12367 bar\n"
                        + "  ex(outer NAT). piaxshell -n -s192.168.0.7:12367 bar\n"
                        + "  ex(inner NAT). piaxshell -p -s192.168.0.7:12367 bar\n");
    }

    /**
     * IPアドレスの文字列表現から適切と思われるInetSocketAddressを取得する。 "port" または "ip:port" の表現が可能。
     * ipにローカルアドレスが指定された場合は、外部からアクセス可能な アドレスを選ぶ。（但し、間違っている可能性あり）
     * 
     * @param ipAddr
     *            IPアドレスの文字列表現
     * @return 適切と思われるInetSocketAddress
     * @throws UnknownHostException
     *             ホスト名不明の場合
     */
    private static InetSocketAddress parseAddr(String ipAddr)
            throws UnknownHostException {
        InetAddress ip;
        int port;
        String[] addrEle = ipAddr.split(":");
        if (addrEle.length == 1) {
            ip = LocalInetAddrs.choice();
            port = Integer.parseInt(addrEle[0]);
        } else {
            ip = InetAddress.getByName(addrEle[0]);
            port = Integer.parseInt(addrEle[1]);
        }
        return new InetSocketAddress(ip, port);
    }

    private static boolean procArgs(String args[]) {
        try {
            String me = null;
            String seed = null;
            boolean isUdp = false;
            double x = Double.NaN;
            double y = Double.NaN;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i].trim();
                if (arg.startsWith("-")) {
                    switch (arg.charAt(1)) {
                    case 'u':
                        isUdp = true;
                        break;
                    case 't':
                        isUdp = false;
                        break;
                    case 'i':
                        me = arg.substring(2);
                        break;
                    case 's':
                        seed = arg.substring(2);
                        break;
                    case 'x':
                        x = Double.parseDouble(arg.substring(2));
                        break;
                    case 'y':
                        y = Double.parseDouble(arg.substring(2));
                        break;
                    case '?':
                        return false;
                    default:
                        return false;
                    }
                } else {
                    peerName = arg;
                }
            }

            if (seed == null || peerName == null)
                return false;
            if (!Double.isNaN(x) && !Double.isNaN(y)) {
                loc = new Location(x, y);
            }
            if (me == null)
                me = seed;
            myAddr = parseAddr(me);
            seedAddr = parseAddr(seed);
            if (isUdp) {
                myLocator = new UdpLocator(myAddr);
                seedLocator = new UdpLocator(seedAddr);
                locType = "udp";
            } else {
                myLocator = new TcpLocator(myAddr);
                seedLocator = new TcpLocator(seedAddr);
                locType = "tcp";
            }
            return true;

        } catch (Exception e) {
            log.error(e.toString());
            return false;
        }
    }

    private final List<AgentId> agents;
    private final List<AgentId> ragents;

    // private final AgentHome home;

    SimpleShell() {
        agents = new ArrayList<AgentId>();
        ragents = new ArrayList<AgentId>();
        // home = piaxPeer.getHome();
        
        StatusRepo.ON_MEMORY = true;
    }

    public String getServiceName() {
        return "SimpleShell";
    }

    void printHelp() {
        System.out
                .print(" *** PIAX SimpleShell Help ***\n"
                        + "  i)nfo           show my peer information\n"
                        +
                        // "  p)eek peerName  peek peer status\n" +
                        "  \n"
                        + "  ag)ents         list agents\n"
                        + "  rag)ents        list remote agents which this peer recognized\n"
                        + "  \n"
                        + "  join [x y]      join P2P net as (x, y) location\n"
                        + "  leave           leave from P2P net\n"
                        + "  move x y        move to (x, y) location\n"
                        + "  put key val     put val as key to DHT\n"
                        + "  get key         get value as key from DHT\n"
                        + "  \n"
                        + "  mk)agent class name\n"
                        + "                  create new agent as class and set name\n"
                        + "  mk)agent file\n"
                        + "                  create new agent by using file described agent info\n"
                        + "                  The format of the file is as follows:\n"
                        + "                  <agent class name> <name>\n"
                        + "  dup agent_NO    duplicate agent from which indicated by agent_NO\n"
                        + "  sl)eep agent_NO sleep agent indicated by agent_NO\n"
                        + "  wa)ke agetn_NO  wakeup agent indicated by agent_NO\n"
                        + "  fin agent_NO    destroy agent indicated by agent_NO\n"
                        + "  go agent_NO peerName\n"
                        + "                  travel agent to peer\n"
                        + "  go2 agent_NO peerId\n"
                        + "                  travel agent to peer specified by peer id\n"
                        + "  dc,discover x y w h method arg ...\n"
                        + "                  discoveryCall to agents in rect(x,y,w,h) area\n"
                        + "  dc2,discover2 x y r method arg ...\n"
                        + "                  discoveryCall to agents in circle(x,y,r) area\n"
                        + "  \n"
                        + "  c)all agent_NO method arg ...\n"
                        + "                  call agent method\n"
                        + "  rc)all peerName, remote_agent_NO method arg ...\n"
                        + "                  call remote agent method\n"
                        +
                        // "  cm,callm method arg ...\n" +
                        // "                  call multiple messages to agents\n"
                        // +
                        "  \n" + "  ?,help          show this help message\n"
                        + "  bye             exit\n" + "\n");
    }

    long stime;

    void mainLoop() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                System.in));

        while (true) {
            try {
                System.out.print("Input Command >");
                String line = reader.readLine();
                if (line == null) {
                    Thread.sleep(1000);
                    continue;
                }
                String[] args = line.split("\\s+");
                if ("".equals(args[0])) {
                    continue;
                }

                stime = System.nanoTime();

                if (args[0].equals("info") || args[0].equals("i")) {
                    if (args.length != 1) {
                        printHelp();
                        continue;
                    }
                    info();
                } else if (args[0].equals("desc")) {
                    desc();
                } else if (args[0].equals("agents") || args[0].equals("ag")) {
                    if (args.length > 1) {
                        printHelp();
                        continue;
                    }
                    agents();
                } else if (args[0].equals("ragents") || args[0].equals("rag")) {
                    if (args.length > 1) {
                        printHelp();
                        continue;
                    }
                    ragents();
                } else if (args[0].equals("join")) {
                    if (args.length != 1 && args.length != 3) {
                        printHelp();
                        continue;
                    }
                    if (args.length == 1) {
                        join();
                    } else {
                        double x = Double.parseDouble(args[1]);
                        double y = Double.parseDouble(args[2]);
                        join(x, y);
                    }
                } else if (args[0].equals("leave")) {
                    if (args.length != 1) {
                        printHelp();
                        continue;
                    }
                    leave();
                } else if (args[0].equals("move")) {
                    if (args.length != 3) {
                        printHelp();
                        continue;
                    }
                    double x = Double.parseDouble(args[1]);
                    double y = Double.parseDouble(args[2]);
                    move(x, y);
                } else if (args[0].equals("put")) {
                    if (args.length != 3) {
                        printHelp();
                        continue;
                    }
                    put(args[1], args[2]);
                } else if (args[0].equals("get")) {
                    if (args.length != 2) {
                        printHelp();
                        continue;
                    }
                    get(args[1]);
                } else if (args[0].equals("mkagent") || args[0].equals("mk")) {
                    if (args.length < 2 || args.length > 3) {
                        printHelp();
                        continue;
                    }
                    if (args.length == 2) {
                        mkagent(args[1]);
                    } else {
                        mkagent(args[1], args[2]);
                    }
                } else if (args[0].equals("dup")) {
                    if (args.length != 2) {
                        printHelp();
                        continue;
                    }
                    dup(Integer.parseInt(args[1]));
                } else if (args[0].equals("sleep") || args[0].equals("sl")) {
                    if (args.length != 2) {
                        printHelp();
                        continue;
                    }
                    sleep(Integer.parseInt(args[1]));
                } else if (args[0].equals("wake") || args[0].equals("wa")) {
                    if (args.length != 2) {
                        printHelp();
                        continue;
                    }
                    wake(Integer.parseInt(args[1]));
                } else if (args[0].equals("fin")) {
                    if (args.length != 2) {
                        printHelp();
                        continue;
                    }
                    fin(Integer.parseInt(args[1]));
                } else if (args[0].equals("go")) {
                    if (args.length != 3) {
                        printHelp();
                        continue;
                    }
                    go(Integer.parseInt(args[1]), args[2]);
                }
                // else if (args[0].equals("go2")) {
                // if (args.length != 3) {
                // printHelp();
                // continue;
                // }
                // go2(Integer.parseInt(args[1]), args[2]);
                // }
                else if (args[0].equals("discover") || args[0].equals("dc")) {
                    if (args.length < 6) {
                        printHelp();
                        continue;
                    }
                    double x = Double.parseDouble(args[1]);
                    double y = Double.parseDouble(args[2]);
                    double w = Double.parseDouble(args[3]);
                    double h = Double.parseDouble(args[4]);
                    Object[] cargs = new String[args.length - 6];
                    for (int i = 0; i < cargs.length; i++) {
                        cargs[i] = args[i + 6];
                    }
                    discover(x, y, w, h, args[5], cargs);
                }
                // else if (args[0].equals("discover2") ||
                // args[0].equals("dc2")) {
                // if (args.length < 5) {
                // printHelp();
                // continue;
                // }
                // double x = Double.parseDouble(args[1]);
                // double y = Double.parseDouble(args[2]);
                // double r = Double.parseDouble(args[3]);
                // Object[] cargs = new String[args.length - 5];
                // for (int i = 0; i < cargs.length; i++) {
                // cargs[i] = args[i + 5];
                // }
                // discover2(x, y, r, args[4], cargs);
                // }
                else if (args[0].equals("call") || args[0].equals("c")) {
                    if (args.length < 3) {
                        printHelp();
                        continue;
                    }
                    Object[] cargs = new String[args.length - 3];
                    for (int i = 0; i < cargs.length; i++) {
                        cargs[i] = args[i + 3];
                    }
                    call(Integer.parseInt(args[1]), args[2], cargs);
                } else if (args[0].equals("rcall2") || args[0].equals("rc2")) {
                    if (args.length < 4) {
                        printHelp();
                        continue;
                    }
                    Object[] cargs = new String[args.length - 4];
                    for (int i = 0; i < cargs.length; i++) {
                        cargs[i] = args[i + 4];
                    }
                    rcall(args[1], Integer.parseInt(args[2]), args[3], cargs);
                } else if (args[0].equals("rcall") || args[0].equals("rc")) {
                    if (args.length < 4) {
                        printHelp();
                        continue;
                    }
                    Object[] cargs = new String[args.length - 4];
                    for (int i = 0; i < cargs.length; i++) {
                        cargs[i] = args[i + 4];
                    }
                    System.out.println("return value:" + rcall0(args[1], args[2], args[3], cargs));
                }
                else if (args[0].equals("help") || args[0].equals("?")) {
                    printHelp();
                    continue;
                } else if (args[0].equals("bye")) {
                    leave();
                    return;
                } else {
                    printHelp();
                    continue;
                }

                long rap = System.nanoTime() - stime;
                System.out.printf("\t## time (msec): %.1f%n", rap / 1000000.0);
            } catch (NumberFormatException e) {
                System.out.println("\t>> arg should be number.");
            } catch (Exception e) {
                e.printStackTrace();
                //log.error(e.toString());
            }
        }
    }

    void descTransport(StringBuffer buf, int indentLevel, Transport<?> tr) {
        // indent
        for (int i = 0; i < indentLevel; i++) {
            buf.append("  ");
        }

        // out class name
        buf.append(tr.getClass().getName());
        TypeVariable<?>[] tp = tr.getClass().getTypeParameters();
        if (tp != null && tp.length > 0) {
            buf.append("<" + tp[0].getName());
            for (int i = 1; i < tp.length; i++) {
                buf.append(", " + tp[i].getName());
            }
            buf.append("> ");
        }

        // out id
        TransportId tid = tr.getTransportId();
        if (tid != null) {
            buf.append("id=" + tr.getTransportId());
        }
        buf.append("\n");

        // lower transport
        Transport<?> lt = null;
        try {
            lt = tr.getLowerTransport();
        } catch (UnsupportedOperationException e) {
            lt = null;
        }
        if (lt != null) {
            descTransport(buf, indentLevel + 1, lt);
        }
    }

    String getTransportDesc() {
        StringBuffer buf = new StringBuffer();

        // RPC Transport
        Transport<?> tr = null;
        try {
            tr = atm.getTransport("RPC");
        } catch (Exception e) {
            // just ignore.
        }
        buf.append("- RPC Transport\n");
        descTransport(buf, 1, tr);
        // CombinedTransport
        buf.append("- CombinedTransport id="
                + p.getHome().getCombinedOverlay().getTransportId() + "\n");
        List<String> attrs = p.getHome().getCombinedOverlay()
                .getDeclaredAttribNames();
        for (String attr : attrs) {
            buf.append("  for attribute=" + attr + "\n");
            Overlay<?, ?> ov = p.getHome().getCombinedOverlay()
                    .getBindOverlay(attr);
            if (ov == null) {
                buf.append("none");
            } else {
                descTransport(buf, 2, ov);
            }
        }

        return buf.toString();
    }

    void desc() {
        System.out.print(getTransportDesc());
    }

    void init() {
        // try {
        // put(peerName + ".pid", home.getPeerId().toString());
        // } catch (Exception e) {
        // System.out.println("\t>> cannot put peer info.");
        // return;
        // }
    }

    void info() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[Info]%n"));
        sb.append(String.format(" name: %s%n", p.getName()));
        sb.append(String.format(" id: %s%n", p.getPeerId()));
        sb.append(String.format(" locator: %s%n", myLocator));
        sb.append(String.format(" seed locator: %s%n", seedLocator));
        sb.append(String.format(" geo location: %s%n",
                p.getAttribValue("$location")));
        System.out.print(sb.toString());
    }

    void agents() {
        String name = null;
        agents.clear();
        int i = 0;
        for (AgentId agId : p.getAgentIds()) {
            agents.add(agId);
            try {
                name = p.getAgentName(agId);
            } catch (AgentException e) {
                e.printStackTrace();
            }
            boolean isSleeping;
            try {
                isSleeping = p.isAgentSleeping(agId);
                System.out.println(" " + i + ". name: " + name + ", ID: "
                        + agId + (isSleeping ? " <sleep>" : ""));
            } catch (AgentException e) {
                e.printStackTrace();
            }
            i++;
        }
    }

    void ragents() {
        Object[] vals = p.getHome().discoveryCall(
                "$location in rect(0.0,0.0,180.0,90.0)", "getAgentId");
        ragents.clear();
        if (vals == null || vals.length == 0) {
            System.out.println("\t>> No result.");
        }
        for (Object val : vals) {
            ragents.add((AgentId) val);
        }

        for (int i = 0; i < ragents.size(); i++) {
            System.out.println(" " + i + ": " + ragents.get(i));
        }
    }

    void join() {
        try {
            p.join();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("\t>> online failed by Exception.");
        }
    }

    void join(double x, double y) {
        move(x, y);
        join();
    }

    void leave() {
        try {
            p.leave();
        } catch (Exception e) {
            System.out.println("\t>> offline failed by Exception.");
        }
    }

    void move(double x, double y) {
        Location location = new Location(x, y);
        try {
            p.setAttrib("$location", location);
        } catch (Exception e) {
            System.out.println("\t>> set location failed.");
        }
        for (AgentId agId : p.getAgentIds()) {
            try {
                p.getHome().rcall(new CalleeId(agId, p.getPeerId(), peerName),
                        "setAttrib", "$location", location);
            } catch (Throwable e) {
                System.out.println("\t>> set location failed.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    public DHT getDHTService() {
        try {
            if (dht == null) {
                dht = new DHT(
                        (Overlay<LowerUpper, HashId>) atm.getOverlay("SZK"),
                        true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dht;
    }

    void put(String key, String val) {
        try {
            getDHTService().put(key, val);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void get(String key) {
        try {
            String val = (String) getDHTService().get(key);
            System.out.println(" value: " + val);
        } catch (IOException e) {
            log.error(e.toString());
        }
    }

    /* agents */
    void mkagent(String clazz, String name) {
        try {
            p.createAgent(clazz, name);
        } catch (Exception e) {
            System.out.println("\t>> cannot create new agent.");
        }
        move(0, 0);
    }

    void mkagent(String file) {
//        String agentPath = System.getProperty("piax.agent.path");
//        if (agentPath == null) {
//            System.out.println("\t>> cannot create agent from agent info file.");
//            return;
//        }
        File agentFile = new File(file);
        if (!agentFile.isFile()) {
            System.out
                    .println("\t>> agent info file named " + file + " is not found");
            return;
        }

        try {
            BufferedReader reader = new BufferedReader(
                    new FileReader(agentFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split("\\s+");
                if (items.length == 2) {
                    p.createAgent(items[0], items[1]);
                }
            }
            reader.close();
        } catch (Exception e) {
            System.out.println("\t>> creating agent failed from agent info file.");
            return;
        }
        move(0, 0);
    }

    void dup(int agentNo) {
        if (agents.size() <= agentNo) {
            System.out.println("\t>> invalid agent NO.");
            return;
        }
        AgentId agId = agents.get(agentNo);
        try {
            p.duplicateAgent(agId);
        } catch (Exception e) {
            System.out.println("\t>> cannot duplicate agent.");
        }
    }

    void sleep(int agentNo) {
        if (agents.size() <= agentNo) {
            System.out.println("\t>> invalid agent NO.");
            return;
        }
        AgentId agId = agents.get(agentNo);
        try {
            p.sleepAgent(agId);
        } catch (Exception e) {
            System.out.println("\t>> cannot sleep agent.");
        }
    }

    void wake(int agentNo) {
        if (agents.size() <= agentNo) {
            System.out.println("\t>> invalid agent NO.");
            return;
        }
        AgentId agId = agents.get(agentNo);
        try {
            p.wakeupAgent(agId);
        } catch (Exception e) {
            System.out.println("\t>> cannot wakeup agent.");
        }
    }

    void fin(int agentNo) {
        if (agents.size() <= agentNo) {
            System.out.println("\t>> invalid agent NO.");
            return;
        }
        AgentId agId = agents.get(agentNo);
        try {
            p.destroyAgent(agId);
        } catch (Exception e) {
            System.out.println("\t>> cannot destroy agent.");
        }
    }

    void go(int agentNo, String pName) {
        try {
            PeerId peerId = new PeerId(pName);
            AgentId agId = agents.get(agentNo);
            p.travelAgent(agId, peerId);
        } catch (Exception e) {
            System.out.println("\t>> cannot resolve peerName.");
        }
    }

    void discover(double x, double y, double w, double h, String method,
            Object... args) {
        String queryStr = String.format(
                "$location in rect(%.1f, %.1f, %.1f, %.1f)", x, y, w, h);
        Object[] vals = p.getHome().discoveryCall(queryStr, method, args);
        if (vals == null || vals.length == 0) {
            System.out.println("\t>> No result.");
        }
        for (Object value : vals) {
            System.out.println(" value: " + value);
        }
    }

    void call(int agentNo, String method, Object... cargs) {
        if (agents.size() <= agentNo) {
            System.out.println("\t>> invalid agent NO.");
            return;
        }
        AgentId agId = agents.get(agentNo);
        try {
            Object obj = p.getHome().rcall(
                    new CalleeId(agId, p.getPeerId(), peerName), method, cargs);
            System.out.println(" return value: " + obj);
        } catch (Throwable e) {
            System.out.println("\t>> cannot call agent.");
        }
    }

    private AgentId StringBytes2AgentId(String agent) {
        byte[] idb = new byte[agent.length() / 2];

        for (int i = 0; i < agent.length(); i += 2) {
            String bs = agent.substring(i, i + 2);
            idb[i / 2] = (byte) (Integer.parseInt(bs, 16) & 0xff);
        }

        return new AgentId(idb);
    }

    // primitive type
    String rcall0(String peer, String agent, String method, Object... cargs) {
        PeerId remotePeer = new PeerId(peer);
        AgentId agId = StringBytes2AgentId(agent);

        try {
            Object obj = p.getHome().rcall(agId, remotePeer, method, cargs);
            return obj.toString();
        } catch (Throwable e) {
            e.printStackTrace();
            return null;
        }
    }

    void rcall(String peer, int agentNo, String method, Object... cargs) throws Exception {
        AgentId agId = ragents.get(agentNo);
        try {
            Object obj = p.getHome().rcall(agId, new PeerId(peer), method,
                    cargs);
            System.out.println(" return value: " + obj);
        } catch (Throwable e) {
            System.out.println("\t>> cannot call agent.");
        }
    }

    public static void main(String... args) {
        if (!procArgs(args)) {
            printUsage();
            System.exit(-1);
        }
        try {
            atm = new DefaultAgentTransportManager(peerName, myLocator,
                    seedLocator);
            // use suzaku for range query overlay.
            atm.setOverlay("RQ", () -> {
                return atm.getOverlay("SZK");
            }, seedLocator);

            p = new AgentPeer(atm, new File("."));
            p.declareAttrib("$location", Location.class);
            try {
                p.bindOverlay("$location", "LLNET");
            } catch (IllegalArgumentException | NoSuchOverlayException
                    | IncompatibleTypeException e) {
                e.printStackTrace();
            }
            System.out.printf("** PIAX gtrans started (%s) **%n"
                    + " - please confirm the end address -%n"
                    + "   myself:   %s (%s)%n" + "   seed: %s%n%n", peerName,
                    myLocator, locType, seedLocator);
        } catch (IdConflictException e) {
            System.out.println("*** PIAX NOT started by internal error:"
                    + e.getMessage());
            System.exit(-1);
        } catch (Exception e) {
            System.out.println("*** PIAX NOT started by error:"
                    + e.getMessage());
            System.exit(-1);
        }

        SimpleShell ss = new SimpleShell();
        // dummy agentを作る
        ss.mkagent("agent.HelloAgent", "ag-" + peerName);

        ss.mainLoop();
        p.fin();
    }
}
