package test.misc;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketCheck {

    static void checkUdpBufferSize() throws Exception {
        int s = 1024 * 1024;
        DatagramSocket udpSoc = new DatagramSocket(10000);
        System.out.println("send:" + udpSoc.getSendBufferSize());
        System.out.println("rcv:" + udpSoc.getReceiveBufferSize());
        udpSoc.setSendBufferSize(s);
        udpSoc.setReceiveBufferSize(s);
        System.out.println("send:" + udpSoc.getSendBufferSize());
        System.out.println("rcv:" + udpSoc.getReceiveBufferSize());
        udpSoc.close();
    }
    
    static void checkTcpBufferSize() throws Exception {
        int s = 1024 * 1024;
        Socket tcpSoc = new Socket();
        System.out.println("send:" + tcpSoc.getSendBufferSize());
        System.out.println("rcv:" + tcpSoc.getReceiveBufferSize());
        tcpSoc.setSendBufferSize(s);
        tcpSoc.setReceiveBufferSize(s);
        System.out.println("send:" + tcpSoc.getSendBufferSize());
        System.out.println("rcv:" + tcpSoc.getReceiveBufferSize());
        tcpSoc.close();
    }
    
    static void checkReuseAddr() throws Exception {
        ServerSocket soc = new ServerSocket(10000);
        System.out.println("reuseAddr:" + soc.getReuseAddress());
        soc.setReuseAddress(true);
        System.out.println("reuseAddr:" + soc.getReuseAddress());
        soc.close();
    }
    
    @SuppressWarnings("null")
    static void perfNetIf() throws Exception {
        int n = 10;
//        InetAddress addr = InetAddress.getLocalHost();
        InetAddress addr = InetAddress.getByName("localhost");
//        InetAddress addr = InetAddress.getByName("192.168.64.106");

        // getByInetAddress
        NetworkInterface netIf = null;
        long stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            netIf = NetworkInterface.getByInetAddress(addr);
        }
        long lap = System.currentTimeMillis() - stime;
        System.out.println("NetIf: " + netIf);
        System.out.println(n + " times getByInetAddress() call(ms): " + lap);
        
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            netIf.isUp();
        }
        lap = System.currentTimeMillis() - stime;
        System.out.println(n + " times isUp() call(ms): " + lap);
    }
    
    static void ping(String host) throws IOException {
        int timeout = 2000;
        InetAddress addr = InetAddress.getByName(host);
        long stime = System.currentTimeMillis();
        boolean isReachable = addr.isReachable(timeout);
        long lap = System.currentTimeMillis() - stime;
        System.out.printf("(ping1) isReachable=%s, reply from %s: time=%sms%n",
                isReachable, host, lap);
    }

    static void ping2(String host) {
        try {
            String strCommand = "";
            System.out.println("My OS :" + System.getProperty("os.name"));
            if (System.getProperty("os.name").startsWith("Windows")) {
                // construct command for Windows Operating system
                strCommand = "ping -n 1 " + host;
            } else {
                // construct command for Linux and OSX
                strCommand = "ping -c 1 " + host;
            }
            System.out.println("Command: " + strCommand);
            // Execute the command constructed
            long stime = System.currentTimeMillis();
            Process myProcess = Runtime.getRuntime().exec(strCommand);
            myProcess.waitFor();
            long lap = System.currentTimeMillis() - stime;
            boolean isReachable = (myProcess.exitValue() == 0);
            System.out.printf("(ping2) isReachable=%s, reply from %s: time=%sms%n",
                    isReachable, host, lap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /*
     * 上記ping, ping2の実行結果は次のようになる。
     * JavaのInetAddress.isReachableは、必ず1000msかかり、場合によってつながらない。
     * （root権限がないと、ICMPを使わないため）
     * ping2は、外部コマンドを使っているため、通常のpingの動作になる。
     * 
     *  (ping1) isReachable=true, reply from 192.168.64.106: time=1013ms
     *  
     *  My OS :Windows 8
     *  Command: ping -n 1 192.168.64.106
     *  (ping2) isReachable=true, reply from 192.168.64.106: time=30ms
     */
    
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
//        checkUdpBufferSize();
//        checkTcpBufferSize();
//        checkReuseAddr();
//        perfNetIf();
        ping("192.168.64.106");
        ping2("192.168.64.106");
    }
}
