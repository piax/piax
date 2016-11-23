package test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.piax.common.PeerLocator;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.util.MersenneTwister;

public class Util {
    public static Random rand = new MersenneTwister(12);
    
    public static int next(int n) {
        return rand.nextInt(n);
    }

    public static void printf(String f, Object... args) {
        System.out.printf(f, args);
    }
    
    public static void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
        }
    }

    public static void waitForKeyin() {
        try {
            System.in.read();
            while (System.in.available() > 0) {
                System.in.read();
            }
        } catch (IOException ignore) {
        }
    }

    public enum Net {
        EMU, UDP, TCP, NETTY
    }

    @SuppressWarnings("unchecked")
    public static <E extends PeerLocator> E genLocator(Net net, String host, int port) {
        PeerLocator loc;
        switch (net) {
        case EMU:
            loc = new EmuLocator(port);
            break;
        case UDP:
            loc = new UdpLocator(new InetSocketAddress(host, port));
            break;
        case TCP:
            loc = new TcpLocator(new InetSocketAddress(host, port));
            break;
        case NETTY:
            loc = new NettyLocator(new InetSocketAddress(host, port));
            break;
        default:
            loc = null;
        }
        return (E) loc;
    }

    public static List<?> fq2List(FutureQueue<?> fq) {
        List<Object> list = new ArrayList<Object>();
        for (RemoteValue<?> rv : fq) {
            if (rv == null) {
                System.out.println("?? null RemoteValue inserted");
                continue;
            }
            list.add(rv.getValue());
        }
        return list;
    }
}
