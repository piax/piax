package test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.suzaku.Suzaku;
//import org.piax.gtrans.ov.szk.Suzaku;
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
        EMU, UDP, TCP, NETTY, ID
    }
    
    public enum Ov {
        SZK, SG
    }
    
    @SuppressWarnings("unchecked")
    public static <O extends Overlay> O genOverlay(Ov ov, String spec) throws IOException, IdConflictException{
        Overlay ret;
        switch (ov) {
        case SZK:
            ret = new Suzaku(spec);
            break;
        case SG:
            ret = new MSkipGraph(spec);
            break;
        default:
            ret = null;
        }
        return (O) ret;
    }

    @SuppressWarnings("unchecked")
    public static <O extends Overlay> O genOverlay(Ov ov, ChannelTransport<?> bt) throws IOException, IdConflictException{
        Overlay ret;
        switch (ov) {
        case SZK:
            ret = new Suzaku(bt);
            break;
        case SG:
            ret = new MSkipGraph(bt);
            break;
        default:
            ret = null;
        }
        return (O) ret;
    }
    
    @SuppressWarnings("unchecked")
    public static <O extends Overlay> O genOverlay(String transId, Ov ov, ChannelTransport<?> bt) throws IOException, IdConflictException{
        Overlay ret;
        switch (ov) {
        case SZK:
            ret = new Suzaku(new TransportId(transId), bt);
            break;
        case SG:
            ret = new MSkipGraph(new TransportId(transId), bt);
            break;
        default:
            ret = null;
        }
        return (O) ret;
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

    @SuppressWarnings("unchecked")
    public static <E extends Endpoint> E genEndpoint(Net net, PeerId pid, String host, int port) {
        Endpoint loc;
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
        case ID:
            loc = new PrimaryKey(pid, new NettyLocator(new InetSocketAddress(host, port)));
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
