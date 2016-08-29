package test.rpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.TooManyListenersException;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteCallable;

import test.Util;

public class TestRPCInterruption extends Util {
    
    public interface SAppIf extends RPCIf {
        @RemoteCallable
        int sum(int s, int n) throws TooManyListenersException, RPCException;
    }
    
    public interface AppIf extends SAppIf {
//        @RemoteCallable(Type.ONEWAY)
        @RemoteCallable
        String longSleep(int stime) throws RPCException;
        
        @RemoteCallable
        void dummy(int n, String... s) throws RPCException;
    }

    static class App<E extends PeerLocator> extends RPCInvoker<AppIf, E>
            implements AppIf {
        App(TransportId transId, ChannelTransport<E> trans) throws IOException,
                IdConflictException {
            super(transId, trans);
        }

        public String longSleep(int stime) {
            System.out.println("I will sleep now: " + stime);
            sleep(stime);
            return "wakeup?";
        }
        
        public int sum(int s, int n) throws TooManyListenersException {
            int sum = 0;
            for (int i = 0; i < n; i++) {
                sum += s + i;
            }
            return sum;
        }
        
        public void dummy(int n, String... s) {
            System.out.println("dummy called: " + n + ", " + Arrays.toString(s));
        }
    }

    public interface ObjIf2 extends RPCIf {
        @RemoteCallable
        String concat(Number... s) throws RPCException;
    }
    
    static class Hoge implements ObjIf2 {
        public String concat(Number... s) {
            return "haaaha:" + Arrays.toString(s);
        }
    }

    /**
     * @param args
     */
    public static <E extends PeerLocator> void main(String[] args) {
        Net ntype = Net.UDP;
        printf("- start -%n");
        printf("- locator type: %s%n", ntype);

        // peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // BaseTransportを生成する
        ChannelTransport<E> tr1, tr2;
        try {
            tr1 = p1.newBaseChannelTransport(
                    Util.<E>genLocator(ntype, "localhost", 10001));
            tr2 = p2.newBaseChannelTransport(
                    Util.<E>genLocator(ntype, "localhost", 10002));
        } catch (IOException e) {
            System.out.println(e);
            return;
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }

        // app
        TransportId appId = new TransportId("app");

        App<E> a1;
        App<E> a2;
        try {
            a1 = new App<E>(appId, tr1);
            a2 = new App<E>(appId, tr2);
        } catch (IOException e) {
            System.out.println(e);
            return;
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }
        
        final AppIf stub = a1.getStub(tr2.getEndpoint());
        Thread th = new Thread() {
            public void run() {
                try {
                    String x = stub.longSleep(2000);
                    System.out.println("longsleep returns " + x);
                } catch (RPCException e) {
                    System.out.println(e);
                    System.out.println("isInterrupted = "
                            + Thread.currentThread().isInterrupted());
                }
                System.out.println("thread finished");
            };
        };
        th.start();
        sleep(1000);
        System.out.println("interrupt");
        th.interrupt();
        try {
            th.join();
        } catch (InterruptedException e) {}

        /*System.out.println("2nd try");
        try {
            String x = stub.longSleep(2000);
            System.out.println("longsleep returns " + x);
        } catch (RPCException e) {
            System.out.println(e);
            System.out.println("isInterrupted = "
                    + Thread.currentThread().isInterrupted());
        }*/
        
        printf("- end1 -%n");
        sleep(100);
        a1.fin();
        a2.fin();
        p1.fin();
        p2.fin();
        printf("- end -%n");
    }
}
