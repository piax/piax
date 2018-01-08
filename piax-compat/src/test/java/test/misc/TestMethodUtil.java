package test.misc;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteCallable;
import org.piax.util.MethodUtil;

import test.Util;

public class TestMethodUtil extends Util {

    // Note: この宣言にpublicは必要。なぜなら、RPCinvokerから見えないと呼べないから。
    public static interface HogeIf extends RPCIf {
        @RemoteCallable
        void hoge();
        @RemoteCallable
        void add(String a, Integer... vals);
    }

    // Note: この宣言にpublicは必要。なぜなら、MethodUtilのinvokeから見えないと呼べないから。
    public static class Hoge implements HogeIf {
        public void hoge() {
            printf("hoge%n");
        }

        public void add(String a, Integer... vals) {
            printf("vals:%s%n", Arrays.asList(vals));
        }
    }

    public static void main(String[] args) throws NoSuchMethodException,
            InvocationTargetException, IOException, IdConflictException {
        Hoge h = new Hoge();

        printf("** Method.invoke call%n");
        MethodUtil.invoke(h, "hoge");
        MethodUtil.invoke(h, "add", "a");
        MethodUtil.invoke(h, "add", "a", 1);
        MethodUtil.invoke(h, "add", "a", 1, 2, 3);
        MethodUtil.invoke(h, "add", "a", new Integer[] { 1, 2, 3 });

        // PeerからRPCInvokerまでを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        ChannelTransport<PeerLocator> bt1 = p1
                .newBaseChannelTransport(genLocator(Net.EMU, "localhost", 10000));
        ChannelTransport<PeerLocator> bt2 = p2
                .newBaseChannelTransport(genLocator(Net.EMU, "localhost", 10001));
        RPCInvoker<RPCIf, PeerLocator> rpc1 = new RPCInvoker<RPCIf, PeerLocator>(
                TransportId.NULL_ID, bt1);
        RPCInvoker<RPCIf, PeerLocator> rpc2 = new RPCInvoker<RPCIf, PeerLocator>(
                TransportId.NULL_ID, bt2);
        ObjectId objId = new ObjectId("hoge");
        rpc2.registerRPCObject(objId, h);
        HogeIf stub = rpc1.getStub(HogeIf.class, objId, bt2.getEndpoint());
        
        printf("%n** stub call%n");
        stub.hoge();
        stub.add("a");
        stub.add("a", 1);
        stub.add("a", 1, 2, 3);
        stub.add("a", new Integer[] { 1, 2, 3 });
        
        p1.fin();
        p2.fin();
    }
}
