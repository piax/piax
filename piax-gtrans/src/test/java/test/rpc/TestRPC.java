package test.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.piax.common.CalleeId;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.IllegalRPCAccessException;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RPCMode;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import test.Util;

public class TestRPC extends Util {
    private static final Logger logger = 
            LoggerFactory.getLogger(TestRPC.class);
    static ArrayBlockingQueue<String> resultQueue = new ArrayBlockingQueue<String>(1);
    
    public interface SAppIf extends RPCIf {
        @RemoteCallable
        int sum(int n) throws RPCException;
        
        @RemoteCallable
        void syncRPC() throws RPCException;
        
        @RemoteCallable
        String arrayArg(String[] a) throws RPCException;;
        
        @RemoteCallable
        String arrayArg2(String[] a, int n, String[] b) throws RPCException;;;
        
        @RemoteCallable
        String varArgs(String[] a, String b, String...c) throws RPCException;;;
        
        @RemoteCallable(Type.ONEWAY)
        void oneway() throws RPCException;;;
        
        void localOneway();
        
        int localMethod(int n);
    }
    
    public interface InvokerAppIf extends SAppIf {
        @RemoteCallable
        void longSleep(int stime) throws RPCException;
        
        @RemoteCallable
        int throwEx() throws Exception;
        
        @RemoteCallable
        Object returnNull() throws RPCException;;;
        
        @RemoteCallable
        int callback(PeerLocator caller);
    }

    public static class InvokerApp<E extends PeerLocator> extends RPCInvoker<InvokerAppIf, E>
            implements InvokerAppIf {
        InvokerApp(TransportId transId, ChannelTransport<E> trans) throws IOException,
                IdConflictException {
            super(transId, trans);
        }

        public void longSleep(int stime) {
            logger.debug("I will sleep now: " + stime);
            sleep(stime);
        }
        
        public int sum(int n) {
            int sum = 0;
            for (int i = 1; i <= n; i++) {
                sum += i;
            }
            return sum;
        }
        
        @Override
        @RemoteCallable(Type.ONEWAY)
        public void oneway() {
            sleep(1000);
            try {
                resultQueue.put("oneway");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void localOneway() {
            sleep(1000);
            try {
                resultQueue.put("localOneway");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        @RemoteCallable
        public void syncRPC() {
            sleep(1000);
            try {
                resultQueue.put("syncRPC");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        @RemoteCallable
        public int throwEx() throws Exception {
            throw new Exception("throwEx");
        }

        @Override
        @RemoteCallable
        public Object returnNull() {
            return null;
        }

        @Override
        public int localMethod(int n) {
            return sum(n);
        }

        @Override
        public String arrayArg(String[] a) {
            String r = "";
            for (String s:a) {
                r += s;
            }
            return r;
        }
        
        @Override
        public String arrayArg2(String[] a, int n, String[] b) {
            String r = "";
            for (String s:a) {
                r += s;
            }
            r += String.format("%d", n-1);
            for (String s:b) {
                r += s;
            }
            return r;
        }
        
        @Override
        public String varArgs(String[] a, String b, String...c) {
            String r = "";
            for (String s:a) {
                r += s;
            }
            r += b.toLowerCase();
            for (String s:c) {
                r += s;
            }
            return r;
        }
        
        @Override
        public int callback(PeerLocator caller) {
            InvokerAppIf stub = getStub((E)caller);
            int sum = 0;
            try {
                sum = stub.sum(10);
            } catch (RPCException e) {
                e.printStackTrace();
            }
            return sum + 1; 
        }
    }
    
    public static class App implements SAppIf {
        
        public App() {}
        
        @Override
        public int sum(int n) {
            int sum = 0;
            for (int i = 1; i <= n; i++) {
                sum += i;
            }
            return sum+1;
        }
        
        @Override
        public String arrayArg(String[] a) {
            String r = "";
            for (String s:a) {
                r += s;
            }
            return r;
        }
        
        @Override
        public String arrayArg2(String[] a, int n, String[] b) {
            String r = "";
            for (String s:a) {
                r += s;
            }
            r += String.format("%d", n-1);
            for (String s:b) {
                r += s;
            }
            return r;
        }
        
        @Override
        public String varArgs(String[] a, String b, String...c) {
            String r = "";
            for (String s:a) {
                r += s;
            }
            r += b.toLowerCase();
            for (String s:c) {
                r += s;
            }
            return r;
        }
        
        @Override
        @RemoteCallable
        public void syncRPC() {
            sleep(1000);
            try {
                resultQueue.put("syncRPC");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        @Override
        @RemoteCallable(Type.ONEWAY)
        public void oneway() {
            sleep(1000);
            try {
                resultQueue.put("oneway");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void localOneway() {
            sleep(1000);
            try {
                resultQueue.put("localOneway");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public int localMethod(int n) {
            return sum(n);
        }
    }
    
    static InvokerApp<PeerLocator> invokerApp1;
    static InvokerApp<PeerLocator> invokerApp2;
    static App app1,app2;

    static ChannelTransport<PeerLocator> transport1, transport2;
    static Peer peer1,peer2;

    /**
     * 前準備
     * ピアやトランスポート、RPC対象のオブジェクトの作成など
     */
    @BeforeClass
    public static void setup() {
        Net ntype = Net.NETTY;//TCP
        logger.debug("- start -%n");
        logger.debug("- locator type: %s%n", ntype);

        // peerを用意する
        peer1 = Peer.getInstance(new PeerId("peer1"));
        peer2 = Peer.getInstance(new PeerId("peer2"));

        // BaseTransportを生成する
        try {
            transport1 = peer1.newBaseChannelTransport(
                    Util.<PeerLocator>genLocator(ntype, "localhost", 10001));
            transport2 = peer2.newBaseChannelTransport(
                    Util.<PeerLocator>genLocator(ntype, "localhost", 10002));
        } catch (IOException e) {
            logger.debug(e.toString());
            return;
        } catch (IdConflictException e) {
            logger.debug(e.toString());
            return;
        }

        // RPCInvokerのサブクラス
        TransportId appId = new TransportId("invokerApp");

        try {
            invokerApp1 = new InvokerApp<PeerLocator>(appId, transport1);
            invokerApp2 = new InvokerApp<PeerLocator>(appId, transport2);
        } catch (IOException e) {
            logger.debug(e.toString());
            return;
        } catch (IdConflictException e) {
            logger.debug(e.toString());
            return;
        }
        
        // 他のクラス
        ObjectId objId = new ObjectId("app");
        app1 = new App();
        app2 = new App();
        try {
            invokerApp1.registerRPCObject(objId, app1);
            invokerApp2.registerRPCObject(objId, app2);
        } catch (IdConflictException e) {
            logger.debug(e.toString());
            return;
        }
    }
    
    /**
     * RPCInvokerのサブクラスへの通常のRPC
     */
    @Test
    public void rpcToInvokerAppNormal() throws RPCException {
        InvokerAppIf stub = invokerApp1.getStub(transport2.getEndpoint());
        int sum = stub.sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * RPCInvokerのサブクラスへの通常のRPC
     * timeout付き
     */
    @Test
    public void rpcToInvokerAppNormalTimeout() throws RPCException {
        InvokerAppIf stub = invokerApp1.getStub(transport2.getEndpoint(),10000);
        int sum = stub.sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * RPCInvokerのサブクラスへの通常のRPC
     */
    @Test
    public void callInvokerAppNormal() throws RPCException {
        int sum = invokerApp1.getStub(transport2.getEndpoint()).sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * RPCInvokerのサブクラスへの通常のRPC
     * timeout付き
     */
    @Test
    public void callInvokerAppNormalTimeout() throws RPCException {
        int sum = invokerApp1.getStub(transport2.getEndpoint(),10000).sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * RPCInvokerのサブクラスへのoneway RPC
     */
    @Test
    public void rpcToInvokerAppOneway() throws RPCException, InterruptedException {
        InvokerAppIf stub = invokerApp1.getStub(transport2.getEndpoint());
        stub.oneway();
        String r = resultQueue.peek();
        assertNull(r);
        r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("oneway",r);
    }
    
    /**
     * RPCInvokerのサブクラスのvoidメソッドへの同期型RPC
     */
    @Test
    public void callInvokerAppVoid() throws RPCException, InterruptedException {
        invokerApp1.getStub(transport2.getEndpoint()).syncRPC();
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * RPCInvokerのサブクラスのExceptionを発生するメソッドへのRPC
     */
    @Test
    public void callInvokerAppException() throws RPCException {
        try {
            invokerApp1.getStub(transport2.getEndpoint()).throwEx();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("throwEx"));
            return;
        }
        fail("expected Exception(\"throwEx\"");
    }
    
    /**
     * RPCInvokerのサブクラスのnullを返すメソッドへのRPC
     */
    @Test
    public void callInvokerAppNull() throws RPCException {
        Object o = invokerApp1.getStub(transport2.getEndpoint()).returnNull();
        assertNull(o);
    }
    
    /**
     * RPCInvokerのlocalメソッドへのRPC
     * IllegalRPCAccessExceptionが発生するはず
     */
    @Test
    public void callInvokerAppLocal() throws RPCException {
        try {
            invokerApp1.getStub(transport2.getEndpoint()).localMethod(10);
        } catch (Throwable e) {
            if (e instanceof IllegalRPCAccessException) {
                // 期待どうり
                return;
            }
            throw e;
        }
        fail("expected IlleaglRPCAccessException");
    }
    
    /**
     * RPCInvokerの最初の引数が配列のメソッドへのRPC
     */
    @Test
    public void callInvokerAppArray() throws RPCException {
        String[] x = {"A","B"};
        String s = invokerApp1.getStub(transport2.getEndpoint()).arrayArg(x);
        assertEquals("AB",s);
    }
    
    /**
     * RPCInvokerの引数が配列とスカラーが混じったメソッドへのRPC
     */
    @Test
    public void callInvokerAppArray2() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        String s = invokerApp1.getStub(transport2.getEndpoint()).arrayArg2(x,5,y);
        assertEquals("AB4CD",s);
    }
    
    /**
     * RPCInvokerの引数が可変長のメソッドへのRPC
     */
    @Test
    public void callInvokerAppVarArgs() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        String s = invokerApp1.getStub(transport2.getEndpoint()).varArgs(x,"C",y);
        assertEquals("ABcDE",s);
    }
    
    /**
     * RPCInvokerのサブクラスへのRPC
     * NetworkTimeoutExceptionが発生するはず
     * 10秒かかる
     */
    @Test
    public void callInvokerAppTimeout() throws RPCException {
        try {
            invokerApp1.getStub(transport2.getEndpoint()).longSleep(11000);
        } catch (RPCException e) {
            Throwable cause = e.getCause();
            if (cause instanceof NetworkTimeoutException) {
                // 期待どうり
                return;
            }
            throw e;
        }
        fail("expected NetworkTimeoutException");
    }

    /**
     * RPCInvokerのサブクラスへの通常のlocal call
     */
    @Test
    public void localCallInvokerAppNormal() throws RPCException {
        int sum = invokerApp1.getStub(transport1.getEndpoint()).sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * RPCInvokerのサブクラスへのoneway local call
     */
    @Test
    public void localCallToInvokerAppOneway() throws RPCException, InterruptedException {
        InvokerAppIf stub = invokerApp1.getStub(transport1.getEndpoint());
        stub.oneway();
        String r = resultQueue.peek();
        assertNull(r);
        r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("oneway",r);
    }
    
    /**
     * RPCInvokerのサブクラスのvoidメソッドへのlocal call
     */
    @Test
    public void localCallInvokerAppVoid() throws RPCException, InterruptedException {
        invokerApp1.getStub(transport1.getEndpoint()).syncRPC();
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * RPCInvokerのサブクラスのExceptionを発生するメソッドへのlocal call
     */
    @Test
    public void localCallInvokerAppException() throws RPCException {
        try {
            invokerApp1.getStub(transport1.getEndpoint()).throwEx();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("throwEx"));
            return;
        }
        fail("expected Exception(\"throwEx\"");
    }
    
    /**
     * RPCInvokerのサブクラスのnullを返すメソッドへのlocal call
     */
    @Test
    public void localCallInvokerAppNull() throws RPCException {
        Object o = invokerApp1.getStub(transport1.getEndpoint()).returnNull();
        assertNull(o);
    }
    
    /**
     * RPCInvokerのlocalメソッドへのlocal call
     */
    @Test
    public void localCallInvokerAppLocal() throws RPCException {
        invokerApp1.getStub(transport1.getEndpoint()).localMethod(10);
    }
    
    /**
     * RPCInvokerの最初の引数が配列のメソッドへのlocal call
     */
    @Test
    public void localCallInvokerAppArray() throws RPCException {
        String[] x = {"A","B"};
        String s = invokerApp1.getStub(transport1.getEndpoint()).arrayArg(x);
        assertEquals("AB",s);
    }
    
    /**
     * RPCInvokerの引数が配列とスカラーが混じったメソッドへのlocal call
     */
    @Test
    public void localCallInvokerAppArray2() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        String s = invokerApp1.getStub(transport1.getEndpoint()).arrayArg2(x,5,y);
        assertEquals("AB4CD",s);
    }
    
    /**
     * RPCInvokerの引数が可変長のメソッドへのlocal call
     */
    @Test
    public void localCallInvokerAppVarArgs() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        String s = invokerApp1.getStub(transport1.getEndpoint()).varArgs(x,"C",y);
        assertEquals("ABcDE",s);
    }
    
    /**
     * RPCInvokerのサブクラスへのlocal call
     */
    @Test
    public void localCallInvokerAppTimeout() throws RPCException {
        invokerApp1.getStub(transport1.getEndpoint()).longSleep(11000);
    }


    /**
     * 通常のオブジェクトへのRPC
     */
    @Test
    public void rpcToAppNormal() throws RPCException {
        SAppIf stub = invokerApp1.getStub(SAppIf.class,
                new ObjectId("app"),transport2.getEndpoint());
        int sum = stub.sum(10);
        assertEquals(56,sum);
    }
    
    /**
     * 通常のオブジェクトへのRPC
     * timeout付き
     */
    @Test
    public void rpcToAppNormalTimeout() throws RPCException {
        SAppIf stub = invokerApp1.getStub(SAppIf.class,
                new ObjectId("app"),transport2.getEndpoint(),10000);
        int sum = stub.sum(10);
        assertEquals(56,sum);
    }
    
    /**
     * 通常のオブジェクトへのRPC
     */
    @Test
    public void calAppNormal() throws RPCException {
        int sum = invokerApp1.getStub(SAppIf.class,
                new ObjectId("app"),transport2.getEndpoint()).sum(10);
        assertEquals(56,sum);
    }
    
    /**
     * 通常のオブジェクトへのRPC。
     * クラス指定
     * IllegalArgumentExceprtionが発生するはず
     */
    @Test(expected = IllegalArgumentException.class)
    public void calAppNormalWithClass() throws RPCException {
        int sum = invokerApp1.getStub(App.class,
                new ObjectId("app"),transport2.getEndpoint()).sum(10);
        assertEquals(56,sum);
    }
    
    /**
     * 通常のオブジェクトへのRPC
     * timeout付き
     */
    @Test
    public void callAppNormalTimeout() throws RPCException {
        int sum = invokerApp1.getStub(SAppIf.class,
                new ObjectId("app"),transport2.getEndpoint(),10000).sum(10);
        assertEquals(56,sum);
    }
    
    /**
     * 通常のオブジェクトへのlocal call
     */
    @Test
    public void localCallAppNormal() throws RPCException {
        int sum = invokerApp1.getStub(SAppIf.class,
                new ObjectId("app"),transport1.getEndpoint()).sum(10);
        assertEquals(56,sum);
    }
    
    /**
     * RemoteRefを用いた通常のオブジェクトへのRPC
     */
    @Test
    public void callAppNormalWithRemoteRef() throws RPCException {
        CalleeId ref = new CalleeId(new ObjectId("app"),
                transport2.getEndpoint(),null);
        int sum = invokerApp1.getStub(SAppIf.class,ref).sum(10);
        assertEquals(56,sum);
    }
    
    /**
     * Dynamic RPC
     */
    @Test
    public void callAppDynamic() throws Throwable {
        Object sum = invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),"sum",10);
        assertTrue(sum instanceof Integer);
        assertEquals(56,((Integer)sum).intValue());
    }
    
    /**
     * 引数なしメソッドへのDynamic RPC
     */
    @Test
    public void callAppNoArgDynamic() throws Throwable {
        invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),"syncRPC");
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * 最初の引数が配列のメソッドへのDynamic RPC
     */
    @Test
    public void callAppArrayDynamic() throws Throwable {
        String[] x = {"A","B"};
        Object s = invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),"arrayArg",(Object)x);
        assertTrue(s instanceof String);
        assertEquals("AB",s);
    }
    
    /**
     * 配列とスカラーが混じったメソッドへのDynamic RPC
     */
    @Test
    public void callAppArray2Dynamic() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        Object s = invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),"arrayArg2",x,5,y);
        assertTrue(s instanceof String);
        assertEquals("AB4CD",s);
    }
    
    /**
     * 引数が可変長のメソッドへのDynamic RPC
     */
    @Test
    public void callAppVarArgsDynamic() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        Object s = invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),"varArgs",x,"C",y);
        assertTrue(s instanceof String);
        assertEquals("ABcDE",s);
    }
    
    /**
     * oneway Dynamic RPC
     */
    @Test
    public void callAppOnewayDynamic() throws Throwable {
        invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),RPCMode.ONEWAY,"oneway");
        String r = resultQueue.peek();
        assertNull(r);
        r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("oneway",r);
    }
    
    /**
     * local methodへのDynamic RPC
     * IllegalRPCAccessExceptionが発生するはず
     */
    @Test
    public void cllAppLocalDynamic() {
        try {
            invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),"localMethod",10);
        } catch (Throwable e) {
            if (e instanceof IllegalRPCAccessException) {
                // 期待どうり
                return;
            }
        }
        fail("expected IlleaglRPCAccessException");
    }
    
    /**
     * local methodへのoneway Dynamic RPC
     * IllegalRPCAccessExceptionが発生するはず
     */
    @Test(expected = IllegalRPCAccessException.class)
    public void callAppLocalOnewayDynamic() throws Throwable {
        invokerApp1.rcall(new ObjectId("app"),
                transport2.getEndpoint(),"localOneway");
        String r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertNull(r);
    }
    
    /**
     * local dynamic call
     */
    @Test
    public void localCallAppDynamic() throws Throwable {
        Object sum = invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),"sum",10);
        assertTrue(sum instanceof Integer);
        assertEquals(56,((Integer)sum).intValue());
    }
    
    /**
     * 引数なしメソッドへのlocal dynamic call
     */
    @Test
    public void localCallAppNoArgDynamic() throws Throwable {
        invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),"syncRPC");
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * 最初の引数が配列のメソッドへのlocal dynamic call
     */
    @Test
    public void localCallAppArrayDynamic() throws Throwable {
        String[] x = {"A","B"};
        Object s = invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),"arrayArg",(Object)x);
        assertTrue(s instanceof String);
        assertEquals("AB",s);
    }
    
    /**
     * 配列とスカラーが混じったメソッドへのlocal dynamic call
     */
    @Test
    public void localCallAppArray2Dynamic() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        Object s = invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),"arrayArg2",x,5,y);
        assertTrue(s instanceof String);
        assertEquals("AB4CD",s);
    }
    
    /**
     * 引数が可変長のメソッドへのlocal dynamic call
     */
    @Test
    public void localCallAppVarArgsDynamic() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        Object s = invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),"varArgs",x,"C",y);
        assertTrue(s instanceof String);
        assertEquals("ABcDE",s);
    }
    
    /**
     * oneway local dynamic call
     */
    @Test
    public void localCallAppOnewayDynamic() throws Throwable {
        invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),RPCMode.ONEWAY,"localOneway");
        String r = resultQueue.peek();
        assertNull(r);
        r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("localOneway",r);
    }
    
    /**
     * local methodへのlocal dynamic call
     */
    @Test
    public void localCallAppLocalDynamic() throws Throwable {
        Object sum = invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),"localMethod",10);
        assertTrue(sum instanceof Integer);
        assertEquals(56,((Integer)sum).intValue());
    }
    
    /**
     * local methodへのlocal oneway dynamic call
     */
    @Test
    public void localCallAppLocalOnewayDynamic() throws Throwable {
        invokerApp1.rcall(new ObjectId("app"),
                transport1.getEndpoint(),RPCMode.ONEWAY,"localMethod",10);
    }
    
    /**
     * oneway宣言なしのvoidメソッドへのoneway RPC
     */
    @Test
    public void callAppVoidOneway1() throws RPCException, InterruptedException {
        invokerApp1.getStub(SAppIf.class,new ObjectId("app"),
                transport2.getEndpoint(),RPCMode.ONEWAY).syncRPC();
        String r = resultQueue.poll();
        assertNull(r);
        r = resultQueue.take();
        assertEquals("syncRPC",r);
    }
    
    /**
     * oneway宣言なしのvoidメソッドへのoneway RPC.
     * クラス指定。
     * IllegalArgumentExceptionが発生するはず。
     */
    @Test(expected = IllegalArgumentException.class)
    public void callAppVoidOneway1WithClass() throws RPCException, InterruptedException {
        invokerApp1.getStub(App.class,new ObjectId("app"),
                transport2.getEndpoint(),RPCMode.ONEWAY).syncRPC();
        String r = resultQueue.poll();
        assertNull(r);
        r = resultQueue.take();
        assertEquals("syncRPC",r);
    }
    
    /**
     * oneway宣言なしのvoidメソッドへのoneway RPC。
     * RemoteRef指定
     */
    @Test
    public void callAppVoidOneway2() throws RPCException, InterruptedException {
        CalleeId ref = new CalleeId(
                new ObjectId("app"),transport2.getEndpoint(),null);
        invokerApp1.getStub(SAppIf.class,ref,RPCMode.ONEWAY).syncRPC();
        String r = resultQueue.poll();
        assertNull(r);
        r = resultQueue.take();
        assertEquals("syncRPC",r);
    }
    
    @Test
    public void callbackTest() {
        int sum = invokerApp1.getStub(transport2.getEndpoint()).callback((PeerLocator)transport1.getEndpoint());
        assertEquals(56, sum);
    }
    
    /**
     * 後始末
     * RPCInvokerのサブクラスの終了処理
     * ピアの終了処理
     */
    @AfterClass
    public static void fin() {
        invokerApp1.fin();
        invokerApp2.fin();
        peer1.fin();
        peer2.fin();
        logger.debug("- end -%n");
    }
    
    /*
     * テストメソッドを個別に動作させたい場合に
     * 書き換えて実行する。
     */
    public static void main(String[] args) {
        TestRPC o = new TestRPC();
        
        try {
            setup();
            
            o.localCallAppLocalOnewayDynamic();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            fin();
        }
    }
}
