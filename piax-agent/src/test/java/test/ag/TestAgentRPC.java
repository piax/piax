package test.ag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.piax.agent.Agent;
import org.piax.agent.AgentException;
import org.piax.agent.AgentId;
import org.piax.agent.AgentIf;
import org.piax.agent.AgentPeer;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.CalleeId;
import org.piax.gtrans.IllegalRPCAccessException;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCMode;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;

import test.Util;

public class TestAgentRPC extends Util {
    
    static ArrayBlockingQueue<String> resultQueue = new ArrayBlockingQueue<String>(1);
    
    public interface AppIf extends AgentIf {
        @RemoteCallable
        int sum(int n) throws RPCException;
        
        @RemoteCallable
        void syncRPC() throws RPCException;;;
        
        @RemoteCallable
        String arrayArg(String[] a) throws RPCException;;;
        
        @RemoteCallable
        String arrayArg2(String[] a, int n, String[] b) throws RPCException;;;
        
        @RemoteCallable
        String varArgs(String[] a, String b, String...c) throws RPCException;;;
        
        @RemoteCallable(Type.ONEWAY)
        void oneway() throws RPCException;;;
        
        void localOneway();
        
        int localMethod(int n);
        
        @RemoteCallable
        void longSleep(int stime) throws RPCException;
        
        @RemoteCallable
        int throwEx() throws Exception;
        
        @RemoteCallable
        Object returnNull() throws RPCException;
    }
    
    public static class App extends Agent implements AppIf {
        
        public App() {}

        public void longSleep(int stime) {
            System.out.println("I will sleep now: " + stime);
            sleep(stime);
        }

        @Override
        public int sum(int n) {
            int sum = 0;
            for (int i = 1; i <= n; i++) {
                sum += i;
            }
            return sum;
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
        @RemoteCallable
        public int throwEx() throws Exception {
            throw new Exception("throwEx");
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
        public Object returnNull() {
            return null;
        }

       @Override
        public int localMethod(int n) {
            return sum(n);
        }

    }
    
    static private AgentPeer localAgentPeer;
    static private AgentId localAgentId;
    static private String localPeerName = "localAgentPeer";
    static private String localAgentName = "localAgentName";
    static private AgentPeer remoteAgentPeer;
    static private AgentId remoteAgentId;
    static private String remotePeerName = "remoteAgentPeer";
    static private String remoteAgentName = "remoteAgentName";
    static private Agent localAgent;
    static private Agent remoteAgent;
    
    /**
     * 前準備
     * @throws Exception 
     */
    @BeforeClass
    public static void setup() throws Exception {
        Net ntype = Net.TCP;

        // AgentPeerを作成
        localAgentPeer = new AgentPeer(localPeerName,genLocator(ntype, "localhost", 10001),
                    genLocator(ntype, "localhost", 10001));
        remoteAgentPeer = new AgentPeer(remotePeerName,genLocator(ntype, "localhost", 10002),
                    genLocator(ntype, "localhost", 10001));
        // createAgent
        localAgentId = localAgentPeer.createAgent(App.class,localAgentName);
        remoteAgentId = remoteAgentPeer.createAgent(App.class, remoteAgentName);
        localAgentPeer.join();
        remoteAgentPeer.join();
        // Agentへの参照を取得。(通常のユーザは行ってはいけない裏ワザ)
        AgentHomeImpl localHome = (AgentHomeImpl)localAgentPeer.getHome();
        localAgent = localHome.getAgentContainer(localAgentId).getAgent();
        AgentHomeImpl remoteHome = (AgentHomeImpl)remoteAgentPeer.getHome();
        remoteAgent = remoteHome.getAgentContainer(remoteAgentId).getAgent();
    }
    
    /**
     * 通常のRPC
     * @throws RPCException
     */
    @Test
    public void rpcToAppNormal() throws RPCException {
        int sum = localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * PeerAgent#submitを使用したRPC
     * @throws RPCException
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    @Test
    public void rpcToAppAsync() throws RPCException, InterruptedException, ExecutionException {
        Future<Integer> f = localAgentPeer.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws RPCException {
                return localAgent.getStub(AppIf.class,remoteAgentId,
                        remoteAgentPeer.getPeerId()).sum(10);       
            }
        });
        int sum = f.get();
        assertEquals(55,sum);
    }
    
    /**
     * oneway RPC
     * @throws RPCException
     * @throws InterruptedException 
     */
    @Test
    public void rpcToAppOneway() throws RPCException, InterruptedException {
        localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId(),RPCMode.ONEWAY).oneway();
        String r = resultQueue.peek();
        assertNull(r);
        r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("oneway",r);
    }
    
    /**
     * voidメソッドへの同期型RPC
     * @throws RPCException
     * @throws InterruptedException 
     */
    @Test
    public void rpcToAppVoid() throws RPCException, InterruptedException {
        localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).syncRPC();
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * AgentPeer#submitを使用したvoidメソッドへの同期型RPC
     * @throws RPCException
     * @throws InterruptedException 
     * @throws ExecutionException 
     */
    @Test
    public void rpcToAppVoidAsync() throws RPCException, InterruptedException,
            ExecutionException {
        Future<Void> f = localAgentPeer.submit(new Callable<Void>() {
           @Override
           public Void call() throws RPCException {
               localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).syncRPC();
               return null;
           }
        });
        String r = resultQueue.poll();
        assertNull(r);
        f.get();
        r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * oneway指定なしのvoidメソッドへのoneway local call
     * 
     * @throws RPCException
     * @throws InterruptedException 
     * @throws AgentException 
     */
    @Test
    public void rpcToAppVoidOneway1() throws RPCException,
            InterruptedException, AgentException {
        //MSkipGraphを使用していると、local呼び出しは同期呼び出しになる。
        localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId(),RPCMode.ONEWAY).syncRPC();
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * oneway指定なしのvoidメソッドへのoneway RPC
     * @throws RPCException
     * @throws InterruptedException 
     */
    @Test
    public void rpcToAppVoidOneway2() throws RPCException, InterruptedException {
        localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId(),RPCMode.ONEWAY).syncRPC();
        String r = resultQueue.poll();
        assertNull(r);
        r = resultQueue.take();
        assertEquals("syncRPC",r);
    }
    
    /**
     * oneway指定なしのvoidメソッドへのoneway RPC
     * CalleeIdを用いる。
     * @throws RPCException
     * @throws InterruptedException
     */
    @Test
    public void rpcToAppVoidOneway3() throws RPCException, InterruptedException {
        localAgent.getStub(AppIf.class,remoteAgent.getCalleeId(),
                RPCMode.ONEWAY).syncRPC();
        String r = resultQueue.poll();
        assertNull(r);
        r = resultQueue.take();
        assertEquals("syncRPC",r);
    }
    
    /**
     * Exceptionを発生するメソッドへのRPC
     * @throws RPCException
     */
    @Test
    public void rpcToAppException() throws RPCException {
        try {
            localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).throwEx();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("throwEx"));
            return;
        }
        fail("expected Exception(\"throwEx\"");
    }
    
    /**
     * nullを返すメソッドへのRPC
     * @throws RPCException
     */
    @Test
    public void rpcToAppNull() throws RPCException {
        Object o = localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).returnNull();
        assertNull(o);
    }
    
    /**
     * localメソッドへのRPC
     * IllegalRPCAccessExceptionが発生するはず
     * @throws RPCException
     */
    @Test
    public void rpcToAppLocal() throws RPCException {
        try {
            localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).localMethod(10);
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
     * 最初の引数が配列のメソッドへのRPC
     * @throws RPCException
     */
    @Test
    public void rpcToAppArray() throws RPCException {
        String[] x = {"A","B"};
        String s = localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).arrayArg(x);
        assertEquals("AB",s);
    }
    
    /**
     * 引数が配列とスカラーが混じったメソッドへのRPC
     * @throws RPCException
     */
    @Test
    public void rpcToAppArray2() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        String s = localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).arrayArg2(x,5,y);
        assertEquals("AB4CD",s);
    }
    
    /**
     * 引数が可変長のメソッドへのRPC
     * @throws RPCException
     */
    @Test
    public void rpcToAppVarArgs() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        String s = localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).varArgs(x,"C",y);
        assertEquals("ABcDE",s);
    }
    
    /**
     * RPC
     * NetworkTimeoutExceptionが発生するはず
     * 10秒かかる
     * @throws RPCException
     */
    @Test
    public void rpcToAppTimeout() throws RPCException {
        try {
            localAgent.getStub(AppIf.class,remoteAgentId,
                remoteAgentPeer.getPeerId()).longSleep(11000);
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
     * 通常のlocal call
     * @throws RPCException
     */
    @Test
    public void localCallToAppNormal() throws RPCException {
        int sum = localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * oneway local call
     * @throws RPCException
     * @throws InterruptedException 
     */
    @Test
    public void localCallToAppOneway() throws RPCException, InterruptedException {
        //MSkipGraphを使用していると、local呼び出しは同期呼び出しになる。
        localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).oneway();
        String r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("oneway",r);
    }
    
    /**
     * voidメソッドへのlocal call
     * @throws RPCException
     * @throws InterruptedException 
     */
    @Test
    public void localCallToAppVoid() throws RPCException, InterruptedException {
        localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).syncRPC();
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * Exceptionを発生するメソッドへのlocal call
     * @throws RPCException
     */
    @Test
    public void localCallToAppException() throws RPCException {
        try {
            localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).throwEx();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("throwEx"));
            return;
        }
        fail("expected Exception(\"throwEx\"");
    }
    
    /**
     * nullを返すメソッドへのlocal call
     * @throws RPCException
     */
    @Test
    public void localCallToAppNull() throws RPCException {
        Object o = localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).returnNull();
        assertNull(o);
    }
    
    /**
     * localメソッドへのlocal call
     * @throws RPCException
     */
    @Test
    public void localCallToAppLocal() throws RPCException {
        localAgent.getStub(AppIf.class,localAgentId,
            localAgentPeer.getPeerId()).localMethod(10);
    }
    
    /**
     * 最初の引数が配列のメソッドへのlocal call
     * @throws RPCException
     */
    @Test
    public void localCallToAppArray() throws RPCException {
        String[] x = {"A","B"};
        String s = localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).arrayArg(x);
        assertEquals("AB",s);
    }
    
    /**
     * 引数が配列とスカラーが混じったメソッドへのlocal call
     * @throws RPCException
     */
    @Test
    public void localCallToAppArray2() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        String s = localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).arrayArg2(x,5,y);
        assertEquals("AB4CD",s);
    }
    
    /**
     * 引数が可変長のメソッドへのlocal call
     * @throws RPCException
     */
    @Test
    public void localCallToAppVarArgs() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        String s = localAgent.getStub(AppIf.class,localAgentId,
             localAgentPeer.getPeerId()).varArgs(x,"C",y);
        assertEquals("ABcDE",s);
    }
    
    /**
     * local call
     * @throws RPCException
     */
    @Test
    public void localCallToAppTimeout() throws RPCException {
        localAgent.getStub(AppIf.class,localAgentId,
                localAgentPeer.getPeerId()).longSleep(11000);
    }


    /**
     * CalleeIdを用いたRPC
     * @throws RPCException
     */
    @Test
    public void rpcToAppNormalWithCalleeId() throws RPCException {
        CalleeId ref = remoteAgent.getCalleeId();
        int sum = localAgent.getStub(AppIf.class,ref).sum(10);
        assertEquals(55,sum);
    }
    
    /**
     * DynamicStubを用いたRPC
     * @throws Throwable 
     */
    @Test
    public void rpcToAppWithDynamicStub() throws Throwable {
        Object sum = localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),"sum",10);
        assertTrue(sum instanceof Integer);
        assertEquals(55,((Integer)sum).intValue());
    }
    
    /**
     * DynamicStubを用いた引数なしメソッドへのRPC
     * @throws Throwable 
     */
    @Test
    public void rpcToAppNoArgWithDynamicStub() throws Throwable {
        localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),"syncRPC");
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * DynamicStubを用いた最初の引数が配列のメソッドへのRPC
     * @throws Throwable 
     */
    @Test
    public void rpcToAppArrayWithDynamicStub() throws Throwable {
        String[] x = {"A","B"};
        Object s = localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),"arrayArg",(Object)x);
        assertTrue(s instanceof String);
        assertEquals("AB",s);
    }
    
    /**
     * DynamicStubを用いた配列とスカラーが混じったメソッドへのRPC
     * @throws Throwable 
     */
    @Test
    public void rpcToAppArray2WithDynamicStub() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        Object s = localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),"arrayArg2",x,5,y);
        assertTrue(s instanceof String);
        assertEquals("AB4CD",s);
    }
    
    /**
     * DynamicStubを用いた引数が可変長のメソッドへのRPC
     * @throws Throwable 
     */
    @Test
    public void rpcToAppVarArgsWithDynamicStub() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        Object s = localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),"varArgs",x,"C",y);
        assertTrue(s instanceof String);
        assertEquals("ABcDE",s);
    }
    
    /**
     * DynamicStubを用いたoneway RPC
     * @throws Throwable 
     */
    @Test
    public void rpcToAppOnewayWithDynamicStub() throws Throwable {
        localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),RPCMode.ONEWAY,"oneway");
        String r = resultQueue.peek();
        assertNull(r);
        r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("oneway",r);
    }
    
    /**
     * DynamicStubを用いたlocal methodへのRPC
     * IllegalRPCAccessExceptionが発生するはず
     * @throws Throwable 
     */
    @Test
    public void rpcToAppLocalWithDynamicStub() throws Throwable {
        try {
            localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),"localMethod",10);
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
     * DynamicStubを用いたlocal methodへのonewayRPC
     * IllegalRPCAccessExceptionが発生するはず
     * @throws Throwable 
     */
    @Test(expected = IllegalRPCAccessException.class)
    public void rpcToAppLocalOnewayWithDynamicStub() throws Throwable {
        localAgent.rcall(remoteAgentId,
                remoteAgentPeer.getPeerId(),"localOneway");
    }
    
    /**
     * DynamicStubを用いたlocal call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppWithDynamicStub() throws Throwable {
        Object sum = localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),"sum",10);
        assertTrue(sum instanceof Integer);
        assertEquals(55,((Integer)sum).intValue());
    }
    
    /**
     * DynamicStubを用いた引数なしメソッドへのlocal call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppNoArgWithDynamicStub() throws Throwable {
        localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),"syncRPC");
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * DynamicStubを用いた最初の引数が配列のメソッドへのlocal call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppArrayWithDynamicStub() throws Throwable {
        String[] x = {"A","B"};
        Object s = localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),"arrayArg",(Object)x);
        assertTrue(s instanceof String);
        assertEquals("AB",s);
    }
    
    /**
     * DynamicStubを用いた配列とスカラーが混じったメソッドへのlocal call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppArray2WithDynamicStub() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        Object s = localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),"arrayArg2",x,5,y);
        assertTrue(s instanceof String);
        assertEquals("AB4CD",s);
    }
    
    /**
     * DynamicStubを用いた引数が可変長のメソッドへのlocal call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppVarArgsWithDynamicStub() throws Throwable {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        Object s = localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),"varArgs",x,"C",y);
        assertTrue(s instanceof String);
        assertEquals("ABcDE",s);
    }
    
    /**
     * DynamicStubを用いたoneway local call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppOnewayWithDynamicStub() throws Throwable {
        //MSkipGraphを使用していると、local呼び出しは同期呼び出しになる。
        localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),RPCMode.ONEWAY,"localOneway");
        String r = resultQueue.poll(11000,TimeUnit.MILLISECONDS);
        assertEquals("localOneway",r);
    }
    
    /**
     * DynamicStubを用いたlocal methodへのlocal call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppLocalWithDynamicStub() throws Throwable {
        Object sum = localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),"localMethod",10);
        assertTrue(sum instanceof Integer);
        assertEquals(55,((Integer)sum).intValue());
    }
    
    /**
     * DynamicStubを用いたlocal methodへのlocal oneway call
     * @throws Throwable 
     */
    @Test
    public void localCallToAppLocalOnewayWithDynamicStub() throws Throwable {
        localAgent.rcall(localAgentId,
                localAgentPeer.getPeerId(),RPCMode.ONEWAY,"localMethod",10);
    }
    
    /**
     * 後始末
     * RPCInvokerのサブクラスの終了処理
     * ピアの終了処理
     */
    @AfterClass
    public static void fin() throws Exception {
        localAgentPeer.fin();
        remoteAgentPeer.fin();
    }
    
    /*
     * テストメソッドを個別に動作させたい場合に
     * 書き換えて実行する。
     */
    public static void main(String[] args) {
        TestAgentRPC o = new TestAgentRPC();
        
        try {
            setup();
            o.rpcToAppVoidOneway1();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                fin();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
