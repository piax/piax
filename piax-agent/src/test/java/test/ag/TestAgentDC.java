package test.ag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.piax.agent.Agent;
import org.piax.agent.AgentId;
import org.piax.agent.AgentIf;
import org.piax.agent.AgentPeer;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.Endpoint;
import org.piax.common.Location;
import org.piax.common.dcl.parser.ParseException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IllegalRPCAccessException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCMode;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;
import org.piax.gtrans.RemoteValue;

import test.Util;

public class TestAgentDC extends Util {
    
    static ArrayBlockingQueue<String> resultQueue = new ArrayBlockingQueue<String>(1);
    
    public interface BaseIf extends AgentIf {
        @RemoteCallable
        String getClassName() throws RPCException;
    }
    
    public interface AppIf extends BaseIf {
        @RemoteCallable
        String hello(String you) throws RPCException;
        
        @RemoteCallable
        int sum(int n);
        
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
        
        String localMethod(String you);
        
        @RemoteCallable
        void longSleep(int stime) throws RPCException;
        
        @RemoteCallable
        int throwEx() throws Exception;
        
        @RemoteCallable
        Object returnNull() throws RPCException;
    }
    
    public static class App extends Agent implements AppIf {
        
        public App() {}
        
        @Override
        public String getClassName() {
            return getClass().getName();
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
        public void longSleep(int stime) {
            System.out.println("I will sleep now: " + stime);
            sleep(stime);
        }

        @Override
        public String hello(String you) {
            return "Hi,"+you+"."+"I'm "+getName();
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
        public String localMethod(String you) {
            return hello(you);
        }

    }
    
    public static class AnotherAgent extends Agent implements BaseIf {
        @Override
        public String getClassName() {
            return getClass().getName();
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
        AgentId anotherAgentId = remoteAgentPeer.createAgent(AnotherAgent.class);
        localAgentPeer.join();
        remoteAgentPeer.join();
        // Agentへの参照を取得。(通常のユーザは行ってはいけない裏ワザ)
        AgentHomeImpl localHome = (AgentHomeImpl)localAgentPeer.getHome();
        localAgent = localHome.getAgentContainer(localAgentId).getAgent();
        AgentHomeImpl remoteHome = (AgentHomeImpl)remoteAgentPeer.getHome();
        remoteAgent = remoteHome.getAgentContainer(remoteAgentId).getAgent();
        Agent anotherAgent = remoteHome.getAgentContainer(anotherAgentId).getAgent();
        String locationName = "$location";
        localAgentPeer.declareAttrib(locationName, Location.class);
        localAgentPeer.bindOverlay(locationName, "LLNET");
        localAgent.setAttrib(locationName, new Location(0,0));
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.bindOverlay(locationName, "LLNET");
        remoteAgent.setAttrib(locationName, new Location(2,2));
        anotherAgent.setAttrib(locationName, new Location(2,2));
    }
    
    /**
     * 通常のgetDCStub
     * @throws RPCException
     */
    @Test
    public void rpcToAppNormal() throws RPCException {
        List<String> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                        AppIf.class).hello(localAgentName));
        assertEquals(1,r.size());
        assertEquals("Hi,"+localAgentName+".I'm "+remoteAgentName,
                r.get(0));
    }
    
    /**
     * 通常のgetDCStub。
     * class指定。
     * IllegalArgumentExceptionが発生するはず。
     * @throws RPCException
     */
    @Test(expected = IllegalArgumentException.class)
    public void rpcToAppNormalWithClass() throws RPCException {
        localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                        App.class).hello(localAgentName));
    }
    
    /**
     * 通常のgetDCStub。
     * DCLエラー
     * RPCExceptionが発生するはず。
     * @throws RPCException
     */
    @Test
    public void rpcToAppNormalDCLError() {
        try {
            localAgent.getList(
                    localAgent.getDCStub("$location inrect(2,2,1,1)",
                            AppIf.class).hello(localAgentName));
        } catch (RPCException e) {
            // 期待どうり
            Throwable th = e.getCause();
            assertTrue(th instanceof ParseException);
        }
    }
    
    /**
     * 通常のgetDCStubAsync
     * @throws RPCException
     * @throws InvocationTargetException 
     */
    @Test
    public void rpcToAppAsync() throws RPCException, InvocationTargetException {
        FutureQueue<String> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                        AppIf.class,RPCMode.ASYNC).hello(localAgentName));
        int n = 0;
        for (RemoteValue<String> rv:r) {
            String v = rv.getValue();
            assertEquals("Hi,"+localAgentName+".I'm "+remoteAgentName,
                v);
            assertNull(rv.getException());
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * 通常のgetDCStubOneway
     * @throws RPCException
     * @throws InvocationTargetException 
     * @throws InterruptedException 
     */
    @Test
    public void rpcToAppOneway() throws RPCException, InvocationTargetException,
            InterruptedException {
        localAgent.getDCStub(
            "$location in rect(2,2,1,1)",AppIf.class).oneway();
        String r = resultQueue.peek();
        assertNull(r);
        r = resultQueue.take();
        assertEquals("oneway",r);
    }
    
    /**
     * onewayメソッドの同期呼び出し
     * @throws RPCException
     * @throws InvocationTargetException 
     * @throws InterruptedException 
     */
    @Test
    public void rpcToAppOnewaySync() throws RPCException, InvocationTargetException,
            InterruptedException {
        localAgent.getDCStub(
            "$location in rect(2,2,1,1)",AppIf.class,RPCMode.SYNC).oneway();
        String r = resultQueue.peek();
        assertEquals("oneway",r);
    }
    
    /**
     * voidメソッドへのgetDCStub
     * @throws RPCException
     * @throws InterruptedException 
     */
    @Test
    public void rpcToAppVoid() throws RPCException, InterruptedException {
        localAgent.getDCStub("$location in rect(2,2,1,1)", AppIf.class)
                .syncRPC();
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * voidメソッドへのgetDCStubAsync
     * @throws RPCException
     * @throws InterruptedException 
     * @throws InvocationTargetException 
     */
    @Test
    public void rpcToAppVoidAsync() throws RPCException, InterruptedException,
            InvocationTargetException {
        localAgent.getDCStub("$location in rect(2,2,1,1)", AppIf.class,RPCMode.ASYNC)
                .syncRPC();
        FutureQueue<Object> fq = localAgent.getFQ();
        int n = 0;
        for (RemoteValue<Object> rv:fq) {
            Object v = rv.get();
            assertNull(v);
            assertNull(rv.getException());
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
        String r = resultQueue.poll();
        assertEquals("syncRPC",r);
    }
    
    /**
     * Exceptionを発生するメソッドへのgetDCStub
     * 答えが返らないだけのはず
     * @throws Exception 
     */
    @Test
    public void rpcToAppException() throws Exception {
        List<Integer> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class).throwEx());
        assertEquals(0,r.size());
    }
    
    /**
     * Exceptionを発生するメソッドへのgetDCStubAsync
     * @throws Exception 
     */
    @Test
    public void rpcToAppAsyncException() throws Exception {
        FutureQueue<Integer> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class,RPCMode.ASYNC).throwEx());
        int n = 0;
        for (RemoteValue<Integer> rv:r) {
            Integer v = rv.getValue();
            assertNull(v);
            Throwable th = rv.getException();
            assertTrue(th instanceof Exception);
            assertEquals("throwEx",th.getMessage());
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * nullを返すメソッドへのgetDCStub
     * @throws RPCException
     */
    @Test
    public void rpcToAppNull() throws RPCException {
        List<Object> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class).returnNull());
        assertEquals(1,r.size());
        assertNull(r.get(0));
    }
    
    /**
     * nullを返すメソッドへのgetDCStubAsync
     * @throws RPCException
     */
    @Test
    public void rpcToAppNullAsync() throws RPCException {
        FutureQueue<Object> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class,RPCMode.ASYNC).returnNull());
        int n = 0;
        for (RemoteValue<Object> rv:r) {
            Object v = rv.getValue();
            assertNull(v);
            assertNull(rv.getException());
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * localメソッドへのgetDCStub
     * 答えが返らないだけのはず
     * @throws RPCException
     */
    @Test
    public void rpcToAppLocal() throws RPCException {
        List<String> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class).localMethod(localAgentName));
        assertEquals(0,r.size());
    }
    
    /**
     * localメソッドへのgetDCStubAsync
     * @throws RPCException
     */
    @Test
    public void rpcToAppLocalAsync() throws RPCException {
        FutureQueue<String> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class,RPCMode.ASYNC).localMethod(localAgentName));
        int n = 0;
        for (RemoteValue<String> rv:r) {
            String v = rv.getValue();
            assertNull(v);
            Throwable th = rv.getException();
            assertTrue(th instanceof IllegalRPCAccessException);
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * localメソッドへのlocal getDCStub
     * @throws RPCException
     */
    @Test
    public void localCallToAppLocal() throws RPCException {
        List<String> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(0,0,1,1)",
                    AppIf.class).localMethod(localAgentName));
        assertEquals(1,r.size());
        assertEquals("Hi,"+localAgentName+".I'm "+localAgentName,
            r.get(0));
    }
    
    /**
     * localメソッドへのlocal getDCStubAsync
     * @throws RPCException
     */
    @Ignore
    @Test
    public void localCallToAppLocalAsync() throws RPCException {
        FutureQueue<String> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(0,0,1,1)",
                    AppIf.class, RPCMode.ASYNC).localMethod(localAgentName));
        int n = 0;
        for (RemoteValue<String> rv:r) {
            String v = rv.getValue();
            assertEquals("Hi,"+localAgentName+".I'm "+localAgentName,v);
            Throwable th = rv.getException();
            assertNull(th);
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * 最初の引数が配列のメソッドへのgetDCStub
     * @throws RPCException
     */
    @Test
    public void rpcToAppArray() throws RPCException {
        String[] x = {"A","B"};
        List<String> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class).arrayArg(x));
        assertEquals(1,r.size());
        assertEquals("AB",r.get(0));
    }
    
    /**
     * 最初の引数が配列のメソッドへのgetDCStubAsync
     * @throws RPCException
     */
    @Test
    public void rpcToAppArrayAsync() throws RPCException {
        String[] x = {"A","B"};
        FutureQueue<String> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class,RPCMode.ASYNC).arrayArg(x));
        int n = 0;
        for (RemoteValue<String> rv:r) {
            String v = rv.getValue();
            assertEquals("AB",v);
            Throwable th = rv.getException();
            assertNull(th);
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * 引数が配列とスカラーが混じったメソッドへのgetDCStub
     * @throws RPCException
     */
    @Test
    public void rpcToAppArray2() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        List<String> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class).arrayArg2(x,5,y));
        assertEquals(1,r.size());
        assertEquals("AB4CD",r.get(0));
    }
    
    /**
     * 引数が配列とスカラーが混じったメソッドへのgetDCStubAsync
     * @throws RPCException
     */
    @Test
    public void rpcToAppArray2Async() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"C","D"};
        FutureQueue<String> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class, RPCMode.ASYNC).arrayArg2(x,5,y));
        int n = 0;
        for (RemoteValue<String> rv:r) {
            String v = rv.getValue();
            assertEquals("AB4CD",v);
            Throwable th = rv.getException();
            assertNull(th);
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * 引数が可変長のメソッドへのgetDCStub
     * @throws RPCException
     */
    @Test
    public void rpcToAppVarArgs() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        List<String> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class).varArgs(x,"C",y));
        assertEquals(1,r.size());
        assertEquals("ABcDE",r.get(0));
    }
    
    /**
     * 引数が可変長のメソッドへのgetDCStubAsync
     * @throws RPCException
     */
    @Test
    public void rpcToAppVarArgsAsync() throws RPCException {
        String[] x = {"A","B"};
        String[] y = {"D","E"};
        FutureQueue<String> r = localAgent.getFQ(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class,RPCMode.ASYNC).varArgs(x,"C",y));
        int n = 0;
        for (RemoteValue<String> rv:r) {
            String v = rv.getValue();
            assertEquals("ABcDE",v);
            Throwable th = rv.getException();
            assertNull(th);
            Endpoint ep = rv.getPeer();
            assertEquals(remoteAgentPeer.getPeerId(),ep);
            n++;
        }
        assertEquals(1,n);
    }
    
    /**
     * 違うクラスのエージェントが呼ばれないか
     * @throws RPCException
     */
    @Test
    public void checkDiffClass() throws RPCException {
        List<String> r = localAgent.getList(
                localAgent.getDCStub("$location in rect(2,2,1,1)",
                    AppIf.class).getClassName());
        for (String s : r) {
            assertEquals(s,App.class.getName());
        }
    }
    
    /**
     * 後始末
     * RPCInvokerのサブクラスの終了処理
     * ピアの終了処理
     */
    @AfterClass
    public static void fin() throws Exception {
        localAgentPeer.leave();
        remoteAgentPeer.leave();
        localAgentPeer.fin();
        remoteAgentPeer.fin();
    }
    
    @Before
    public void clearQueue() {
        resultQueue.clear();
    }
    
    /*
     * テストメソッドを個別に動作させたい場合に
     * 書き換えて実行する。
     */
    public static void main(String[] args) {
        TestAgentDC o = new TestAgentDC();
        
        try {
            setup();
            o.rpcToAppOneway();
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
