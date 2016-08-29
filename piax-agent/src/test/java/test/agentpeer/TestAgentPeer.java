package test.agentpeer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.piax.agent.Agent;
import org.piax.agent.AgentAccessDeniedException;
import org.piax.agent.AgentCapabilityException;
import org.piax.agent.AgentException;
import org.piax.agent.AgentId;
import org.piax.agent.AgentInstantiationException;
import org.piax.agent.AgentPeer;
import org.piax.agent.AgentPeerListener;
import org.piax.agent.AgentTransportManager;
import org.piax.agent.DefaultAgentTransportManager;
import org.piax.agent.NoSuchAgentException;
import org.piax.agent.SetupTransportException;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.NoSuchRemoteObjectException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.ov.NoSuchOverlayException;

import test.Util;

public class TestAgentPeer extends Util {
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
    static private ArrayBlockingQueue<String> resultQueue = new ArrayBlockingQueue<String>(3);
    
    /**
     * リスナー・クラス
     * 呼ばれたらメッセージをresultQueueに入れる
     */
    class AListener implements AgentPeerListener {

        @Override
        public void onCreation(AgentId agId) {
            try {
                resultQueue.put("onCreation "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        @Override
        public void onDestruction(AgentId agId) {
            try {
                resultQueue.put("onDestruction "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onSleeping(AgentId agId) {
            try {
                resultQueue.put("onSleeping "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onDuplicating(AgentId agId) {
            try {
                resultQueue.put("onDuplicating "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onSaving(AgentId agId) {
            try {
                resultQueue.put("onSaving "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onDeparture(AgentId agId) {
            try {
                resultQueue.put("onDeparture "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onArrival(AgentId agId) {
            try {
                resultQueue.put("onArrival "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean onArriving(String agentClassName, String agId,
                String agentName, PeerId from) {
            try {
                resultQueue.put("onArriving "+agentClassName
                        +" "+agId+" "+agentName+" "+from);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }

        @Override
        public void onJoining(String ovName) {
            try {
                resultQueue.put("onJoining "+ovName);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onJoinCompleted(String ovName) {
            try {
                resultQueue.put("onJoinCompleted "+ovName);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onDuplication(AgentId agId) {
            try {
                resultQueue.put("onDuplication "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onWakeup(AgentId agId) {
            try {
                resultQueue.put("onWakeup "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onRestore(AgentId agId) {
            try {
                resultQueue.put("onRestore "+agId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Arriveを拒否するリスナー・クラス 
     */
    class DenyAListener extends AListener {
        @Override
        public boolean onArriving(String agentClassName, String agId,
                String agentName, PeerId from) {
            try {
                resultQueue.put("onArriving "+agentClassName
                        +" "+agId+" "+agentName+" "+from);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return false;
        }
    }
    
    /**
     * 前準備
     * エージェント・ピア、エージェントの作成
     * @throws Exception
     */
    @Before
    public void setup() throws Exception {
        Net ntype = Net.TCP;

        File agPath = new File("classes");
        // AgentPeerを作成
        localAgentPeer = new AgentPeer(localPeerName,genLocator(ntype, "localhost", 10001),
                    genLocator(ntype, "localhost", 10001),agPath);
        AgentTransportManager tm = new DefaultAgentTransportManager(
                remotePeerName,
                genLocator(ntype, "localhost", 10002),
                genLocator(ntype, "localhost", 10001));
        remoteAgentPeer = new AgentPeer(remotePeerName,tm,agPath);
        // createAgent
        localAgentId = localAgentPeer.createAgent(
                "test.agentpeer.SampleAgent", localAgentName);
        remoteAgentId = remoteAgentPeer.createAgent(
                SampleAgent.class, remoteAgentName);
        localAgentPeer.join();
        remoteAgentPeer.join();
        // Agentへの参照を取得。(通常のユーザは行ってはいけない裏ワザ)
        AgentHomeImpl localHome = (AgentHomeImpl)localAgentPeer.getHome();
        localAgent = localHome.getAgentContainer(localAgentId).getAgent();
        AgentHomeImpl remoteHome = (AgentHomeImpl)remoteAgentPeer.getHome();
        remoteAgent = remoteHome.getAgentContainer(remoteAgentId).getAgent();
    }
    
    /**
     * getFullNameのテスト
     * @throws AgentException
     */
    @Test
    public void getFullName() throws AgentException {
        String fname = localAgent.getFullName();
        assertEquals(localAgentName+"@"+localPeerName,fname);        
    }
    
    /**
     * getAgentPeerのテスト
     * @throws AgentException
     */
    @Test
    public void getAgentPeer() throws AgentException {
        AgentPeer apeer = localAgent.getAgentPeer();
        assertEquals(localAgentPeer,apeer);
    }
    
    /**
     * getNameのテスト
     * @throws AgentException
     */
    @Test
    public void getName() throws AgentException {
        String name = localAgentPeer.getName();
        assertEquals(localPeerName,name);
    }
    
    /**
     * 宣言していない属性をbindする。
     * IllegalArgumentExceptionを発生するはず。
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test(expected=IllegalArgumentException.class)
    public void bindOverlayNotDeclare() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        localAgentPeer.bindOverlay("NotDeclare", "LLNET");
    }

    /**
     * 登録されていないオーバレイにbindする。
     * NoSuchOverlayExceptionが発生するはず
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test(expected=NoSuchOverlayException.class)
    public void bindOverlayUnknown() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        localAgentPeer.declareAttrib(locationName);
        localAgentPeer.bindOverlay(locationName, "XXX");
    }
    
    /**
     * setAttribのテスト
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void setAttrib() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.setAttrib(locationName,"XX");
    }
    
    /*
     * getAttribValueのテスト
     */
    @Test
    public void getAttribValue() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.setAttrib(locationName,"XX");
        Object r = remoteAgentPeer.getAttribValue(locationName);
        assertTrue(r instanceof String);
        assertEquals("XX",r);
    }
    
    /**
     * getAttribNamesのテスト
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void getAttribNames() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        String anotherAttr = "num";
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.declareAttrib(anotherAttr);
        remoteAgentPeer.setAttrib(locationName,"XX");
        remoteAgentPeer.setAttrib(anotherAttr,"YY");
        List<String> r = remoteAgentPeer.getAttribNames();
        assertEquals(2,r.size());
        assertEquals(locationName,r.get(0));
        assertEquals(anotherAttr,r.get(1));
    }
    
    /**
     * 宣言した型と異なる値を属性に設定する。
     * IncompatibleTypeExceptionが発生するはず。
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test(expected = IncompatibleTypeException.class)
    public void setAttribTypeIncompatible() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName,Location.class);
        remoteAgentPeer.setAttrib(locationName,"XX");
    }
    
    /**
     * オーバレイが受け付ける型と異なる値を属性に設定する。
     * IncompatibleTypeExceptionが発生するはず。
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test(expected = IncompatibleTypeException.class)
    public void setAttribTypeIncompatible2() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.bindOverlay(locationName, "LLNET");
        remoteAgentPeer.setAttrib(locationName, "XX");
    }
    
    /**
     * 型宣言された属性へのsetAttribのテスト
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void setAttribType() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName,Location.class);
        remoteAgentPeer.setAttrib(locationName,new Location(1.0,1.0));
    }
    
    /**
     * isIndexedのテスト
     * 
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void isIndexed() throws IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException,
            SetupTransportException {
        String locationName = "$location";
        String attribName = "num";
        remoteAgentPeer.declareAttrib(locationName,Location.class);
        remoteAgentPeer.declareAttrib(attribName);
        remoteAgentPeer.setAttrib(locationName,new Location(1.0,1.0));
        remoteAgentPeer.setAttrib(attribName,1,false); // indexを使用しない属性値
        boolean r = remoteAgentPeer.isIndexed(locationName);
        assertTrue(r);
        r = remoteAgentPeer.isIndexed(attribName);
        assertFalse(r);
    }
    
    /**
     * LLNETのbind
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void bindLLNET() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.bindOverlay(locationName, "LLNET");
        remoteAgentPeer.setAttrib(locationName, new Location(10,10));
    }
    
    /**
     * getBindOverlayのテスト
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void getBindOverlay() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.bindOverlay(locationName, "LLNET");
        String name = remoteAgentPeer.getBindOverlay(locationName);
        assertEquals("LLNET",name);
    }
    
    /**
     * unbindOverlayのテスト
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void unbindOverlay() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.bindOverlay(locationName, "LLNET");
        String name = remoteAgentPeer.getBindOverlay(locationName);
        assertEquals("LLNET",name);
        remoteAgentPeer.unbindOverlay(locationName);
        name = remoteAgentPeer.getBindOverlay(locationName);
        assertNull(name);
    }
    
    /**
     * 型宣言された属性へのLLNETのbind
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void bindLLNETType() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String locationName = "$location";
        remoteAgentPeer.declareAttrib(locationName,Location.class);
        remoteAgentPeer.bindOverlay(locationName, "LLNET");
        remoteAgentPeer.setAttrib(locationName, new Location(10,10));
    }
    
    /**
     * LLNETをbindして、discovertCallをする。
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void bindLLNETdc() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String locationName = "$location";
        localAgentPeer.declareAttrib(locationName, Location.class);
        localAgentPeer.bindOverlay(locationName, "LLNET");
        remoteAgentPeer.declareAttrib(locationName);
        remoteAgentPeer.bindOverlay(locationName, "LLNET");
        remoteAgentPeer.setAttrib(locationName, new Location(10,10));
        List<String> r = localAgent.getList(localAgent.getDCStub(locationName+" in rect(9.5,9.5,1,1)",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(localAgent.getDCStub("$location in rect(8.0,8.0,1,1)",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size());
    }
    
    /**
     * DOLRをbindして整数を属性に設定して
     * getDCStubをする。
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void bindDOLRInt() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String attrName = "number";
        localAgentPeer.declareAttrib(attrName);
        localAgentPeer.bindOverlay(attrName, "DOLR");
        remoteAgentPeer.declareAttrib(attrName);
        remoteAgentPeer.bindOverlay(attrName, "DOLR");
        remoteAgentPeer.setAttrib(attrName, 1);
        List<String> r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 2",
                SampleAgentIf.class).hello());
        assertEquals(0,r.size());
    }
    
    /**
     * DOLRをbindして文字列を属性に設定して
     * getDCStubをする。
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void bindDOLRStr() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String attrName = "number";
        localAgentPeer.declareAttrib(attrName);
        localAgentPeer.bindOverlay(attrName, "DOLR");
        remoteAgentPeer.declareAttrib(attrName);
        remoteAgentPeer.bindOverlay(attrName, "DOLR");
        remoteAgentPeer.setAttrib(attrName, "X");
        List<String> r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq \"X\"",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq \"Y\"",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size());
    }
    
    /**
     * MSkipGraphをbindして
     * discovertCallをする。
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void bindMSG() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String attrName = "number";
        localAgentPeer.declareAttrib(attrName);
        localAgentPeer.bindOverlay(attrName, "MSG");
        remoteAgentPeer.declareAttrib(attrName);
        remoteAgentPeer.bindOverlay(attrName, "MSG");
        remoteAgentPeer.setAttrib(attrName, 1);
        List<String> r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 2",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size());
    }
    
    /**
     * MSkipGraphをbindして、
     * AgentのsetAttribとAgentPeerのsetAttribの関係を
     * テストする。
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void bindMSGovr() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String attrName = "number";
        localAgentPeer.declareAttrib(attrName);
        localAgentPeer.bindOverlay(attrName, "MSG");
        remoteAgentPeer.declareAttrib(attrName);
        remoteAgentPeer.bindOverlay(attrName, "MSG");
        remoteAgentPeer.setAttrib(attrName, 1);
        List<String> r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 2",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size()); // 見つからないはず
        remoteAgent.setAttrib(attrName,2); // AgentのsetAttribでオーバライドする
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 2",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size()); // 今度は2で見つかる
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size()); // 1では見つからない
    }
    
    /**
     * removeAttribのテスト
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void removeAttr() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String attrName = "number";
        localAgentPeer.declareAttrib(attrName);
        localAgentPeer.bindOverlay(attrName, "MSG");
        remoteAgentPeer.declareAttrib(attrName);
        remoteAgentPeer.bindOverlay(attrName, "MSG");
        remoteAgentPeer.setAttrib(attrName, 1);
        List<String> r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 2",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size());//　見つからないはず
        remoteAgent.setAttrib(attrName,2); // AgentのsetAttribでオーバライドする
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 2",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());// 今度は2で見つかる
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size()); // 1では見つからないはず
        remoteAgent.removeAttrib(attrName); // Agentの属性値は削除する。
                                            // AgentPeerの属性値が復活するはず。
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(1,r.size());// 1で見つかる
        assertEquals("I'm "+remoteAgentName,r.get(0));
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 2",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size());// 2では見つからないはず
        remoteAgentPeer.removeAttrib(attrName); // AgentPeerの属性値も削除する
        r = localAgent.getList(
                localAgent.getDCStub(attrName+" eq 1",
            SampleAgentIf.class).hello());
        assertEquals(0,r.size()); // 1でも見つからないはず
    }
    
    /**
     * getPeerIdのテスト
     * 
     * @throws Exception
     */
    @Test
    public void getPeerId() throws Exception {
        PeerId pid = localAgentPeer.getPeerId();
        assertEquals(new PeerId(localPeerName),pid);
    }
    
    /**
     * getAgentIdsのテスト
     */
    @Test
    public void getAgentIds() {
        Set<AgentId> ids = localAgentPeer.getAgentIds();
        assertEquals(1,ids.size());
        AgentId[] ida = ids.toArray(new AgentId[1]);
        assertEquals(localAgentId,ida[0]);
    }
    
    /**
     * getAgentIdsClassのテスト
     */
    @Test
    public void getAgentIdsClass() {
        Set<AgentId> ids = localAgentPeer.getAgentIds(
                "test.agentpeer.SampleAgent");
        assertEquals(1,ids.size());
        AgentId[] ida = ids.toArray(new AgentId[1]);
        assertEquals(localAgentId,ida[0]);
        ids = localAgentPeer.getAgentIds("test.agentpeer.SampleAgentX");
        assertEquals(0,ids.size());
    }
    
    /**
     * getAgentNameのテスト
     * 
     * @throws AgentException
     */
    @Test
    public void getAgentName() throws AgentException {
        String name = localAgentPeer.getAgentName(
                localAgentId);
        assertEquals(localAgentName,name);
    }
    
    /**
     * 存在しないAgentを指定してのgetAgentName。
     * NoSuchAgentExceptionが発生するはず
     * @throws AgentException
     */
    @Test(expected = NoSuchAgentException.class)
    public void getAgentNameNotexist() throws AgentException {
        localAgentPeer.getAgentName(
                remoteAgentId);
    }
    
    /**
     * getAgentClassのテスト
     * @throws AgentException
     */
    @Test
    public void getAgentClass() throws AgentException {
        Class<?> clazz = localAgentPeer.getAgentClass(
                localAgentId);
        assertEquals(SampleAgent.class,clazz);
    }
    
    /**
     * 存在しないAgentを指定してのgetAgentClass。
     * NoSuchAgentExceptionが発生するはず
     * @throws AgentException
     */
    @Test(expected = NoSuchAgentException.class)
    public void getAgentClassNotexist() throws AgentException {
        localAgentPeer.getAgentClass(
                remoteAgentId);
    }
    
    /**
     * 存在しないクラスを指定してエージェントを作成する。
     * ClassNotFoundExceptionが発生するはず。
     * @throws ClassNotFoundException
     * @throws AgentInstantiationException
     */
    @Test(expected = ClassNotFoundException.class)
    public void createAgentClassNotFound() throws ClassNotFoundException,
            AgentInstantiationException {
        localAgentPeer.createAgent(
                "test.agentpeer.XSampleAgent");
    }
    
    /**
     * giveAgentNameのテスト
     * @throws AgentException
     * @throws ClassNotFoundException
     */
    @Test
    public void giveAgentName() throws AgentException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.SampleAgent");
        String name = "changedName";
        boolean b = localAgentPeer.giveAgentName(aid, name);
        assertTrue(b);
        String r = localAgentPeer.getAgentName(aid);
        assertEquals(name,r);
    }
    
    /**
     * 既に名前のあるエージェントにgiveAgentNameを適用する。
     * 
     * @throws AgentException
     */
    @Test
    public void giveAgentNameFalse() throws AgentException {
        String name = "changedName";
        boolean b = localAgentPeer.giveAgentName(localAgentId, name);
        assertFalse(b); // 失敗してfalseになるはず
    }
    
    /**
     * 存在しないエージェントを指定してgiveAgentNameを呼び出す。
     * NoSuchAgentExceptionは発生するはず。
     * @throws AgentException
     */
    @Test(expected = NoSuchAgentException.class)
    public void giveAgentNameException() throws AgentException {
        String name = "changedName";
        localAgentPeer.giveAgentName(remoteAgentId, name);
    }
    
    /**
     * getDeclareAttribNamesのテスト
     * 
     * @throws AgentException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     * @throws SetupTransportException
     */
    @Test
    public void getDeclaredAttribNames() throws AgentException,
          IllegalArgumentException, NoSuchOverlayException,
          IncompatibleTypeException, SetupTransportException {
        String attrName = "number";
        String locationName = "$location";
        localAgentPeer.declareAttrib(attrName);
        localAgentPeer.declareAttrib(locationName);
        List<String> names = localAgentPeer.getDeclaredAttribNames();
        assertEquals(2,names.size());
        assertEquals(attrName,names.get(0));
        assertEquals(locationName,names.get(1));
    }
    
    /**
     * createAgentおよび
     * リスナーのonCreationのテスト
     * 
     * @throws ClassNotFoundException
     * @throws AgentInstantiationException
     * @throws InterruptedException
     */
    @Test
    public void onCreation() throws ClassNotFoundException,
            AgentInstantiationException, InterruptedException {
        localAgentPeer.setListener(new AListener());
        // クラス名を指定してのエージェントの作成の場合
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.SampleAgent", "myAgent");
        String r = resultQueue.take();
        assertEquals("onCreation "+aid,r);
        // クラスを指定してのエージェントの作成の場合
        aid = localAgentPeer.createAgent(SampleAgent.class, "myAgent2");
        r = resultQueue.take();
        assertEquals("onCreation "+aid,r);
    }
    
    /**
     * destroyAgentおよび
     * リスナーのonDesctructionのテスト
     * @throws AgentException
     * @throws InterruptedException
     */
    @Test
    public void onDestruction() throws AgentException, InterruptedException {
        localAgentPeer.setListener(new AListener());
        localAgentPeer.destroyAgent(localAgentId);
        String r = resultQueue.take();
        assertEquals("onDestruction "+localAgentId,r);
    }
    
    /**
     * リスナーのonJoining, onJoinCompletedのテスト
     * @throws Exception
     */
    @Test
    public void onJoiningAndJoinCompleted() throws Exception {
        AgentPeer apeer = new AgentPeer("myPeer",genLocator(Net.TCP, "localhost", 10003),
                    genLocator(Net.TCP, "localhost", 10003));
        apeer.setListener(new AListener());
        apeer.join();
        String r = resultQueue.take();
        assertEquals("onJoining MSG",r); // まずonJoinigが呼び出される。
        r = resultQueue.take();
        assertEquals("onJoinCompleted MSG",r);// 次にonJoinCompletedが呼び出される。
        apeer.fin();
    }
    
    /**
     * PersisitentAgentでないエージェントに対しsleepAgentを適用する。
     * AgentCapabilityExceptionがは発生するはず。
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(expected = AgentCapabilityException.class)
    public void sleepAgentNotPersistent() throws ObjectStreamException, AgentException,
            IOException, InterruptedException {
        localAgentPeer.sleepAgent(localAgentId);
    }
    
    /**
     * sleepAgent,wakeupAgent,isAgentSleepingおよび
     * リスナーのonSleeping, onWakeupのテスト
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Test
    public void sleepAgent() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.PersistentSampleAgent", "myAgent");
        localAgentPeer.setListener(new AListener());
        localAgentPeer.sleepAgent(aid);
        String r = resultQueue.take();
        assertEquals("onSleeping "+aid,r);
        assertTrue(localAgentPeer.isAgentSleeping(aid));
        assertFalse(localAgentPeer.isAgentSleeping(localAgentId));// localAgentはスリープしてない
        localAgentPeer.wakeupAgent(aid);
        r = resultQueue.take();
        assertEquals("onWakeup "+aid,r);
        assertFalse(localAgentPeer.isAgentSleeping(aid));// wakeupしている
    }
    
    /**
     * PersisitentAgentでないエージェントに対しduplicateAgentを適用する。
     * AgentCapabilityExceptionがは発生するはず。
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(expected = AgentCapabilityException.class)
    public void duplicateAgentNotPersistent() throws ObjectStreamException, AgentException,
            IOException, InterruptedException {
        localAgentPeer.duplicateAgent(localAgentId);
    }
    
    /**
     * duplicateAgentおよび
     * リスナーのonDuplicating, onDuplicationのテスト
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Test
    public void duplicateAgent() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.PersistentSampleAgent", "myAgent");
        localAgentPeer.setListener(new AListener());
        AgentId naid = localAgentPeer.duplicateAgent(aid);
        String r = resultQueue.take();
        assertEquals("onDuplicating "+aid,r);
        r = resultQueue.take();
        assertEquals("onDuplication "+naid,r);
    }
    
    /**
     * PersisitentAgentでないエージェントに対しsaveAgentを適用する。
     * AgentCapabilityExceptionがは発生するはず。
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(expected = AgentCapabilityException.class)
    public void saveAgentNotPersistent() throws ObjectStreamException, AgentException,
            IOException, InterruptedException {
        localAgentPeer.saveAgent(localAgentId);
    }
    
    /**
     * saveAgent, restoreAgentおよび
     * リスナーのonSaving, onRestoreのテスト
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Test
    public void saveAgent() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.PersistentSampleAgent", "myAgent");
        PersistentSampleAgentIf stub
            = localAgentPeer.getHome().getStub(PersistentSampleAgentIf.class,aid);
        localAgentPeer.setListener(new AListener());
        stub.setN(1);
        localAgentPeer.saveAgent(aid);
        String r = resultQueue.take();
        assertEquals("onSaving "+aid,r);
        stub.setN(2);
        assertEquals(2,stub.getN());
        localAgentPeer.restoreAgent(aid);
        r = resultQueue.take();
        assertEquals("onRestore "+aid,r);
        assertEquals(1,stub.getN());
    }
    
    /**
     * PersisitentAgentでないエージェントに対しFile指定のsaveAgentを適用する。
     * AgentCapabilityExceptionがは発生するはず。
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(expected = AgentCapabilityException.class)
    public void saveAgentNotPersistentToFile() throws ObjectStreamException, AgentException,
            IOException, InterruptedException {
        localAgentPeer.saveAgent(localAgentId,new File("/tmp/testAgentPeer.jar"));
    }
    
    /**
     * File指定のsaveAgent, restoreAgentおよび
     * リスナーのonSaving, onRestoreのテスト
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Test
    public void saveAgentToFile() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.PersistentSampleAgent", "myAgent");
        PersistentSampleAgentIf stub 
            = localAgentPeer.getHome().getStub(PersistentSampleAgentIf.class, aid);
        localAgentPeer.setListener(new AListener());
        stub.setN(1);
        File file = new File("/tmp/testAgentPeer.jar");
        localAgentPeer.saveAgent(aid,file);
        String r = resultQueue.take();
        assertEquals("onSaving "+aid,r);
        stub.setN(2);
        assertEquals(2,stub.getN());
        AgentId naid = localAgentPeer.restoreAgent(file);
        r = resultQueue.take();
        assertEquals("onRestore "+naid,r);
        assertEquals(aid,naid);
        assertEquals(1,stub.getN());
    }

    /**
     * MobileAgentでないエージェントに対しtravelAgentを適用する。
     * AgentCapabilityExceptionがは発生するはず。
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Test(expected = AgentCapabilityException.class)
    public void travelAgentNotMobile() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        localAgentPeer.travelAgent(localAgentId,remoteAgentPeer.getPeerId());
    }
    
    /**
     * travelAgentおよび
     * リスナーのonDepartureのテストをする。
     * 移動したエージェントが存在しないことを確認する。
     * 
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Test
    public void travelAgent0() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.MobileSampleAgent", "myAgent");
        PersistentSampleAgentIf stub
            = localAgentPeer.getHome().getStub(PersistentSampleAgentIf.class, aid);
        localAgentPeer.setListener(new AListener());
        stub.setN(1);
        localAgentPeer.travelAgent(aid,remoteAgentPeer.getPeerId());
        String r = resultQueue.take();
        assertEquals("onDeparture "+aid,r);
        /*
         *  移動済みのエージェントを呼びだそうとすると
         *  NoSuchRemoteObjectExceptionがは発生するはず。
         */
        try {
            stub.setN(2); 
        } catch (UndeclaredThrowableException e) {
            Throwable th = e.getCause();
            assertTrue(th instanceof RPCException);
            th = th.getCause();
            assertTrue(th instanceof NoSuchRemoteObjectException);
            return;
        }
        fail("expected NoSuchRemoteObjectException");
    }
    
     /**
     * travelAgentおよび
     * リスナーのonDeparture, onArriving, onArrivalのテスト
      * 
      * @throws ObjectStreamException
      * @throws AgentException
      * @throws IOException
      * @throws InterruptedException
      * @throws ClassNotFoundException
      */
    @Test
    public void travelAgent() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.MobileSampleAgent", "myAgent");
        PersistentSampleAgentIf stub 
            = localAgentPeer.getHome().getStub(PersistentSampleAgentIf.class, aid);
        localAgentPeer.setListener(new AListener());
        remoteAgentPeer.setListener(new AListener());
        stub.setN(1);
        localAgentPeer.travelAgent(aid,remoteAgentPeer.getPeerId());
        String r = resultQueue.take();
        assertEquals("onDeparture "+aid,r);
        r = resultQueue.take();
        assertEquals("onArriving test.agentpeer.MobileSampleAgent "+aid
                +" myAgent "+localAgentPeer.getPeerId(),r);
        r = resultQueue.take();
        assertEquals("onArrival "+aid,r);
        PersistentSampleAgentIf rstub
            = remoteAgentPeer.getHome().getStub(PersistentSampleAgentIf.class, aid);
        assertEquals(1,rstub.getN());
    }
    
    /**
     * travelAgentでリスナのOnArrivingでfalseを返し
     * 、拒否される場合のテスト
     * @throws ObjectStreamException
     * @throws AgentException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Test
    public void travelAgentDenied() throws ObjectStreamException, AgentException,
            IOException, InterruptedException, ClassNotFoundException {
        AgentId aid = localAgentPeer.createAgent(
                "test.agentpeer.MobileSampleAgent", "myAgent");
        localAgentPeer.setListener(new AListener());
        remoteAgentPeer.setListener(new DenyAListener());
        PersistentSampleAgentIf stub = 
                localAgentPeer.getHome().getStub(PersistentSampleAgentIf.class,aid);
        stub.setN(1);
        /*
         * 拒否されるのでAgentAccessDeniedExceptionが発生するはず。
         */
        try {
            localAgentPeer.travelAgent(aid,remoteAgentPeer.getPeerId());
            fail("AgentAccessDeniedException expected");
        } catch (AgentAccessDeniedException e) {
            // 期待通り
        }
        String r = resultQueue.take();
        assertEquals("onDeparture "+aid,r); // onDepatureは呼び出されている
        r = resultQueue.take();
        assertEquals("onArriving test.agentpeer.MobileSampleAgent "+aid
                +" myAgent "+localAgentPeer.getPeerId(),r); // onArrivingも呼び出されている
        r = resultQueue.poll();
        assertNull(r); // onArrivalは呼び出されないはず
        assertEquals(1,stub.getN()); // 移動は失敗したので残っているはず
    }
    
    /**
     * 後始末
     * @throws Exception
     */
    @After
    public void finAgents() throws Exception {
        localAgentPeer.setListener(null);
        remoteAgentPeer.setListener(null);
        localAgent.destroy();
        remoteAgent.destroy();
        localAgentPeer.leave();
        remoteAgentPeer.leave();
        localAgentPeer.fin();
        remoteAgentPeer.fin();
        resultQueue.clear();
    }
    
        /*
     * テストメソッドを個別に動作させたい場合に
     * 書き換えて実行する。
     */
    public static void main(String[] args) {
        TestAgentPeer o = new TestAgentPeer();
        
        try {
            o.setup();
            o.bindDOLRInt();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                o.finAgents();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
