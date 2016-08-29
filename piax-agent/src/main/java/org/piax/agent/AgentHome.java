/*
 * AgentHome.java - An agent home
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
 * $Id: AgentHome.java 1195 2015-06-08 04:00:15Z teranisi $
 */

package org.piax.agent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.List;
import java.util.Set;

import org.piax.common.CalleeId;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCMode;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.combined.CombinedOverlay;

/**
 * A class of agent home
 */
public interface AgentHome extends AgentCaller, Attributable {
    
    public static final String AG_SLEEP_DIR = "agents";
    public static final String AG_SAVE_DIR = "saved";

    public static final int DESTRUCTION = 1;
    public static final int SLEEPING = 2;
    public static final int DUPLICATING = 3;
    public static final int SAVING = 4;
    public static final int DEPARTURE = 5;
    
    AgentPeer getAgentPeer();
    
    /**
     * リスナーを設定する。
     * AgentPeerを使用する場合は、
     * AgentPeerの設定と矛盾するのを避けるため
     * AgentPeer以外から呼び出してはいけない。
     * @param listener
     */
    void setListener(AgentHomeListener listener);
    AgentHomeListener getListener();
    
    void onCall(Agent ag, int mode);
    
    void fin();
    
    /* peer info */
    
    PeerId getPeerId();

    ChannelTransport<?> getRPCTransport();
    void changeRPCTransport(ChannelTransport<?> trans);
    
    CombinedOverlay getCombinedOverlay();

    /* agents info */
    
    CalleeId getCalleeId(AgentId agentId) throws NoSuchAgentException;
    
    Set<AgentId> getAgentIds();

    Set<AgentId> getAgentIds(String clazz);

    String getAgentName(AgentId agentId) throws AgentException;

    Class<? extends AgentIf> getAgentClass(AgentId agentId) throws AgentException;

    boolean giveAgentName(AgentId agentId, String name)
            throws AgentException;

    boolean isAgentSleeping(AgentId agentId) throws AgentException;

    /* remote/local agent method call */
    <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, int timeout, RPCMode rpcMode);

    <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, RPCMode rpcMode);

    <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, int timeout);

    <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer);

    <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid,
            int timeout, RPCMode rpcMode);

    <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid,
            RPCMode rpcMode);

    <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid,
            int timeout);

    <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid);

    <S extends RPCIf> S getStub(Class<S> clz, AgentId agId);

     Object rcall(AgentId agId,
            Endpoint remotePeer, int timeout, RPCMode rpcMode,
            String method, Object... args) throws Throwable;

     Object rcall(AgentId agId,
            Endpoint remotePeer, RPCMode rpcMode,
            String method, Object... args) throws Throwable;

     Object rcall(AgentId agId,
            Endpoint remotePeer, int timeout,
            String method, Object... args) throws Throwable;

     Object rcall(AgentId agId,
            Endpoint remotePeer,
            String method, Object... args) throws Throwable;

     Object rcall(CalleeId cid,
            int timeout, RPCMode rpcMode,
            String method, Object... args) throws Throwable;

     Object rcall(CalleeId cid,
            RPCMode rpcMode,
            String method, Object... args) throws Throwable;

     Object rcall(CalleeId cid,
            int timeout,
            String method, Object... args) throws Throwable;

     Object rcall(CalleeId cid,
            String method, Object... args) throws Throwable;


    /* methods for handling agents */
    AgentId createAgent(String clazz, String name)
            throws ClassNotFoundException, AgentInstantiationException;

    AgentId createAgent(Class<? extends Agent> clazz, String name)
            throws AgentInstantiationException;

    AgentId createAgent(String clazz) throws ClassNotFoundException,
            AgentInstantiationException;

    AgentId createAgent(Class<? extends Agent> clazz)
            throws AgentInstantiationException;

    void destroyAgent(AgentId agentId) throws AgentException;

    void sleepAgent(AgentId agentId) throws AgentException,
            ObjectStreamException, IOException;

    void wakeupAgent(AgentId agentId) throws AgentException,
            ObjectStreamException;

    AgentId duplicateAgent(AgentId agentId) throws AgentException,
            ObjectStreamException;

    void saveAgent(AgentId agentId) throws AgentException,
            ObjectStreamException, IOException;

    void saveAgent(AgentId agentId, File fname) throws AgentException,
            FileNotFoundException, ObjectStreamException, IOException;

    void restoreAgent(AgentId agentId) throws AgentException,
            ClassNotFoundException, ObjectStreamException, IOException;

    AgentId restoreAgent(File fname) throws FileNotFoundException,
            InvalidAgentJarException, ClassNotFoundException,
            ObjectStreamException, IOException;

    void travelAgent(AgentId agentId, PeerId peerId)
            throws AgentException, ClassNotFoundException,
            ObjectStreamException, IOException;
    
    void declareAttrib(String attribName) throws IllegalStateException;

    void declareAttrib(String attribName, Class<?> type)
            throws IllegalStateException;

    /**
     * peer及び、すべてのagentが保有する属性名のリストを返す。
     * 
     * @return peer及び、すべてのagentが保有する属性名のリスト
     */
    List<String> getDeclaredAttribNames();

    /**
     * 指定されたattribNameを持つAttributeに、
     * 指定されたTransportId（複数可）をsuffixとして持ち、型の互換性のあるOverlayをbindさせる。
     * 
     * @param attribName attribName
     * @param suffix suffixとして指定されたTransportId（複数可）
     * @throws IllegalArgumentException 該当するAttributeが存在しない場合
     * @throws NoSuchOverlayException 該当するOverlayが存在しない場合
     * @throws IncompatibleTypeException 型の互換性が原因で候補が得られない場合
     */
    void bindOverlay(String attribName, TransportId... suffix)
            throws IllegalArgumentException, NoSuchOverlayException,
            IncompatibleTypeException;

    /**
     * 指定されたattribNameを持つAttributeに、
     * 指定されたTransportIdPathをsuffixとして持ち、型の互換性のあるOverlayをbindさせる。
     * 
     * @param attribName attribName
     * @param suffix suffixとして指定されたTransportIdPath
     * @throws IllegalArgumentException 該当するAttributeが存在しない場合
     * @throws NoSuchOverlayException 該当するOverlayが存在しない場合
     * @throws IncompatibleTypeException 型の互換性が原因で候補が得られない場合
     */
    void bindOverlay(String attribName, TransportIdPath suffix)
            throws IllegalArgumentException, NoSuchOverlayException,
            IncompatibleTypeException;

    void unbindOverlay(String attribName) throws IllegalArgumentException,
            IllegalStateException;

    Overlay<?, ?> getBindOverlay(String attribName)
            throws IllegalArgumentException;
}
