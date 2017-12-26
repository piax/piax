/*
 * AgentPeer.java - An agent peer.
 * 
 * Copyright (c) 2015 PIAX development team
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
 * $Id: AgentHomeImpl.java 1064 2014-07-02 05:31:54Z ishi $
 */

package org.piax.agent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;

/**
 * <p>
 * トランスポートの詳細には立ち入りたくないユーザのための
 * ユーティリティ的なクラスである。
 * AgentHomeやPeerなどのクラスをラッピングして、簡易版のピアを実装する。
 * AgentHomeのメソッドからトランスポートを直接扱うもの、
 * RPC、discoveryCallなど不要なものを 除いたメソッドを備える。
 * AgentHomeやPeerクラスを扱うことなくエージェントを操作可能である。
 * トランスポートの詳細に関しては、AgentTransportManagerに任せるようにできている。
 * トランスポートを自由にカスタマイズしたいユーザにおいても、
 * AgentTransportManagerを独自に実装してコンストラクタの引数に指定することで
 * このクラスのトランスポートの詳細をカスタマイズ可能である。
 * 
 * テストベッドにおいては、システムの都合上、このクラスの使用が必須となる。
 * </p>
 * 
 * <p>
 * テストベッド外では、以下のように使用する。
 * </p>
 * 
 * <pre>
 * AgentPeer apeer = new AgentPeer("peer1", new MyAgentTransportManager(...), "agents/");
 * apeer.declareAttrib("name",String.class);
 * apeer.bindOverlay("name","DOLR");
 * apeer.join();
 * </pre>
 * <p>
 * DefaultInetAgentTransportManagerを使用する場合は、以下のようにする。
 * </p>
 * 
 * <pre>
 * AgentPeer apeer = new AgentPeer("peer1", 
 *   new TcpLocator("myAddress:myPort"), new TcpLocator("seedAddress:seedPort"),
 *   "agents/");
 * apeer.declareAttrib("name",String.class);
 * apeer.bindOverlay("name","DOLR");
 * apeer.join();
 * </pre>
 * <p>
 * テストベッドのMainAgentでは、 TestbedAgentTransportManagerクラスを定義して自身のjar内に
 * 入れることによりAgentTransportManagerを自身のものに設定可能である。 
 * 
 * <h3>リスナー</h3>
 * <p>
 * リスナーを設定することによりエージェントの状態の変更や
 * 支配下のオーバレイのjoinの通知を受けることができる。
 * 詳細は、AgentPeerListenerの説明を参照のこと。
 * </p>
 * <h3>エクゼキュータ</h3>
 * <p>
 * 非同期型RPCがなくなったため、同様のことを行うには別スレッドでRPCを実行する必要がある。
 * AgentPeerには、それを助けるためのThredPoolExecutorを用意した。以下のように使用する。
 * </p>
 * <pre>
 * final AgentRef ref;
 * ...
 * Future&lt;Integer&gt; f = agentPeer.submit(new Callable&lt;Integer&gt;() {
 *     {@literal @}Override
 *     public Integer call() throws RPCException {
 *         return rcall(AppIf.class,ref).sum(10);       
 *     }
 *  });
 *  int sum = f.get();
 * </pre>
 */
public class AgentPeer {
    private AgentTransportManager transportManager = null;
    private AgentHomeImpl home = null;
    private final String peerName;
    private AgentPeerListener listener = null;
    /**
     * submitで使用するスレッドの最大数
     */
    public static final int MAX_SUBMIT_THREAD = 10000;
    
    /**
     * taskを実行するexecutor
     */
    private ThreadPoolExecutor executor;
    
    /**
     * exectorを設定する。
     * デフォルト以外のexecutorを使用する場合に使用する。
     * exectorの実行と競合しないようにsynchronizedとする。
     * @param newExecutor 設定するexecutor
     */
    public synchronized void setExecutor(ThreadPoolExecutor newExecutor) {
        if (executor != null) {
            // 既に存在する場合は、以前のものの後始末をする。
            executor.shutdown();
        }
        executor = newExecutor;
    }
    
    public ThreadPoolExecutor getExecutor() {
        return executor;
    }
    
    /**
     * executorを使用してタスクを実行する。
     * @param task 実行するタスク
     * @return 実行結果を表すFuture
     * @throws InterruptedException an interrupted exception.
     */
    public <T> Future<T> submit(Callable<T> task) throws InterruptedException {
        return executor.submit(task);
    }

    public void setListener(AgentPeerListener listener) {
        this.listener = listener;
        home.setListener(listener);
        transportManager.setListener(listener);
    }
    
    public AgentPeerListener getListener() {
        return listener;
    }
    
    /**
     * トランスポートの初期化を行う。
     * 
     * @param transportManager
     * @throws Exception
     */
    private void setupTransports(AgentTransportManager transportManager,
            ClassLoader parentAgentLoader, File... agClassPath)
            throws Exception {
        this.transportManager = transportManager;
        ChannelTransport<?> ctr = null;

        try {
            //ctr = transportManager.getRPCTransport();
            transportManager.setup(home);
            ctr = (ChannelTransport<?>)transportManager.getTransport("RPC");
            home = new AgentHomeImpl(this, ctr, parentAgentLoader, agClassPath);
        } catch (Exception e) {
            if (ctr != null) {
                ctr.fin();
            }
            throw new SetupTransportException(e);
        }
    }
    

    /**
     * トランスポートの設定以外のことを行うコンストラクタ。 内部処理専用で外部からは呼べない。
     * 
     * @param name
     *            ピア名。システム内でユニークである必要がある。
     * @param classPath
     *            　エージェントが作成される際に、クラスのclassファイルが 存在するパスの並び。既にそのクラスがパス中に存在しない場合は
     *            これらのパスが左から順に検索される。 各パスは'/'で終了していれば、ディレクトリを示し、
     *            そうでなければJARファイル名を示すものとする。
     * @throws Exception
     */
    private AgentPeer(String name) {
        peerName = name;
        executor = new ThreadPoolExecutor(0,MAX_SUBMIT_THREAD,0,TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"AgentPeer:"+peerName);
            }
        });
    }

    /**
     * コンストラクタ。 トランスポートの設定も行う。
     * 
     * @param name
     *            ピア名。システム内でユニークである必要がある。
     * @param transportManager
     *            使用するトランスポートマネージャ
     * @param agClassPath エージェントが作成される際に、クラスのclassファイルが 存在するパスの並び。既にそのクラスがパス中に存在しない場合は
     *            これらのパスが左から順に検索される。 各パスは'/'で終了していれば、ディレクトリを示し、
     *            そうでなければJARファイル名を示すものとする。
     * @throws Exception an exception occurred during instanciate.
     */
    public AgentPeer(String name, AgentTransportManager transportManager,
            File... agClassPath) throws Exception {
        this(name);
        setupTransports(transportManager,
                AgentConfigValues.defaultParentAgentLoader, agClassPath);
    }
    
    /**
     * Constructor using AgentTransportManager.
     * 
     * @param transportManager the AgentTransportManager instance.
     * @param agClassPath エージェントが作成される際に、クラスのclassファイルが 存在するパスの並び。既にそのクラスがパス中に存在しない場合は
     *            これらのパスが左から順に検索される。 各パスは'/'で終了していれば、ディレクトリを示し、
     *            そうでなければJARファイル名を示すものとする。
     * @throws Exception an exception occurred during instanciate.
     */
    public AgentPeer(AgentTransportManager transportManager,
            File... agClassPath) throws Exception {
        this(transportManager.getName());
        setupTransports(transportManager,
                AgentConfigValues.defaultParentAgentLoader, agClassPath);
    }

    /**
     * コンストラクタ。 トランスポートの設定も行う。
     * 
     * @param name
     *            ピア名。システム内でユニークである必要がある。
     * @param transportManager 使用するトランスポートマネージャ
     * @param parentAgentLoader Agentのclass loaderの親クラスローダ
     * @param agClassPath エージェントが作成される際に、クラスのclassファイルが 存在するパスの並び。既にそのクラスがパス中に存在しない場合は
     *            これらのパスが左から順に検索される。 各パスは'/'で終了していれば、ディレクトリを示し、
     *            そうでなければJARファイル名を示すものとする。
     * @throws Exception an exception occurred during instanciate.
     */
    public AgentPeer(String name, AgentTransportManager transportManager,
            ClassLoader parentAgentLoader, File... agClassPath)
            throws Exception {
        this(name);
        setupTransports(transportManager, parentAgentLoader, agClassPath);
    }

    /**
     * 簡易版コンストラクタ
     * 
     * DefaultTransportManagerを使用する場合に限り使用できるコンストラクタ。
     * 
     * @param name
     *            ピア名。システム内でユニークである必要がある。
     * @param peerLocator
     *            自身のピアのロケータ
     * @param seedLocator
     *            シードのロケータ
     * @param agClassPath
     *            　エージェントが作成される際に、クラスのclassファイルが 存在するパスの並び。既にそのクラスがパス中に存在しない場合は
     *            これらのパスが左から順に検索される。 各パスは'/'で終了していれば、ディレクトリを示し、
     *            そうでなければJARファイル名を示すものとする。
     * @throws Exception an exception occurred during instanciate.
     */
    public AgentPeer(String name, PeerLocator peerLocator,
            PeerLocator seedLocator, File... agClassPath) throws Exception {
        this(name);
        AgentTransportManager tm = new DefaultAgentTransportManager(
                getName(), peerLocator, seedLocator);
        setupTransports(tm, AgentConfigValues.defaultParentAgentLoader,
                agClassPath);
    }

    /**
     * 簡易版コンストラクタ
     * 
     * DefaultTransportManagerを使用する場合に限り使用できるコンストラクタ。
     * 
     * @param name
     *            ピア名。システム内でユニークである必要がある。
     * @param peerLocator
     *            自身のピアのロケータ
     * @param seedLocator
     *            シードのロケータ
     * @param parentAgentLoader
     *            Agentのclass loaderの親クラスローダ
     * @param agClassPath
     *            　エージェントが作成される際に、クラスのclassファイルが 存在するパスの並び。既にそのクラスがパス中に存在しない場合は
     *            これらのパスが左から順に検索される。 各パスは'/'で終了していれば、ディレクトリを示し、
     *            そうでなければJARファイル名を示すものとする。
     * @throws Exception an exception occurred during instanciate.
     */
    public AgentPeer(String name, PeerLocator peerLocator,
            PeerLocator seedLocator, ClassLoader parentAgentLoader,
            File... agClassPath) throws Exception {
        this(name);
        AgentTransportManager tm = new DefaultAgentTransportManager(
                getName(), peerLocator, seedLocator);
        setupTransports(tm, parentAgentLoader, agClassPath);
    }

    /**
     * 使用しているAgentTransportManagerを返す。
     * 
     * @return　使用しているAgentTransportManager
     */
    public AgentTransportManager getTransportManager() {
        return transportManager;
    }

    /**
     * ピア名を返す。
     * 
     * @return ピア名
     */
    public String getName() {
        return peerName;
    }

    /**
     * AgentHomeを返す。
     * 
     * @return AgentHome
     */
    public AgentHome getHome() {
        return home;
    }

    /**
     * 支配下のオーバレイをすべてleaveさせる。
     * 
     * @throws Exception an exception occurs during leave.
     */
    public void leave() throws Exception {
        transportManager.leave();
    }

    /**
     * 支配下のオーバレイをすべてjoinさせる。
     * 
     * @throws Exception an exception occurs during join.
     */
    public void join() throws Exception {
        transportManager.join();
    }

    /**
     * 属性名attribNameをオーバレイoverlayNameに結びつける。 このクラスではbindOverlayは、このメソッドのみとなる。
     * オーバレイ名と実際のオーバレイの関連付けは、AgentTransportManagerに任される。
     * 例えばDefaultAgentTransportManagerでは、"LLNET"と"DOLR"および"MSG"のみがサポートされる。
     * 
     * @param attribName
     *            属性名
     * @param overlayName
     *            オーバレイ名
     * @throws IllegalArgumentException a argument exception.
     * @throws NoSuchOverlayException an overlay exception.
     * @throws IncompatibleTypeException a type exception.
     * @throws SetupTransportException a transport exception.
     */
    public void bindOverlay(String attribName, String overlayName)
            throws IllegalArgumentException, NoSuchOverlayException,
            IncompatibleTypeException, SetupTransportException {
        Overlay<?, ?> ov = transportManager.getOverlay(overlayName);
        home.bindOverlay(attribName, ov.getTransportIdPath());
    }

    /**
     * 属性attribNameに結び付けられたオーバレイの名前を返す。
     * オーバレイ名と実際のオーバレイの関連付けは、AgentTransportManagerに任される。
     * 例えばDefaultAgentTransportManagerでは、"LLNET"と"DOLR"および"MSG"のみがサポートされる。
     * AgentTransportManagerが知らないオーバレイであった場合は、"UNKNOWN"を返す。
     * 
     * @param attribName
     *            属性名
     * @return オーバレイ名
     * @throws IllegalArgumentException an argument exception.
     */
    public String getBindOverlay(String attribName)
            throws IllegalArgumentException {
        Overlay<?, ?> ov = home.getBindOverlay(attribName);
        return transportManager.getOverlayName(ov);
    }

    /************* 以下はAgentHomeのメソッドからTransportを直接扱うものなど不要なものを除いたもの ********/

    public void fin() {
        if (home == null)
            return;
        home.fin();
        Peer peer = Peer.getInstance(getPeerId());
        if (peer != null) {
            peer.fin();
        }
    }

    public PeerId getPeerId() {
        return transportManager.getPeerId();
    }

    public boolean setAttrib(String name, Object value)
            throws IllegalArgumentException, IncompatibleTypeException {
        return home.setAttrib(name, value);
    }

    public boolean setAttrib(String name, Object value, boolean useIndex)
            throws IllegalArgumentException, IncompatibleTypeException {
        return home.setAttrib(name, value, useIndex);
    }

    public boolean removeAttrib(String name) {
        return home.removeAttrib(name);
    }

    public Object getAttribValue(String attribName) {
        return home.getAttribValue(attribName);
    }

    public List<String> getAttribNames() {
        return home.getAttribNames();
    }

    public boolean isIndexed(String attribName) throws IllegalArgumentException {
        return home.isIndexed(attribName);
    }

    public Set<AgentId> getAgentIds() {
        return home.getAgentIds();
    }

    public Set<AgentId> getAgentIds(String clazz) {
        return home.getAgentIds(clazz);
    }

    public String getAgentName(AgentId agentId) throws AgentException {
        return home.getAgentName(agentId);
    }

    public Class<? extends AgentIf> getAgentClass(AgentId agentId)
            throws AgentException {
        return home.getAgentClass(agentId);
    }

    public boolean giveAgentName(AgentId agentId, String name)
            throws AgentException {
        return home.giveAgentName(agentId, name);
    }

    public boolean isAgentSleeping(AgentId agentId) throws AgentException {
        return home.isAgentSleeping(agentId);
    }

    public AgentId createAgent(String clazz, String name)
            throws ClassNotFoundException, AgentInstantiationException {
        return home.createAgent(clazz, name);
    }

    public AgentId createAgent(Class<? extends Agent> clazz, String name)
            throws AgentInstantiationException {
        return home.createAgent(clazz, name);
    }

    public AgentId createAgent(String clazz) throws ClassNotFoundException,
            AgentInstantiationException {
        return home.createAgent(clazz);
    }

    public AgentId createAgent(Class<? extends Agent> clazz)
            throws AgentInstantiationException {
        return home.createAgent(clazz);
    }

    public void destroyAgent(AgentId agentId) throws AgentException {
        home.destroyAgent(agentId);
    }

    public void sleepAgent(AgentId agentId) throws AgentException,
            ObjectStreamException, IOException {
        home.sleepAgent(agentId);
    }

    public void wakeupAgent(AgentId agentId) throws AgentException,
            ObjectStreamException {
        home.wakeupAgent(agentId);
    }

    public AgentId duplicateAgent(AgentId agentId) throws AgentException,
            ObjectStreamException {
        return home.duplicateAgent(agentId);
    }

    public void saveAgent(AgentId agentId) throws AgentException,
            ObjectStreamException, IOException {
        home.saveAgent(agentId);
    }

    public void saveAgent(AgentId agentId, File fname) throws AgentException,
            FileNotFoundException, ObjectStreamException, IOException {
        home.saveAgent(agentId, fname);
    }

    public void restoreAgent(AgentId agentId) throws AgentException,
            ClassNotFoundException, ObjectStreamException, IOException {
        home.restoreAgent(agentId);
    }

    public AgentId restoreAgent(File fname) throws FileNotFoundException,
            InvalidAgentJarException, ClassNotFoundException,
            ObjectStreamException, IOException {
        return home.restoreAgent(fname);
    }

    public void travelAgent(AgentId agentId, PeerId peerId)
            throws AgentException, ClassNotFoundException,
            ObjectStreamException, IOException {
        home.travelAgent(agentId, peerId);
    }

    public void declareAttrib(String attribName) throws IllegalStateException {
        home.declareAttrib(attribName);
    }

    public void declareAttrib(String attribName, Class<?> type)
            throws IllegalStateException {
        home.declareAttrib(attribName, type);
    }

    public List<String> getDeclaredAttribNames() {
        return home.getDeclaredAttribNames();
    }

    public void unbindOverlay(String attribName)
            throws IllegalArgumentException, IllegalStateException {
        home.unbindOverlay(attribName);
    }

}
