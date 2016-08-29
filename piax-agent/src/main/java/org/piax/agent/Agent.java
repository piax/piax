/*
 * Agent.java - A class that corresponds agents.
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
 * $Id: Agent.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent;

import java.util.List;

import org.piax.agent.impl.AgentAttribs;
import org.piax.agent.impl.AgentContainer;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.agent.impl.InitialAgentMode;
import org.piax.common.CalleeId;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>エージェントを表すクラスである。</p>
 * 
 * <h2>RPC</h2>
 * <p>
 *　RPCは、以下のようにして行う。
 * </p>
 * <pre>
 * int n = getStub(AppIf.class,targetAgentId,targetPeer).appMethod(arg1,arg2);
 * </pre>
 * <p>
 * ここで、AppIfは、対象のエージェントが実装しているインターフェースである。
 * AppIfは、RPCIfを継承していなければいけない。targetAgentIdは、対象のAgentIdである。
 * targetPeerは、対象のオブジェクトが存在するピアを表すEndpointである。Endpointの実際の型は、
 * デフォルトではPeerIdであるが、RPCのために用意したトランスポートを変更した場合は
 * それが対象とするものとなる。
 * appMethodは、対象が持つメソッドであり、arg1, arg2は、appMethodの引数であるものとする。
 * appMethodの返り値の型はintであるとする。
 * appMethodは、AppIfあるいは、
 * そのスーパー・インターフェースで宣言されていなければいけない。さらに、対象が呼び出し側と異なるピアに存在する場合は
 * {@literal @}RemoteCallableアノテーションがついていて、かつRPCExceptionをスローするように宣言されていなければいけない。
 * 例えば、以下の通りである。
 * </p>
 * <pre>
 * import org.piax.gtrans.RemoteCallable;
 * 
 * interface AppIf extends RPCIf {
 *   {@literal @}RemoteCallable
 *   int appMethod(int a1, String a2) throws RPCException;
 * }
 * </pre>
 * <h3>引数のコピー</h3>
 * <p>
 * RPCでは、引数は、一般にはコピーされるが、保証されない。呼び出されたメソッド内で引数のオブジェクトを変更した場合、
 * それが呼び出し側に反映されるかどうかは不明であるので注意が必要である。
 * 呼び出された側では、変更しない方が無難である。
 * </p>
 * <h3>oneway RPC</h3>
 * <p>
 * 呼び出したメソッドの終了を待つ通常の同期型RPCの他に、終了を待たないoneway RPCが存在する。
 * oneway RPCでは、呼び出したメソッドの返り値を受け取ることはできない。
 * {@literal @}RemoteCallableアノテーションにType.ONEWAYの引数を指定すると
 * デフォルトではONEWAY RPCとして呼ばれる。
 * oneway RPCを指定しても実際に、非同期になるかどうかは、
 * 使用しているトランスポートに依存するので、注意が必要である。
 * </p>
 * <h3>動的RPC</h3>
 * <p>
 * RPCは原則として対象のインターフェースを指定して行うが
 * 例外的に対象のインターフェースが、実行時まで不明な場合がある。
 * 例えば、デバッグなどで実行時に人間が、メソッドや引数を指定する場合である。
 * この場合は、人間はインターフェースを知っているが、呼び出し側のプログラムには
 * インターフェースが存在しないことがある。
 * このような場合のために、動的な呼び出しを用意している。
 * 以下のように使用する。
 * </p>
 * <pre>
 * try {
 *     Object r = rpcInvoker.rcall(targetObjectId,targetEndpoint,"appMethod",arg1,arg2);
 * } catch (Throwable t) {
 *     // 例外処理
 * }
 * </pre>
 * <p>
 * 返り値は常にObject型となる。intなど基本型はボクシングが行われてInteger型などになる。
 * 例外は、何が発生するか不明なので、Throwableを受けなければいけない。
 * </p>
 * <p>
 * <h3>その他</h3>
 * <p>
 * getStubには、いくつかバリエーションが存在する。
 * timeoutを指定できるもの、RPCModeを指定できるもの、
 * targetObjectId,targetEndpointの代わりにCallerrIdを指定するものが存在する。
 * timeoutが指定できないものでは、GTransConfigValues.rpcTimeoutが使用される。
 * RPCModeを指定できるものでは、指定によりoneway RPCかどうかの決定方法を
 * 以下のように変更可能である。
 * <ul>
 *　<li>AUTOならば、Annotationにより決定する。(デフォルト)</li>
 *  <li>SYNCならば、常に同期型である。</li>
 *  <li>ONEWAYならば、常にOnewayである。</li>
 * </ul>
 * 詳細は、各メソッドの説明を見て欲しい。
 * </p>
 * <h3>ディスカバリコール</h3>
 * <p>
 * ディスカバリコールは、getListとgetDCStub組み合わせて以下のように行う。
 * </p>
 * <pre>
 * List&lt;Integer&gt; r = getList(getDCStub("$location in rect(2.0,2.0)",AppIf.class)
 *                    .appMethod(arg1,arg2));
 * </pre>
 * <p>
 * ここで"$location in rect(2.0,2.0)"は、ディスカバリコールの検索条件である。
 * 呼び出されるメソッドの条件はRPCの場合と同じである。dcListと組み合わせるのは、
 * Javaの型システムのためである。
 * Listの型パラメータは呼び出しメソッドの返り値の型と同じである。
 * ただし、この例のように返り値が基本型である場合は、それをボクシングした型になる。
 * oneway呼び出しの場合は、値は返らない。
 * 呼び出し対象がvoidの場合は、getListを使用せずに以下のように行う。
 * </p>
 * <pre>
 * getDCStub("$location in rect(2.0,2.0)",AppIf.class)
 *                    .appVoidMethod(arg1,arg2);
 * </pre>
 * oneway呼び出しをされたくない場合は、以下のようにSYNCモード指定をしたgetDCStubを用いる。
 * <pre>
 * List&lt;Integer&gt; r = getList(getDCStub("$location in rect(2.0,2.0)",
 * 　　　　　　　　　　AppIf.class, RPCMode.SYNC).appMethod(arg1,arg2));
 * </pre>
 * <p>
 * 非同期ディスカバリコールは、
 * getFQとモード指定のgetDCStubを組み合わせて以下のように行う。
 * </p>
 * <pre>
 * FutureQueue&lt;Integer&gt; r = getFQ(getDCStub("$location in rect(2.0,2.0)",
 * 　　　AppIf.class,RPCMode.ASYNC).appMethod(arg1,arg2));
 * </pre>
 * <p>
 * ただし、呼び出し対象がvoidの場合は、getFQを別に呼び出して以下のように行う。
 * </p>
 * <pre>
 * getDCStub("$location in rect(2.0,2.0)",AppIf.class,RPCMode.ASYNC)
 *                    .appMethod(arg1,arg2);
 * FutureQueue&lt;Object&gt; r = getFQ();                   
 * </pre>
 * <p>
 * この場合、FutureQueueの型パラメータは、常にObjectでなければいけない。
 * 呼び出しが実際に非同期になるかどうかは、
 * 使用しているトランスポートに依存しているので注意が必要である。
 * </p>
 * <p>
 * onewayディスカバリコールは、モード指定のgetStubを用いて以下のように行う。
 * </p>
 * <pre>
 * getDCStub("$location in rect(2.0,2.0)",AppIf.class,RPCMode.ONEWAY)
 *                    .appVoidMethod(arg1,arg2);
 * </pre>
 * <p>
 * onewayでは、返り値を受け取ることは出来ない。
 * 呼び出しが実際に非同期になるかどうかは、
 * 使用しているトランスポートに依存しているので注意が必要である。
 * </p>
 * <h3>CalleeId</h3>
 * <p>
 * リモートのエージェントのIdとしてCalleeIdを用意している。
 * CalleeIdは、以下のようにRPCのtargetAgentId, targetPeerの代わりに用いることができる。
 * getCalleeId()を使って、そのエージェントのCalleeIdを得ることができる。
 * </p>
 * <pre>
 * CalleeId cid;
 * ...
 * int n = getStub(AppIf.class,cid).appMethod(arg1,arg2);
 * </pre>
 */
public abstract class Agent implements AgentIf {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(Agent.class);

    protected AgentId id;
    protected String name;
    protected long createTime;
    protected PeerId motherPeerId;
    transient private AgentContainer agContainer = null;
    protected AgentAttribs attribs;
    
    /**
     * AgentPeerから作成されたAgentは
     * AgentPeerを返す。
     * そうでない場合は、nullをかえす。
     * @return AgentPeerを返す。
     */
    public AgentPeer getAgentPeer() {
        return getHome().getAgentPeer();
    }
    
    protected Agent() {
        id = AgentId.newId();
    }
    
    public CalleeId getCalleeId() {
        try {
            return getHome().getCalleeId(getId());
        } catch (NoSuchAgentException e) {
            logger.error("myself is no exist. should not happen",e);
            return null;
        }
    }

    public void _$init(AgentHomeImpl home, String name) {
        this.name = name;

        createTime = System.currentTimeMillis();
        motherPeerId = home.getPeerId();
        try {
            attribs = new AgentAttribs(home.comb.table, id);
        } catch (IllegalStateException ignore) {
            logger.error("", ignore);
        } catch (IdConflictException ignore) {
            logger.error("", ignore);
        }
    }

    public void _$loadInit(AgentHomeImpl home) {
        attribs.onLoad(home.comb.table);
    }

    public void _$cloneInit(AgentHomeImpl home, AgentId agId) {
        id = agId;
        attribs.onClone(home.comb.table, agId);
        createTime = System.currentTimeMillis();
    }

    public void _$bindAndActivate(AgentContainer agContainer,
            InitialAgentMode mode) {
        this.agContainer = agContainer;
        attribs.attach();
        if (mode == InitialAgentMode.CREATION) {
            onCreation();
        }
    }

    public AgentAttribs _$getAttribs() {
        return attribs;
    }

    protected AgentContainer _$getContainer() {
        return agContainer;
    }
    
    public final AgentId getId() {
        return id;
    }

    public final String getName() {
        return name;
    }

    public final boolean giveName(String name) {
        if (this.name != null) {
            return false;
        }
        this.name = name;
        agContainer.giveAgentName(name);
        return true;
    }

    public final String getFullName() {
        return name + "@" + motherPeerId;
    }

    public final long getCreationTime() {
        return createTime;
    }

    protected final PeerId getMotherPeerId() {
        return motherPeerId;
    }

    protected final AgentHome getHome() {
        return agContainer.getHome();
    }

    /* --- attributes handling --- */

    public boolean setAttrib(String name, Object value)
            throws IllegalArgumentException, IncompatibleTypeException {
        return attribs.setAttrib(name, value);
    }

    public boolean setAttrib(String name, Object value, boolean useIndex)
            throws IllegalArgumentException, IncompatibleTypeException {
        return attribs.setAttrib(name, value, useIndex);
    }

    public boolean removeAttrib(String name) {
        return attribs.removeAttrib(name);
    }

    public Object getAttribValue(String attribName) {
        return attribs.getAttribValue(attribName);
    }

    public List<String> getAttribNames() {
        return attribs.getAttribNames();
    }
    
    public boolean isIndexed(String attribName) throws IllegalArgumentException {
        return attribs.isIndexed(attribName);
    }

    public final void destroy() {
        // TODO loc
        if (agContainer != null && !agContainer.isSleeping()) {
            try {
                // onDestruction();
                agContainer.destroy();
            } catch (IllegalAgentModeException e) {
                // never occur
            }
        }
    }

    
    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * agentIdを持つエージェントのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param agId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, int timeout, RPCMode rpcMode) {
        return getHome().getStub(clz,agId,remotePeer,timeout,rpcMode);
    }

    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * agentIdを持つエージェントのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param agId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, RPCMode rpcMode) {
        return getHome().getStub(clz,agId,remotePeer,rpcMode);
    }

    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * agentIdを持つエージェントのメソッドを呼び出すためのstubを返す。
     * onewayかどうかは、Annotationにより決定される。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param agId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @param timeout timeout値（msec）
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, int timeout) {
        return getHome().getStub(clz,agId,remotePeer,timeout);
    }

    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * agentIdを持つエージェントのメソッドを呼び出すためのstubを返す。
     * onewayかどうかは、Annotationにより決定される。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param agId RPC呼び出しの対象となるオブジェクトのId
     * @param remotePeer リモートピアを示すEndpoint
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer) {
        return getHome().getStub(clz,agId,remotePeer);
    }

    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで示すエージェントのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param cid RPC呼び出しの対象となるエージェントのCalleeId
     * @param timeout timeout値（msec）
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid, int timeout,
            RPCMode rpcMode) {
        return getHome().getStub(clz,cid,timeout,rpcMode);
    }

    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで示すエージェントのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param cid RPC呼び出しの対象となるエージェントのCalleeId
     * @param rpcMode Onewayかどうかを指定する。
     * 　　　　AUTOならば、Annotationにより決定する。
     *        SYNCならば、常に同期型である。
     *        ONEWAYならば、常にOnewayである。
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid,
            RPCMode rpcMode) {
        return getHome().getStub(clz,cid,rpcMode);
    }

    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで示すエージェントのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param cid RPC呼び出しの対象となるエージェントのCalleeId
     * @param timeout timeout値（msec）
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid, int timeout) {
        return getHome().getStub(clz,cid,timeout);
    }

    /**
     * リモートピア上の、clz型のインターフェースを実装し、
     * cidで示すエージェントのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param cid RPC呼び出しの対象となるエージェントのCalleeId
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid) {
        return getHome().getStub(clz,cid);
    }

    /**
     * 同一ピア上の、clz型のインターフェースを実装し、
     * agIdで示すエージェントのメソッドを呼び出すためのstubを返す。
     * 
     * @param clz RPC呼び出しの対象となるエージェントが実装しているインターフェース
     * @param agId RPC呼び出しの対象となるエージェントのエージェントId
     * @return RPCのためのstub
     */
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId) {
        return getHome().getStub(clz,agId);
    }

    public Object rcall(AgentId agId, Endpoint remotePeer, int timeout,
            RPCMode rpcMode, String method, Object... args) throws Throwable {
        return getHome().rcall(agId,remotePeer,timeout,rpcMode,method,args);
    }

    public Object rcall(AgentId agId, Endpoint remotePeer, RPCMode rpcMode,
            String method, Object... args) throws Throwable {
        return getHome().rcall(agId,remotePeer,rpcMode,method,args);
    }

    public Object rcall(AgentId agId, Endpoint remotePeer, int timeout,
            String method, Object... args) throws Throwable {
        return getHome().rcall(agId, remotePeer, timeout,method,args);
    }

    public Object rcall(AgentId agId, Endpoint remotePeer, String method,
            Object... args) throws Throwable {
        return getHome().rcall(agId, remotePeer, method,args);
    }

    public Object rcall(CalleeId cid, int timeout, RPCMode rpcMode,
            String method, Object... args) throws Throwable {
        return getHome().rcall(cid, timeout, rpcMode,method,args);
    }

    public Object rcall(CalleeId cid, RPCMode rpcMode, String method,
            Object... args) throws Throwable {
        return getHome().rcall(cid, rpcMode, method,args);
    }

    public Object rcall(CalleeId cid, int timeout, String method,
            Object... args) throws Throwable {
        return getHome().rcall(cid, timeout, method,args);
    }

    public Object rcall(CalleeId cid, String method, Object... args)
            throws Throwable {
        return getHome().rcall(cid, method, args);
    }

    /*--- discovery call with stub ---*/

    /**
     * discoveryCall用のスタブを取得する。
     * rpcModeには、以下が指定可能である。
　　 * <ul>
　　 *　<li>AUTOならば、Annotationにより決定する。(デフォルト)</li>
　　 *  <li>SYNCならば、常に同期型である。</li>
　　 *  <li>ASYNCならば、常に非同期型である。</li>
　　 *  <li>ONEWAYならば、常にOnewayである。</li>
　　 * </ul>
     * ONEWAY以外ではスタブは、明示的には使用せず
     * 必ず以下のようにgetListやgetFQとともに使用する。
     * <pre>
     * //同期型
     * List<String> r = getList(getDCStub(App.class,"$location in rect(10.0,11.0)")
     *                    .appMethod(arg),RPCMode.SYNC);
     * //非同期型
     * FutureQueue<String> r = getFQ(getDCStub(App.class,"$location in rect(10.0,11.0)")
     *                    .appMethod(arg),RPCMode.ASYNC);
     * </pre>
     * ただし、呼び出しメソッドの型がvoidの場合は、getListやgetFQを使用せず以下のようにする。
     * <pre>
     * getDCStub(App.class,"$location in rect(10.0,11.0)").appMethod(arg);
     * </pre>
     * @param queryCond 条件
     * @param clz 対象のエージェントが実装している、RPCIfのサブ・インターフェース。
     * @param rpcMode 呼び出しモード
     */
    public <S extends RPCIf> S getDCStub(
            String queryCond,Class<S> clz, RPCMode rpcMode) {
        return getHome().getDCStub(queryCond,clz,rpcMode);
    }

    /**
     * discoveryCall用のスタブを取得する。
     * getStub(queryCond, clz, rpcMode)において、
     * rpcModeにRPCMode.AUTOを指定した場合と同じである。
     * @param queryCond 条件
     * @param clz 対象のエージェントが実装している、RPCIfのサブ・インターフェース。
     */
    public <S extends RPCIf> S getDCStub(
            String queryCond,Class<S> clz) {
        return getHome().getDCStub(queryCond,clz);
    }

    /**
     * 同期指定のdiscoveryCall用のスタブと共に用いて、
     * 同期のdiscoveryCallを行う。
     * @param discoveryCall 引数の部分には、同期指定のdiscoveryCall用のスタブの呼び出しを記述する。
     */
    public <T> List<T> getList(T discoveryCall) {
        return getHome().getList(discoveryCall);
    }

    /**
     * 非同期指定のdiscoveryCall用のスタブと共に用いて、
     * 非同期のdiscoveryCallを行う。
     * @param discoveryCallAsync 引数の部分には、非同期指定のdiscoveryCall用のスタブの呼び出しを記述する。
     */
    public <T> FutureQueue<T> getFQ(T discoveryCallAsync) {
        return getHome().getFQ(discoveryCallAsync);
    }

    /**
     * 非同期指定のdiscoveryCall用のスタブと共に用いて、
     * 非同期のdiscoveryCallの結果を得る。
     */
    public FutureQueue<Object> getFQ() {
        return getHome().getFQ();
    }

    /*--- synchronous discovery call ---*/

    @Override
    public Object[] discoveryCall(String queryCond, String method,
            Object... args) {
        return discoveryCall(queryCond, null, method, args);
    }

    @Override
    public Object[] discoveryCall(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args) {
        return agContainer.getHome().discoveryCall(
                queryCond, clazz, method, args);
    }

    /*--- asynchronous discovery call ---*/

    public FutureQueue<?> discoveryCallAsync(String queryCond,
            String method, Object... args) {
        return discoveryCallAsync(queryCond, null, method, args);
    }

    public FutureQueue<?> discoveryCallAsync(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args) {
        return agContainer.getHome().discoveryCallAsync(
                queryCond, clazz, method, args);
    }

    /*--- oneway discovery call ---*/

    public void discoveryCallOneway(String queryCond, String method,
            Object... args) {
        discoveryCallOneway(queryCond, null, method, args);
    }

    public void discoveryCallOneway(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args) {
        agContainer.getHome().discoveryCallOneway(queryCond, 
                clazz, method, args);
    }

    public void onCreation() {
    }

    public void onDestruction() {
    }
}
