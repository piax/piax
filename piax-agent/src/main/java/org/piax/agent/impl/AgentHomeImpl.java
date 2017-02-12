/*
 * AgentHomeImpl.java - An implementation of agent home
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
 * $Id: AgentHomeImpl.java 1255 2015-08-02 13:14:50Z teranisi $
 */

package org.piax.agent.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.piax.agent.Agent;
import org.piax.agent.AgentAccessDeniedException;
import org.piax.agent.AgentCapabilityException;
import org.piax.agent.AgentConfigValues;
import org.piax.agent.AgentException;
import org.piax.agent.AgentHome;
import org.piax.agent.AgentHomeListener;
import org.piax.agent.AgentId;
import org.piax.agent.AgentIf;
import org.piax.agent.AgentInstantiationException;
import org.piax.agent.AgentPeer;
import org.piax.agent.IllegalAgentModeException;
import org.piax.agent.InvalidAgentJarException;
import org.piax.agent.MobileAgent;
import org.piax.agent.NoSuchAgentException;
import org.piax.agent.PersistentAgent;
import org.piax.agent.impl.asm.ByteCodes;
import org.piax.agent.impl.loader.AgentLoader;
import org.piax.agent.impl.loader.AgentLoaderPool;
import org.piax.agent.impl.loader.DefaultAgentLoader;
import org.piax.agent.impl.loader.DefaultAgentLoaderMgr;
import org.piax.common.CalleeId;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.StatusRepo;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.common.attribs.RowData;
import org.piax.common.dcl.parser.ParseException;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.IllegalRPCAccessException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCHook;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RPCMode;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.combined.CombinedOverlay;
import org.piax.util.ByteUtil;
import org.piax.util.ClassUtil;
import org.piax.util.MethodUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of agent home
 */
public class AgentHomeImpl implements AgentHome, OverlayListener<Destination, Key> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(AgentHomeImpl.class);
    
    public static final ObjectId DEFAULT_OBJ_ID = new ObjectId("agentHome");
    public static TransportId COMBINED_OV_ID = new TransportId("agentCombined");
    public static TransportId RPC_ID = new TransportId("agentRPC");
    
    private final PeerId peerId;
    protected final DefaultAgentLoaderMgr defaultAgLoaderMgr;
    private final AgentLoaderPool loaderPool;
    protected final AgentsShelf agContainers;
    protected AgentHomeListener listener = null;
    
    /*--- 通信関係 ---*/
    public final CombinedOverlay comb;
    private final AgentRPCInvoker<Endpoint> rpc;
    
    /** peerに属性をセットするためのRowData */
    private final RowData peerRow;
    
    /**
     * AgentPeerから作成した際に、AgentPeerを保持する
     */
    private AgentPeer agentPeer = null;
    
    /**
     * AgentPeerを返す。
     */
    public AgentPeer getAgentPeer() {
        return agentPeer;
    }
    
    /**
     * CalleeIdを返す。
     * @throws NoSuchAgentException an exception occurs if no such agent
     */
    public CalleeId getCalleeId(AgentId agentId) throws NoSuchAgentException {
           return new CalleeId(agentId, rpc.getEndpoint(),
                   getAgentContainer(agentId).getAgentName());
    }
    
    public void setListener(AgentHomeListener listener) {
        this.listener = listener;
    }
    
    public AgentHomeListener getListener() {
        return listener;
    }

    public interface AgentRPCIf extends RPCIf {
        @RemoteCallable
        AgentId load(byte[] jar, InitialAgentMode mode)
                throws RPCException, InvalidAgentJarException, ObjectStreamException,
                ClassNotFoundException, AgentAccessDeniedException;
    }
    
    class AgentRPCInvoker<E extends Endpoint> extends RPCInvoker<AgentRPCIf, E>
            implements AgentRPCIf {
        private AgentRPCInvoker(TransportId transId, ChannelTransport<E> trans)
                throws IdConflictException, IOException {
            super(transId, trans);
        }

        @Override
        public void registerRPCObject(ObjectId objId, RPCIf obj)
                throws IdConflictException {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public boolean unregisterRPCObject(ObjectId objId, RPCIf obj) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RPCIf getRPCObject(ObjectId agId) {
            if (agId.equals(objId)) return this;
            try {
                AgentContainer agc = getAgentContainer((AgentId) agId);
                return agc.getAgent();
            } catch (NoSuchAgentException e) {
                return null;
            } catch (IllegalAgentModeException e) {
                return null;
            }
        }
        
        public AgentId load(byte[] jar, InitialAgentMode mode)
                throws InvalidAgentJarException, 
                ObjectStreamException, ClassNotFoundException,
                AgentAccessDeniedException {
            return AgentHomeImpl.this.remoteLoad(jar, mode);
        }
    }
    
    public AgentHomeImpl(AgentPeer agentPeer, ChannelTransport<?> trans, 
            File... agClassPath) throws IOException, IdConflictException {
        this(agentPeer, trans, AgentConfigValues.defaultParentAgentLoader, agClassPath);
    }
    
    public AgentHomeImpl(ChannelTransport<?> trans, 
            File... agClassPath) throws IOException, IdConflictException {
        this(null, trans, AgentConfigValues.defaultParentAgentLoader, agClassPath);
    }

    public AgentHomeImpl(ChannelTransport<?> trans, ClassLoader parentAgentLoader,
        File... agClassPath) throws IOException, IdConflictException {
        this(null,trans,parentAgentLoader,agClassPath);
    }

    @SuppressWarnings("unchecked")
    public AgentHomeImpl(AgentPeer agentPeer, ChannelTransport<?> trans, ClassLoader parentAgentLoader,
            File... agClassPath) throws IOException, IdConflictException {
        this.agentPeer = agentPeer;
        peerId = agentPeer.getPeerId();
        defaultAgLoaderMgr = new DefaultAgentLoaderMgr(parentAgentLoader, agClassPath);
        loaderPool = new AgentLoaderPool(parentAgentLoader);
        agContainers = new AgentsShelf();
        comb = new CombinedOverlay(Peer.getInstance(peerId), COMBINED_OV_ID);
        comb.setListener(DEFAULT_OBJ_ID, this);
        peerRow = comb.setSuperRow(peerId);
        rpc = new AgentRPCInvoker<Endpoint>(RPC_ID, (ChannelTransport<Endpoint>) trans);
    }

    public ChannelTransport<?> getRPCTransport() {
        return rpc.getTransport();
    }
    
    public void changeRPCTransport(ChannelTransport<?> trans) {
        rpc.changeTransport(trans);
    }
    
    public CombinedOverlay getCombinedOverlay() {
        return comb;
    }
    
    public void fin() {
        // destroy all active agents
        Set<AgentContainer> agcs = agContainers.getAll();
        for (AgentContainer agc : agcs) {
            if (!agc.isSleeping()) {
                try {
                    agc.destroy();
                } catch (IllegalAgentModeException e) {
                    logger.error("", e);
                }
            }
        }
        comb.fin();
        rpc.fin();
    }
    
    /**
     * Agent ID指定のCommPortを生成する。
     * 
     */
//    public CommPort newCommPort(AgentId agId, CommPortReceiver receiver) 
//    throws MagicNumberConflictException {
//        return new CommPort(agId.getBytes(), msgHub, receiver);
//    }
    
    public void onCall(Agent ag, int mode) {
        switch (mode) {
        case DESTRUCTION:
            ag.onDestruction();
            break;
        case SLEEPING:
            ((PersistentAgent)ag).onSleeping();
            break;
        case DUPLICATING:
            ((PersistentAgent)ag).onDuplicating();
            break;
        case SAVING:
            ((PersistentAgent)ag).onSaving();
            break;
        case DEPARTURE:
            ((MobileAgent)ag).onDeparture();
            break;
        }
        if (listener != null) {
            switch (mode) {
            case DESTRUCTION:
                listener.onDestruction(ag.getId());
                break;
            case SLEEPING:
                listener.onSleeping(ag.getId());
                break;
            case DUPLICATING:
                listener.onDuplicating(ag.getId());
                break;
            case SAVING:
                listener.onSaving(ag.getId());
                break;
            case DEPARTURE:
                listener.onDeparture(ag.getId());
                break;
            }
        }
    }

    /* peer info */

    public PeerId getPeerId() {
        return peerId;
    }

    public StatusRepo getStatusRepo() throws IOException {
        return Peer.getInstance(peerId).getStatusRepo();
    }

    /* agents info */

    String makeAgentJarPath(AgentId agentId, boolean isSleep) {
        if (isSleep) {
            return AgentHome.AG_SLEEP_DIR + File.separator + agentId + ".jar";
        } else {
            return AgentHome.AG_SAVE_DIR + File.separator + agentId + ".jar";
        }
    }
    
    public Set<AgentId> getAgentIds() {
        return agContainers.getAllById();
    }
    
    private Set<AgentId> toAgentIds(Set<AgentContainer> agcs) {
        Set<AgentId> agIds = new HashSet<AgentId>();
        for (AgentContainer agc : agcs) {
            agIds.add(agc.getAgentId());
        }
        return agIds;
    }
    
    public Set<AgentId> getAgentIds(String clazz) {
        return toAgentIds(agContainers.matches(clazz));
    }

    public String getAgentName(AgentId agentId)
            throws AgentException {
        AgentContainer agc = agContainers.get(agentId);
        if (agc == null) {
            throw new NoSuchAgentException();
        }
        return agc.getAgentName();
    }

    public Class<? extends AgentIf> getAgentClass(AgentId agentId)
            throws AgentException {
        AgentContainer agc = agContainers.get(agentId);
        if (agc == null) {
            throw new NoSuchAgentException();
        }
        return agc.getAgentClass();
    }

    public boolean giveAgentName(AgentId agentId, String name)
            throws AgentException {
        AgentContainer agc = agContainers.get(agentId);
        if (agc == null) {
            throw new NoSuchAgentException();
        }
        return agc.getAgent().giveName(name);
    }

    public boolean isAgentSleeping(AgentId agentId)
            throws AgentException {
        AgentContainer agc = agContainers.get(agentId);
        if (agc == null) {
            throw new NoSuchAgentException();
        }
        return agc.isSleeping();
    }

    /* remote/local agent method call */


    /* handling agents */
    
    protected Agent newAgent(String clazz, String name,
            ClassLoader loader)
            throws ClassNotFoundException, AgentInstantiationException {
        // load class
        Class<?> agc = Class.forName(clazz, false, loader);
        Agent ag;
        try {
            ag = (Agent) agc.newInstance();
        } catch (ClassCastException e) {
            throw new AgentInstantiationException("Specified clazz is not Agent");
        } catch (InstantiationException e) {
            throw new AgentInstantiationException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new AgentInstantiationException(e.getMessage());
        } catch (ExceptionInInitializerError e) {
            throw new AgentInstantiationException(
                    "Static initializer causes error : "
                            + e.getCause().getMessage());
        }
        ag._$init(this, name);
        return ag;
    }

    // is not private as called by AgentContainer
    public AgentId load(byte[] jar, InitialAgentMode mode)
            throws InvalidAgentJarException, 
            ObjectStreamException, ClassNotFoundException {
        LoadImage image = new LoadImage(jar);
        return doLoad(image, mode);
    }
    
    // is not private as called by AgentContainer
    public AgentId remoteLoad(byte[] jar, InitialAgentMode mode)
            throws InvalidAgentJarException, 
            ObjectStreamException, ClassNotFoundException,
            AgentAccessDeniedException {
        LoadImage image = new LoadImage(jar);
        if (listener != null) {
            if (!listener.onArriving(image.agentClassName, image.agentId,
                    image.agentName, rpc.getSrcPeerId())) {
                throw new AgentAccessDeniedException("Arriving Agent denied");
            }
        }
        return doLoad(image, mode);
    }
    
    private AgentId doLoad(LoadImage image, InitialAgentMode mode)
            throws InvalidAgentJarException, 
            ObjectStreamException, ClassNotFoundException {
        logger.trace("ENTRY:");
        try {
            image.loadLoadImage(loaderPool);
            AgentId agId;
            // new AgentId. when DUPLICATION 
            if (mode == InitialAgentMode.DUPLICATION) {
                agId = AgentId.newId();
                image.agent._$cloneInit(this, agId);
            } else {
                image.agent._$loadInit(this);
            }
            agId = image.agent.getId();
            AgentContainer agc = agContainers.get(agId);
            if (agc == null) {
                if (mode == InitialAgentMode.WAKEUP) {
                    logger.warn("AgentContainer is lost");
                }
                agc = new AgentContainer(this, image);
                agContainers.add(agc);
            } else {
                if (mode == InitialAgentMode.DUPLICATION
                        || mode == InitialAgentMode.ARRIVAL) {
                    logger.warn("Unexpected AgentContainer exists");
                }
                // if the agent already exists, replace it
                // TODO check!
//            agContainers.remove(agId);
//            agc.detachAgent();
                agc.reload(image);
//            agContainers.add(agc);
            }
            if (mode == InitialAgentMode.ARRIVAL) {
                ((MobileAgent) image.agent)._$addPeer(getPeerId());
            }
            agc.attachAgent(this, mode);
            if (listener != null) {
                switch (mode) {
                case ARRIVAL:
                    listener.onArrival(agId);
                    break;
                case DUPLICATION:
                    listener.onDuplication(agId);
                    break;
                case WAKEUP:
                    listener.onWakeup(agId);
                    break;
                case RESTORE:
                    listener.onRestore(agId);
                    break;
                default:
                    // do nothing
                    break;
                }
            }
            return agId;
        } finally {
            logger.trace("EXIT:");
        }
    }

    public AgentId load(PeerId toPeer, byte[] jar, InitialAgentMode mode)
            throws IOException,
            InvalidAgentJarException, 
            ObjectStreamException, ClassNotFoundException,
            AgentAccessDeniedException {
        try {
            AgentRPCIf stub = rpc.getStub(toPeer);
            return stub.load(jar, mode);
        } catch (RPCException e) {
            Throwable cause = e.getCause();
            if (cause != null && cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException(e);
        } catch (RuntimeException e) {
            logger.error("", e);
            throw e;
        }
    }
    
    public AgentId createAgent(String clazz, String name)
            throws ClassNotFoundException, AgentInstantiationException {
        
        DefaultAgentLoader defaultLoader = 
            defaultAgLoaderMgr.getDefaultAgentLoader();
        ByteCodes bcodes = defaultLoader.getClassByteCodes(clazz);
        // get suitable loader
        AgentLoader loader = 
            loaderPool.getAccecptableLoaderAsAdded(clazz, bcodes);

        // create the Agent instance and the container
        Agent agent = newAgent(clazz, name, loader);
        LoadImage image = new LoadImage(agent, bcodes, loader);
        AgentContainer agc = new AgentContainer(this, image);
        agContainers.add(agc);
        agc.attachAgent(this, InitialAgentMode.CREATION);
        AgentId agId = agc.getAgentId();
        if (listener != null) {
            listener.onCreation(agId);
        }
        return agId;
    }

    public AgentId createAgent(Class<? extends Agent> clazz, 
            String name)
            throws AgentInstantiationException {
        /*
         * TODO optimize
         */
        String className = clazz.getName();
        
        DefaultAgentLoader defaultLoader = 
            defaultAgLoaderMgr.getDefaultAgentLoader();
        ByteCodes bcodes = defaultLoader.getClassByteCodes(className);
        // get suitable loader
        AgentLoader loader = 
            loaderPool.getAccecptableLoaderAsAdded(className, bcodes);

        // create the Agent instance and the container
        Agent agent = null;
        try {
            agent = newAgent(className, name, loader);
        } catch (ClassNotFoundException ignore) {}
        LoadImage image = new LoadImage(agent, bcodes, loader);
        AgentContainer agc = new AgentContainer(this, image);
        agContainers.add(agc);
        agc.attachAgent(this, InitialAgentMode.CREATION);
        AgentId agId = agc.getAgentId();
        if (listener != null) {
            listener.onCreation(agId);
        }
        return agId;
    }

    public AgentId createAgent(String clazz) 
            throws ClassNotFoundException, AgentInstantiationException {
        return createAgent(clazz, null);
    }
    
    public AgentId createAgent(Class<? extends Agent> clazz) 
            throws AgentInstantiationException {
        return createAgent(clazz, null);
    }

    // called by Agent
    protected boolean removeAgContainer(AgentId agId) {
        return agContainers.remove(agId);
    }
    
    // also public to overlays
    public final AgentContainer getAgentContainer(AgentId agentId) 
            throws NoSuchAgentException {
        AgentContainer agc = agContainers.get(agentId);
        if (agc == null) {
            throw new NoSuchAgentException();
        }
        return agc;
    }
    
    public void destroyAgent(AgentId agentId) 
            throws AgentException {
        AgentContainer agc = getAgentContainer(agentId);
        agc.getAgent().destroy();
    }

    public void sleepAgent(AgentId agentId) 
            throws AgentException, ObjectStreamException, IOException {
        AgentContainer agc = getAgentContainer(agentId);
        Agent ag = agc.getAgent();
        if (!(ag instanceof PersistentAgent)) {
            throw new AgentCapabilityException();
        }
        ((PersistentAgent) ag).sleep();
    }
    
    public void wakeupAgent(AgentId agentId) 
    throws AgentException, ObjectStreamException {
        AgentContainer agc = getAgentContainer(agentId);
        if (!agc.hasPersistency()) {
            throw new AgentCapabilityException();
        }
        agc.wakeup();
    }

    public AgentId duplicateAgent(AgentId agentId) 
            throws AgentException, ObjectStreamException {
        AgentContainer agc = getAgentContainer(agentId);
        if (!agc.hasPersistency()) {
            throw new AgentCapabilityException();
        }
        return agc.duplicate();
    }

    public void saveAgent(AgentId agentId) 
            throws AgentException, ObjectStreamException, IOException {
        saveAgent(agentId, null);
    }

    public void saveAgent(AgentId agentId, File fname)
            throws AgentException, ObjectStreamException, IOException {
        AgentContainer agc = getAgentContainer(agentId);
        if (!agc.hasPersistency()) {
            throw new AgentCapabilityException();
        }
        agc.save(fname);
    }

    public void restoreAgent(AgentId agentId) 
            throws AgentException, ClassNotFoundException,
            ObjectStreamException, IOException {
        byte[] jar;
        try {
            String jarPath = makeAgentJarPath(agentId, false);
            jar = getStatusRepo().restoreBytes(jarPath);
            getStatusRepo().delete(jarPath);
        } catch (FileNotFoundException e) {
            throw new NoSuchAgentException();
        } catch (IOException e) {
            throw new NoSuchAgentException();
        }
        if (jar == null) {
            throw new NoSuchAgentException("StatusRepo is null");
        }
        try {
            load(jar, InitialAgentMode.RESTORE);
        } catch (InvalidAgentJarException e) {
            // this exception will not occur
            logger.error("", e);
        }
    }

    public AgentId restoreAgent(File fname) 
            throws FileNotFoundException, InvalidAgentJarException, 
            ClassNotFoundException, ObjectStreamException, IOException {
        byte[] jar = ByteUtil.file2Bytes(fname);
        fname.delete();
        return load(jar, InitialAgentMode.RESTORE);
    }

    public void travelAgent(AgentId agentId, PeerId peerId) 
            throws AgentException, ClassNotFoundException,
            ObjectStreamException, IOException {
        AgentContainer agc = getAgentContainer(agentId);
        if (!agc.hasMobility()) {
            throw new AgentCapabilityException();
        }
        agc.travel(peerId);
    }
    
    /*** handling attributes ***/
    
    public void declareAttrib(String attribName)
            throws IllegalStateException {
        comb.declareAttrib(attribName);
    }

    public void declareAttrib(String attribName, Class<?> type)
            throws IllegalStateException {
        comb.declareAttrib(attribName, type);
    }

    public List<String> getDeclaredAttribNames() {
        return comb.getDeclaredAttribNames();
    }

    public void bindOverlay(String attribName, TransportId... suffix)
            throws IllegalArgumentException, NoSuchOverlayException,
            IncompatibleTypeException {
        bindOverlay(attribName, new TransportIdPath(suffix));
    }

    public void bindOverlay(String attribName, TransportIdPath suffix)
            throws IllegalArgumentException, NoSuchOverlayException,
            IncompatibleTypeException {
        comb.bindOverlay(attribName, suffix);
    }
    
    public void unbindOverlay(String attribName)
            throws IllegalArgumentException, IllegalStateException {
        comb.unbindOverlay(attribName);
    }

    public Overlay<?, ?> getBindOverlay(String attribName)
            throws IllegalArgumentException {
        return comb.getBindOverlay(attribName);
    }
    
    /**
     * peerの持つRowDataのrowIdを返す。
     * 
     * @return peerの持つRowDataのrowId
     */
    public Id getPeerRowId() {
        return peerRow.rowId;
    }

    /**
     * {@inheritDoc}
     */
    public boolean setAttrib(String name, Object value)
            throws IllegalArgumentException, IncompatibleTypeException {
        return peerRow.setAttrib(name, value);
    }

    /**
     * {@inheritDoc}
     */
    public boolean setAttrib(String name, Object value, boolean useIndex)
            throws IllegalArgumentException, IncompatibleTypeException {
        return peerRow.setAttrib(name, value, useIndex);
    }

    /**
     * {@inheritDoc}
     */
    public boolean removeAttrib(String name) {
        return peerRow.removeAttrib(name);
    }

    /**
     * {@inheritDoc}
     */
    public Object getAttribValue(String attribName) {
        return peerRow.getAttribValue(attribName);
    }

    /**
     * {@inheritDoc}
     */
    public List<String> getAttribNames() {
        return peerRow.getAttribNames();
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean isIndexed(String attribName) throws IllegalArgumentException {
        return peerRow.isIndexed(attribName);
    }

    /**** discovery call ****/
    
    /*--- synchronous discovery call ---*/

    public Object[] discoveryCall(String queryCond, String method,
            Object... args) {
        return discoveryCall(queryCond, null, method, args);
    }

    public Object[] discoveryCall(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args) {
        // RPCHook patch by sho (13/10/10)
        if (RPCHook.hook != null) {
            RPCHook.RValue rv = RPCHook.hook.callerHook(
                    RPCHook.CallType.DC_SYNC, null, queryCond, method, args);
            method = rv.method;
            args = rv.args;
        }
        FutureQueue<?> fq = discoveryCallAsync(queryCond, clazz, method, args);
        List<Object> values = new ArrayList<Object>();
        for (RemoteValue<?> rv : fq) {
            if (rv.getException() == null) {
                Object v = rv.getValue();
                values.add(v);
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new UndeclaredThrowableException(new InterruptedException());
        }
        return values.toArray();
    }

    /*--- asynchronous discovery call ---*/

    public FutureQueue<?> discoveryCallAsync(String queryCond,
            String method, Object... args) {
        return discoveryCallAsync(queryCond, null, method, args);
    }
    
    public FutureQueue<?> discoveryCallAsync(String dcond,
            Class<? extends AgentIf> clazz, String method, Object... args) {
    		return discoveryCallAsync(dcond, clazz, false, method, args); 
    }

    public FutureQueue<?> discoveryCallAsync(String dcond,
            Class<? extends AgentIf> clazz, boolean oneway, String method, Object... args) {
        logger.trace("ENTRY:");
        // RPCHook patch by sho (13/10/10)
     	if (RPCHook.hook != null) {
            RPCHook.RValue rv = RPCHook.hook.callerHook(
                    RPCHook.CallType.DC_ASYNC, null, dcond, method, args);
            method = rv.method;
            args = rv.args;
     	}
     	/*
     	 * 互換性を保つため発生したExceptionは、UndeclaredThrowableException
     	 * とする。
     	 */
        try {
            String clz = clazz == null ? null : clazz.getName();
            AgCallPack agCall = new AgCallPack(peerId, clz, method, args);
            return comb.request(DEFAULT_OBJ_ID, DEFAULT_OBJ_ID, dcond, agCall,
            					   new TransOptions(GTransConfigValues.rpcTimeout,
            							   			oneway ? ResponseType.NO_RESPONSE : ResponseType.AGGREGATE,
            							   			oneway ? RetransMode.NONE : RetransMode.SLOW));
        } catch (ProtocolUnsupportedException e) {
            throw new UndeclaredThrowableException(e);
        } catch (ParseException e) {
            throw new UndeclaredThrowableException(e);
        } catch (IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            logger.trace("EXIT:");
        }
    }

    /*--- oneway discovery call ---*/

    public void discoveryCallOneway(String queryCond, String method,
            Object... args) {
        discoveryCallOneway(queryCond, null, method, args);
    }

    public void discoveryCallOneway(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args) {
        // RPCHook patch by sho (13/10/10)
        if (RPCHook.hook != null) {
            RPCHook.RValue rv = RPCHook.hook.callerHook(
                    RPCHook.CallType.DC_ONEWAY, null, queryCond, method, args);
            method = rv.method;
            args = rv.args;
        }
        // Set oneway flag as true.
        discoveryCallAsync(queryCond, clazz, true, method, args);
    }
    
    public static class AgCallPack implements Serializable {
        private static final long serialVersionUID = -1L;

        public final String clazz;
        public String method;
        public Object[] args;
        public PeerId srcPeerId;

        public AgCallPack(PeerId srcPeerId, String clazz,
                String method, Object... args) {
            this.srcPeerId = srcPeerId;
            this.clazz = clazz;
            this.method = method;
            this.args = args;
        }
        public String toString() {
        		return "[class=" + clazz + ",method=" + method +",args=" + args + ",srcPeerId=" + srcPeerId + "]";
        }
    }

    public void onReceive(Overlay<Destination, Key> ov,
            OverlayReceivedMessage<Key> rmsg) {
        throw new UnsupportedOperationException();
    }
    
    private Object invoke0(AgCallPack agCall, AgentId agId)
            throws NoSuchMethodException, InvocationTargetException,
            NoSuchAgentException {
        AgentContainer agc = agContainers.get(agId);
        if (agc == null) {
            throw new NoSuchAgentException("No such agent");
        }
        Agent ag;
        try {
            ag = agc.getAgent();
        } catch (IllegalAgentModeException e) {
            // in case of sleep...
            throw new NoSuchAgentException("Illegal agent mode");
        }
        if (ag == null) {
            // in case of detach agent
            throw new NoSuchAgentException("detached agent");
        }
        Class<?> agClass = null;
        if (agCall.clazz != null) {
            try {
                agClass = Class.forName(agCall.clazz, false, agc.getAgentClassLoader());
            } catch (ClassNotFoundException e) {
                throw new NoSuchAgentException("type mismatch");
            }
        }
        if (agClass != null) {
            Class<?> tClass = agc.getAgentClass();
            if (!agClass.isAssignableFrom(tClass)) {
                throw new NoSuchAgentException("type mismatch");
            }
        }
        // RPCHook patch by sho (13/10/10)
        if (RPCHook.hook != null) {
            RPCHook.RValue rv = RPCHook.hook.calleeHook(agCall.method,
                    agCall.args);
            agCall.method = rv.method;
            agCall.args = rv.args;
        }
        return MethodUtil.invoke(ag, RPCIf.class,
                agCall.srcPeerId.equals(peerId), agCall.method, agCall.args);
    }

    public FutureQueue<?> onReceiveRequest(Overlay<Destination, Key> trans,
            OverlayReceivedMessage<Key> rmsg) {
        if (!(rmsg.getMessage() instanceof AgCallPack)) {
            logger.error("invalid payload accepted");
            return FutureQueue.emptyQueue();
        }
        FutureQueue<Object> fq = new FutureQueue<Object>();
        Collection<Key> matchedKeys = rmsg.getMatchedKeys();
        if (matchedKeys.size() > 0) {
            AgCallPack agCall = (AgCallPack) rmsg.getMessage();
            for (Key k : rmsg.getMatchedKeys()) {
                if (((RowData) k).rowId instanceof AgentId) {
                    AgentId agId = (AgentId) ((RowData) k).rowId;
                    Object ret;
                    try {
                        ret = invoke0(agCall, agId);
                        fq.add(new RemoteValue<Object>(peerId, ret));
                    } catch (NoSuchAgentException e) {
                        continue;
                    } catch (NoSuchMethodException e) {
                        continue;
                    } catch (IllegalRPCAccessException e) {
                        fq.add(new RemoteValue<Object>(peerId, e));
                    } catch (InvocationTargetException e) {
                        fq.add(new RemoteValue<Object>(peerId, e.getCause()));
                    }
                } else {
                    logger.error("illegal rowId type: {}",
                            ((RowData) k).rowId.getClass());
                }
            }
       }
        fq.setEOFuture();
        return fq;
    }

    // unused
    public void onReceive(Transport<Destination> trans, ReceivedMessage rmsg) {
    }
    public void onReceive(RequestTransport<Destination> trans,
            ReceivedMessage rmsg) {
    }
    public FutureQueue<?> onReceiveRequest(RequestTransport<Destination> trans,
            ReceivedMessage rmsg) {
        return null;
    }

    @SuppressWarnings("unchecked")
    public <S extends RPCIf> S getDCStub(
            String queryCond, Class<S> clz,RPCMode mode) {
        if (!clz.isInterface()) {
            throw new IllegalArgumentException("Specified class is not an interface");
        }
        ClassLoader loader = clz.getClassLoader();
        Class<?>[] ifs = ClassUtil.gatherLowerBoundSuperInterfaces(clz,
                RPCIf.class);
        DCInvocationHandler handler =
                new DCInvocationHandler(this,
                        (Class<? extends AgentIf>)clz,queryCond,mode);
        return (S) Proxy.newProxyInstance(loader, ifs, handler);
    }
    
    public <S extends RPCIf> S getDCStub(
            String queryCond,Class<S> clz) {
        return getDCStub(queryCond,clz,RPCMode.AUTO);
    }

    public <T> List<T> getList(T discoveryCall) {
        return DCInvocationHandler._$getResults();
    }

    public <T> FutureQueue<T> getFQ(T discoveryCallAsync) {
        return DCInvocationHandler._$getAsyncResults();
    }

    public FutureQueue<Object> getFQ() {
        return DCInvocationHandler._$getAsyncResults();
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, int timeout, RPCMode rpcMode) {
        return rpc.getStub(clz,agId,remotePeer,timeout,rpcMode);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, RPCMode rpcMode) {
        return rpc.getStub(clz,agId,remotePeer,rpcMode);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer, int timeout) {
        return rpc.getStub(clz,agId,remotePeer,timeout);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId,
            Endpoint remotePeer) {
        return rpc.getStub(clz,agId,remotePeer);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid, int timeout,
            RPCMode rpcMode) {
        return rpc.getStub(clz,cid,timeout,rpcMode);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid,
            RPCMode rpcMode) {
        return rpc.getStub(clz,cid,rpcMode);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid, int timeout) {
        return rpc.getStub(clz,cid,timeout);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, CalleeId cid) {
        return rpc.getStub(clz,cid);
    }

    @Override
    public <S extends RPCIf> S getStub(Class<S> clz, AgentId agId) {
        return rpc.getStub(clz,agId,rpc.getEndpoint());
    }

    @Override
    public Object rcall(AgentId agId, Endpoint remotePeer, int timeout,
            RPCMode rpcMode, String method, Object... args) throws Throwable {
        return rpc.rcall(agId,remotePeer,timeout,rpcMode,method,args);
    }

    @Override
    public Object rcall(AgentId agId, Endpoint remotePeer, RPCMode rpcMode,
            String method, Object... args) throws Throwable {
        return rpc.rcall(agId,remotePeer,rpcMode,method,args);
    }

    @Override
    public Object rcall(AgentId agId, Endpoint remotePeer, int timeout,
            String method, Object... args) throws Throwable {
        return rpc.rcall(agId, remotePeer, timeout,method,args);
    }

    @Override
    public Object rcall(AgentId agId, Endpoint remotePeer, String method,
            Object... args) throws Throwable {
        return rpc.rcall(agId, remotePeer, method,args);
    }

    @Override
    public Object rcall(CalleeId cid, int timeout, RPCMode rpcMode,
            String method, Object... args) throws Throwable {
        return rpc.rcall(cid, timeout, rpcMode,method,args);
    }

    @Override
    public Object rcall(CalleeId cid, RPCMode rpcMode, String method,
            Object... args) throws Throwable {
        return rpc.rcall(cid, rpcMode, method,args);
    }

    @Override
    public Object rcall(CalleeId cid, int timeout, String method,
            Object... args) throws Throwable {
        return rpc.rcall(cid, timeout, method,args);
    }

    @Override
    public Object rcall(CalleeId cid, String method, Object... args)
            throws Throwable {
        return rpc.rcall(cid, method, args);
    }

}
