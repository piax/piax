/*
 * AgentTransportManager.java - A manager for agent transport.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * この抽象クラスは、AgentPeerを使用する場合に、そのトランスポートを
 * 構築する役割を果たすクラスを示す。
 * Agentを使用する場合は、RPC用のトランスポートを構築し、CombinedOverlayの配下の
 * オーバレイを必要に応じて構築、設定する必要がある。
 * AgentPeerでは、一般的にはユーザは、このインタフェースを実装するクラスを記述して
 * AgentPeerに設定する必要がある。
 * AgentPeerは、トランスポートの初期化が必要な場合に、getRPCTransport, setupOverlayの
 * 順に呼び出す。
 */
public abstract class AgentTransportManager {
        /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(AgentTransportManager.class);
    /**
     * リスナー
     */
    private AgentTransportListener listener = null;
    
    /**
     * リスナーを設定する。AgentPeerから呼び出される。
     * AgentPeerの設定と矛盾するのを避けるため、AgentPeer以外から
     * 呼び出してはいけない。
     * @param listener リスナー
     */
    void setListener(AgentTransportListener listener) {
        this.listener = listener;
    }
    
    /**
     * リスナーを取得する。
     * @return リスナー
     */
    protected AgentTransportListener getListener() {
        return listener;
    }
    
    public abstract PeerId getPeerId();
    public abstract String getName();
    /**
     * joinを行っているか否か
     */
    private boolean joined = false;
    
    private class OverlayEntry {
        Overlay<?,?> instance;
        Endpoint seed;
        AgentOverlayFactory factory;
        String name;
        
        OverlayEntry(Overlay<?,?> instance, Endpoint seed,
                AgentOverlayFactory factory, String name) {
            this.instance = instance;
            this.seed = seed;
            this.factory = factory;
            this.name = name;
        }
    }

    private class TransportEntry {
        Transport<?> instance;
        AgentTransportFactory factory;
        String name;
        
        TransportEntry(Transport<?> instance, AgentTransportFactory factory, String name) {
            this.instance = instance;
            this.factory = factory;
            this.name = name;
        }
    }
    
    private Map<String, OverlayEntry> overlayMap = new HashMap<String,OverlayEntry>();
    private Map<String, TransportEntry>transportMap = new HashMap<String, TransportEntry>();
    
	/**
	 * Define overlays and transports.
	 * This method must be implemented in the subclass.
	 * @param home the AgentHome instance.
	 * @throws Exception
	 */
	abstract public void setup(AgentHome home) throws Exception;

	/**
	 * Returns an instance named transportName.
	 * If the instance does not exist, the transport is newly instanciated.
	 * 
	 * @param transportName
	 * @return　transport instance
	 * @throws SetupTransportException Transport setup failure
	 */
	public synchronized Transport<?> getTransport(String transportName) throws SetupTransportException {
	    TransportEntry te = transportMap.get(transportName);
	    if (te == null) {
	        throw new SetupTransportException("Transport not registered: " + transportName);
	    }
	    if (te.instance != null) {
	        return te.instance;
	    }
	    if (te.factory == null) {
	        logger.error("Neither transport instance nor factory exists");
	        throw new SetupTransportException("Transport factory not registered: " + transportName);
	    }
	    Transport<?> trans = null;
	    try {
	        trans = te.factory.newInstance();
	    } catch (Throwable th) {
	        throw new SetupTransportException("new transport",th);
	    }
	    if (trans == null) {
	        logger.error("Transport factory returns null");
	        throw new SetupTransportException("new transport");
	    }
	    te.instance = trans;
	    return trans;
	}
	
	/**
	 * overlayNameで示されるオーバレイのインスタンスを返す。
	 * 名前とオーバレイのインスタンスは、内部で関連付けて置き、
	 * 同一名では同一のインスタンスを返す。
	 * インスタンスが存在しなければ、作成する。その際
	 * 既に存在するオーバレイでjoinが行われている場合は、
	 * joinを行う。
	 * 
	 * @param overlayName オーバレイ名
	 * @return　オーバレイIDパス
	 * @throws SetupTransportException オーバレイの作成に失敗
	 * @throws NoSuchOverlayException 
	 */
	public synchronized Overlay<?,?> getOverlay(String overlayName) throws SetupTransportException,
	    NoSuchOverlayException {
	    OverlayEntry oe = overlayMap.get(overlayName);
	    if (oe == null) {
	        throw new NoSuchOverlayException("Unknown overlay");
	    }
	    if (oe.instance != null) {
	        return oe.instance;
	    }
	    if (oe.factory == null) {
	        logger.error("Neither overlay instance nor factory exists");
	        throw new NoSuchOverlayException("Neither instance nor factory exists");
	    }
	    Overlay<?,?> ov = null;
	    try {
	        ov = oe.factory.newInstance();
	        if (joined && (!ov.isJoined())) {
	            if (listener != null) {
	                listener.onJoining(oe.name);
	            }
	            ov.join(oe.seed);
	            if (listener != null) {
	                listener.onJoinCompleted(oe.name);
	            }
	        }
	    } catch (Throwable th) {
	        throw new SetupTransportException("new overlay",th);
	    }
	    if (ov == null) {
	        logger.error("Overlay factory returns null");
	        throw new NoSuchOverlayException("Overlay factory returns null");
	    }
	    oe.instance = ov;
	    return ov;
	}
	
	/**
	 * 指定したoverlayに結びつけている名前を返す
	 * @param overlay オーバレイ
	 * @return　オーバレイ名
	 */
	public synchronized String getOverlayName(Overlay<?,?> overlay) {
	    if (overlay == null) return null;
	    for (OverlayEntry oe: overlayMap.values()) {
	        if (oe.instance == overlay) {
	            return oe.name;
	        }
	    }
	    return "UNKNOWN";
	}

	/**
	 * サポートしているオーバレイの名前のリストを返す。
	 * 現在インスタンスが存在しているオーバレイの
	 * 名前のリストではないことに注意。
	 * @return サポートしているオーバレイの名前の配列
	 */
	public synchronized String[] listOverlay() {
		return overlayMap.keySet().stream().toArray(String[]::new);
	}
	
	/**
	 * 支配下のオーバレイをすべてleaveさせる。
	 * @throws Exception
	 */
	public synchronized void leave() throws Exception {
	    if (!joined) {
	        //まだjoinしていないので何もしない。
	        return;
	    }
	    for (OverlayEntry oe: overlayMap.values()) {
	        if (oe.instance != null) {
	            oe.instance.leave();
	        }
	    }
	    joined = false;
	}
	
    /**
     * 支配下のオーバレイをすべてjoinさせる。
     * @throws Exception
     */
	public synchronized void join() throws Exception {
	    if (joined) {
	        //既にjoinしているので何もしない
	        return;
	    }
	    for (OverlayEntry oe: overlayMap.values()) {
	        if (oe.instance != null) {
	            if (listener != null) {
	                listener.onJoining(oe.name);
	            }
	            oe.instance.join(oe.seed);
	            if (listener != null) {
	                listener.onJoinCompleted(oe.name);
	            }
	        }
	    }
	    joined = true;
	}

	/**
	 * Define an overlay with name.
	 * @param name
	 * @param factory
	 * @param seed
	 * @throws IOException
	 */
	public void defineOverlay(String name, AgentOverlayFactory factory, Endpoint seed) throws IOException {
		OverlayEntry oe = overlayMap.get(name);
		// already defined.
	    if (oe != null) {
	    		return;
	    }
		oe = new OverlayEntry(null, seed, factory, name);
	    overlayMap.put(name, oe);
	}
	
	/**
	 * Give overlay factory a name.
	 * Unlike addOverlay, this method binds name and overlay factory even
	 * if an overlay factory with given name already exists. 
	 * 
	 * @param name オーバレイの名前
	 * @param factory オーバレイのインスタンスを作成するファクトリ
	 * @param seed joinする際のシード
	 * @throws IOException
	 */
	public void setOverlay(String name, AgentOverlayFactory factory, Endpoint seed) throws IOException {
		OverlayEntry oe = new OverlayEntry(null, seed, factory, name);
	    overlayMap.put(name, oe);
	}
	
	/*
	 * Define transport factory with name.
	 */
	public synchronized void defineTransport(String name, AgentTransportFactory factory) throws IOException {
		TransportEntry te = transportMap.get(name);
		// already defined.
	    if (te != null) {
	    		return;
	    }
	    te = new TransportEntry(null, factory, name);
	    transportMap.put(name,te);
	}
	/*
	 * Give transport factory a name. 
	 */
	public synchronized void setTransport(String name, AgentTransportFactory factory) throws IOException {
		TransportEntry te = new TransportEntry(null, factory, name);
	    transportMap.put(name,te);
	}
	
	/**
	 * オーバレイを削除する。
	 * インスタンスが登録されていれば、leave, finを行う。
	 * @param name
	 * @throws NoSuchOverlayException
	 * @throws IOException
	 */
	protected synchronized void removeOverlay(String name) throws NoSuchOverlayException, IOException {
	    OverlayEntry oe = overlayMap.remove(name);
	    if (oe == null) {
	        // オーバレイが存在しない
	        throw new NoSuchOverlayException();
	    }
	    if (oe.instance != null) {
	        try {
	            oe.instance.leave();
	        } finally {
	            oe.instance.fin();
	        }
	    }
	}
}
