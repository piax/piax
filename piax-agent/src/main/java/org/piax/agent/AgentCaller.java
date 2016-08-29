/*
 * AgentCaller.java - An interface for discovery-calls
 * 
 * Copyright (c) 2009-2015 PIAX development team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * $Id: AgentCaller.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent;

import java.util.List;

import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCMode;

/**
 * An interface for discovery-calls
 */
public interface AgentCaller {
    
    /*--- synchronous discovery call ---*/

    /**
     * @param queryCond
     * @param method
     * @param args
     */
    Object[] discoveryCall(String queryCond, String method, Object... args);
    
    /**
     * @param queryCond
     * @param clazz
     * @param method
     * @param args
     */
    Object[] discoveryCall(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args);
    
    /*--- asynchronous discovery call ---*/

    /**
     * @param queryCond
     * @param method
     * @param args
     */
    FutureQueue<?> discoveryCallAsync(String queryCond,
            String method, Object... args);
    
    /**
     * @param queryCond
     * @param clazz
     * @param method
     * @param args
     */
    FutureQueue<?> discoveryCallAsync(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args);

    /*--- oneway discovery call ---*/

    /**
     * @param queryCond
     * @param method
     * @param args
     */
    void discoveryCallOneway(String queryCond,
            String method, Object... args);
    
    /**
     * @param queryCond
     * @param clazz
     * @param method
     * @param args
     */
    void discoveryCallOneway(String queryCond,
            Class<? extends AgentIf> clazz, String method, Object... args);

    /*--- discoveryCall with stub ---*/
    public <S extends RPCIf> S getDCStub(String queryCond,
            Class<S> clz, RPCMode rcpMode);

    public <S extends RPCIf> S getDCStub(String queryCond,
            Class<S> clz);

    public <T> List<T> getList(T discoveryCall);

    public <T> FutureQueue<T> getFQ(T discoveryCallAsync);
    public FutureQueue<Object> getFQ();

}
