/*
 * RPCInvocationHandler.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RPCInvocationHandler.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.gtrans.DynamicStub;
import org.piax.gtrans.IllegalRPCAccessException;
import org.piax.gtrans.NoSuchRemoteObjectException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCHook;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RPCMode;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.ReturnValue;
import org.piax.util.MethodUtil;

/**
 * 
 */
public class RPCInvocationHandler<E extends Endpoint> implements InvocationHandler {

    RPCInvoker<?, E> invoker;
    private ObjectId target;
    private E remotePeer;
    private int timeout;
    
    private RPCMode syncMode;

    public RPCInvocationHandler(RPCInvoker<?, E> invoker, ObjectId target,
            E remotePeer, int timeout, RPCMode syncMode) {
        if (syncMode == RPCMode.ASYNC) {
            throw new UnsupportedOperationException("ASYNC mode is not supported yet.");
        }
        this.invoker = invoker;
        this.target = target;
        this.remotePeer = remotePeer;
        this.timeout = timeout;
        this.syncMode = syncMode;
    }
    
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    void invokeOnewayRemote(String methodName, Object[] args) throws Throwable {
     	if (RPCHook.hook != null) {
    		RPCHook.RValue rv = RPCHook.hook.callerHook(RPCHook.CallType.ONEWAY,
    				target, remotePeer.toString(), methodName,args);
    		methodName = rv.method;
    		args = rv.args;
    	}
        invoker.sendOnewayInvoke(target, remotePeer,
                methodName, args);
    }
    
    /**
     * oneway呼び出し
     * @param methodName
     * @param args
     * @throws Throwable
     */
    private void callOneway(boolean onlyLocalCall, String methodName, Object[] args) throws Throwable {
        // case of local call
        if (onlyLocalCall && (!invoker.getEndpoint().equals(remotePeer))) {
            throw new IllegalRPCAccessException(
                    "could not call remotely without RemoteCallable annotation");
        }
        /*
         * TODO
         * 非同期処理になるので、remote peerと同じ処理にしている
         */
        // case of remote call
        invokeOnewayRemote(methodName,args);
    }
        
    Object invokeSyncRemote(String methodName, Object[] args)
            throws Throwable {
        ReturnValue<?> returnValue;
        if (RPCHook.hook != null) {
            RPCHook.RValue rv = RPCHook.hook.callerHook(RPCHook.CallType.SYNC,
                    target,remotePeer.toString(), methodName,args);
            methodName = rv.method;
            args = rv.args;
        }
        returnValue = invoker.sendInvoke(target, remotePeer, timeout,
                methodName, args);
        Throwable e = returnValue.getException();
        if (e != null) {
            throw e;
        }
        return returnValue.getValue();
    }
    

    
    /**
     * sync呼びだし
     * @param methodName
     * @param args
     * @return
     * @throws Throwable
     */
    private Object call(boolean onlyLocalCall, String methodName, Object[] args)
            throws Throwable {
        Object result;
        // case of local call
        if (invoker.getEndpoint().equals(remotePeer)) {
            try {
                RPCIf obj = invoker.getRPCObject(target);
                if (obj == null) {
                    throw new RPCException(new NoSuchRemoteObjectException(
                            "target object of ID not found: " + target));
                }
             	if (RPCHook.hook != null) {
            		RPCHook.RValue rv = RPCHook.hook.callerHook(RPCHook.CallType.SYNC,
            				target,remotePeer.toString(),methodName,args);
            		RPCHook.hook.calleeHook(rv.method,rv.args);
            	}
                result = MethodUtil.invoke(obj,RPCIf.class,methodName, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } catch (Throwable e) {
                throw e;
            }
            return result;
        }
        
        // case of remote call without RemoteCallable annotation
        if (onlyLocalCall) {
            throw new IllegalRPCAccessException(
                    "could not call remotely without RemoteCallable annotation");
        }
        // case of remote call
        return invokeSyncRemote(methodName,args);
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        String methodName;
        RPCMode mode = syncMode;
        boolean onlyLocalCall;
        if (method.getDeclaringClass() == DynamicStub.class) {
            //　型を指定しない動的な呼び出し
            methodName = (String)args[0];
            args = (Object[])args[1];
            /*
             *  インターフェースが指定されていないので
             *  呼び出し側ではRemopteCallableかどうかチェックしない
             */
            onlyLocalCall = false;
            /*
             *  インターフェースが指定されていないので
             *  AUTO場合は、SYNCと同等
             */
            if (mode == RPCMode.AUTO) {
                mode = RPCMode.SYNC;
            }
        } else {
            methodName = method.getName();
            RemoteCallable anno = method.getAnnotation(RemoteCallable.class);
            if (anno != null) {
                if (mode == RPCMode.AUTO) {
                    if (anno.value() == RemoteCallable.Type.ONEWAY) {
                        mode = RPCMode.ONEWAY;
                    } else {
                        mode = RPCMode.SYNC;
                    }
                }
                onlyLocalCall = false;
            } else {
                /*
                 * RemoteCallable指定がないので
                 * ローカル呼び出しのみ 
                 */
                onlyLocalCall = true;
            }
        }
        if (mode == RPCMode.ONEWAY) {
            callOneway(onlyLocalCall,methodName, args);
            return null;
        } else {
            /* SYNC */
            return call(onlyLocalCall, methodName, args);
        }
    }
}
