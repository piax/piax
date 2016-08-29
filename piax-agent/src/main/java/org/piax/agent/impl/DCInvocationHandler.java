/*
 * DCInvocationHandler.java - A handler for DC invocation.
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

package org.piax.agent.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;
import org.piax.agent.AgentIf;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCMode;
import org.piax.gtrans.RemoteCallable;

/**
 * A handler for DC invocation.
 */
public class DCInvocationHandler implements InvocationHandler {
    /*--- 通信関係 ---*/
    private final Class<? extends AgentIf> clazz;
    private final String queryCond;
    private final AgentHomeImpl home;
    
    private final RPCMode rpcMode;
    
    static private ThreadLocal<Object> results = new ThreadLocal<Object>();
    
    
    public DCInvocationHandler(AgentHomeImpl home,
            Class<? extends AgentIf> clazz,
            String queryCond, RPCMode rpcMode) {
        this.clazz = clazz;
        this.queryCond = queryCond;
        this.home = home;
        this.rpcMode = rpcMode;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        String methodName;
        Object[] rargs;
        methodName = method.getName();
        rargs = args;
        try {
            switch (rpcMode) {
            case SYNC:
                {   
                    Object[] r = home.discoveryCall(queryCond,
                        clazz,methodName,rargs);
                    results.set(r);
                }
                break;
            case ASYNC:
                {   
                    FutureQueue<?> r = home.discoveryCallAsync(queryCond,
                        clazz,methodName,rargs);
                    results.set(r);
                }
                break;
            case ONEWAY:
                home.discoveryCallOneway(queryCond,
                      clazz,methodName,rargs);
                break;
            case AUTO:
                RemoteCallable anno = method.getAnnotation(RemoteCallable.class);
                if (anno != null && anno.value() == RemoteCallable.Type.ONEWAY) {
                    home.discoveryCallOneway(queryCond,
                            clazz,methodName,rargs);
                } else {
                    Object[] r = home.discoveryCall(queryCond,
                        clazz,methodName,rargs);
                    results.set(r);
                }
                break;
            }
        } catch (UndeclaredThrowableException e) {
            /*
             * 互換性のためにUndeclaredThrowableExceptionとして投げられた
             * 例外は、ここでRPCExceptionにラップし直す。
             */
            throw new RPCException(e.getCause());
        }
        /*
         * 返り値は、スカラーを期待されているので
         * とりあえず、methodの返り値に合った値を返す。
         */
        Class<?> rt = method.getReturnType();
        if (rt.isPrimitive()) {
            // primitiveは、voidを含めて9種類
            switch(rt.getName()) {
            case "int":
            case "byte":
            case "short":
            case "long":
            case "float":
            case "double":
                // 数値の場合は0を返す。
                return 0;
            case "char":
                return 'A';
            case "boolean":
                return false;
            case "void":
                return null;
            default:
                return null;
            }
        } else {
            // オブジェクトの場合はnullを返す。
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    static public <T> List<T> _$getResults() {
        Object[] o = (Object[])results.get();
        if (o == null) return null;
        ArrayList<T> r = new ArrayList<T>(o.length);
        for (Object e : o) {
            r.add((T)e);
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    static public <T> FutureQueue<T> _$getAsyncResults() {
        return (FutureQueue<T>)results.get();
    }
    
}
