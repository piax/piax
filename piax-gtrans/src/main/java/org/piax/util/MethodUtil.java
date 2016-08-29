/*
 * MethodUtil.java - A utility class for method handling.
 * 
 * Copyright (c) 2009-2015 PIAX develoment team
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
 * $Id: MethodUtil.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.util;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;

import org.piax.gtrans.IllegalRPCAccessException;
import org.piax.gtrans.RemoteCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Method invocation utility
 */
public class MethodUtil {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(MethodUtil.class);
    
    /** key object for cache map */
    private static class MethodKey {
        final Class<?> clazz;
        final Class<?> superIf;
        final String methodName;
        final int paramNum;
        MethodKey(Class<?> clazz, Class<?> superIf, String methodName, int paramNum) {
            this.clazz = clazz;
            this.superIf = superIf;
            this.methodName = methodName;
            this.paramNum = paramNum;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o == null || !(o instanceof MethodKey)) 
                return false;
            MethodKey mkey = (MethodKey) o;
            return clazz == mkey.clazz
                && superIf == mkey.superIf
                && methodName.equals(mkey.methodName)
                && paramNum == mkey.paramNum;
        }
        
        @Override
        public int hashCode() {
            return clazz.hashCode() 
                ^ ((superIf == null) ? 0 : superIf.hashCode()) 
                ^ methodName.hashCode() ^ paramNum;
        }
    }

    /** map for cache use */
    private static WeakHashMap<MethodKey, Method[]> methodCache =
        new WeakHashMap<MethodKey, Method[]>();
    
    /**
     * 指定されたclazzとそのスーパーinterfaceとmethod名と引数の数の4つの情報にマッチする
     * Methodオブジェクトのリストを返す。
     * また、可変長引数を持つメソッドについては、チェックに負荷がかかるため、paramNumの指定は無視している。
     * スーパーinterface superIfが指定された場合は、
     * 指定されたclazzの持つinterface（super classから継承されたものも含めて）の中で、
     * 指定された superIfと同じかsub interfaceの関係にある interfaceの集合の中で、
     * 下界（lower bound）なものを対象とする。
     * このスーパーinterfaceは、RPCの対象オブジェクトのinterface、通常はRPCIfのサブinterface、
     * を指定する際に用いる。
     * <p>
     * 効率化のため、計算した値はキャッシュに登録しておく。
     * 
     * @param clazz 基準となるClass
     * @param superIf スーパーinterface
     * @param methodName method名
     * @param paramNum 引数の数
     * @return マッチするMethodオブジェクトのリスト
     */
    private static Method[] getMethods(Class<?> clazz, Class<?> superIf, 
            String methodName, int paramNum) {
        MethodKey key = new MethodKey(clazz, superIf, methodName, paramNum);
        Method[] invocableMethods = methodCache.get(key);
        
        // if null, make invocable Method list
        if (invocableMethods == null) {
            // if superIf is not null, target classes will be sub interfaces
            // of superIf.
            Class<?>[] classes = (superIf == null)? new Class[]{clazz}
                : ClassUtil.gatherLowerBoundSuperInterfaces(clazz, superIf);
            
            List<Method> methods = new ArrayList<Method>();
            for (Class<?> cls : classes) {
                Method[] ms = cls.getMethods();
                for (Method method : ms) {
                    if (method.getName().equals(methodName)) {
                        // if declared class is not public, skip
                        if (!Modifier.isPublic(
                                method.getDeclaringClass().getModifiers()))
                            continue;
                        if (method.isVarArgs()) {
                            /*
                             * 可変長引数の場合はOKとする
                             */
                            methods.add(method);
                            continue;
                        }
                        Class<?>[] params = method.getParameterTypes();
                        if (params.length == paramNum) {
                            methods.add(method);
                        }
                    }
                }
            }
            invocableMethods = new Method[methods.size()];
            methods.toArray(invocableMethods);
            methodCache.put(key, invocableMethods);
        }
        return invocableMethods;
    }
    
    /**
     * Converts primitive type to wrapper class.
     * 
     * @param type primitive type
     * @return wrapper class type
     */
    private static Class<?> boxing(Class<?> type) {
        if (type == Boolean.TYPE) return Boolean.class;
        if (type == Character.TYPE) return Character.class;
        if (type == Byte.TYPE) return Byte.class;
        if (type == Short.TYPE) return Short.class;
        if (type == Integer.TYPE) return Integer.class;
        if (type == Long.TYPE) return Long.class;
        if (type == Float.TYPE) return Float.class;
        if (type == Double.TYPE) return Double.class;
        return null;
    }
    
    /**
     * Tests whether the type represented by the specified argument can be
     * converted to the specified parameter type.
     * 
     * @param <T> parameter type
     * @param arg argument object
     * @param paramType parameter type
     * @return <code>true</code> if arg is assignable, <code>false</code> otherwise.
     */
    private static <T> boolean isAssignable(Object arg, Class<T> paramType) {
        /*
         *      param type <-- arg type
         * -----------------------------
         * OK   class          null
         * NG   primitive      null
         * OK   class          same or subclass
         * NG   class          superclass
         * OK   primitive      class (same mean by unboxing)
         * NG   primitive      class (other case)
         * 
         * Note: the arg type will not be primitive by autoboxing.
         */
        if (arg == null) {
            if (paramType.isPrimitive()) return false;
            return true;
        }
        Class<?> argType = arg.getClass();
        if (paramType == argType) return true;
        Class<?> _paramType = paramType.isPrimitive() ? boxing(paramType)
                : paramType;
        if (_paramType.isAssignableFrom(argType)) return true;
        return false;
    }
    
    /**
     * Searches <code>Method</code> object which has the specified declared class
     * and the specified name and the parameters that are assignable from
     * the specified arguments.
     * <p>
     * if superIf is not <code>null</code>, the target methods will be restricted
     * of the interfaces which are super of specified class and are sub of
     * the specified superIf.
     * 
     * @param clazz declared class
     * @param superIf super interface
     * @param methodName method name
     * @param args the arguments used for the method call
     * @return the matched <code>Method</code> object,
     *          <code>null</code> if not matched.
     */
    private static Method getMethod(Class<?> clazz, Class<?> superIf,
            String methodName, Object... args) {
        Method[] invocableMethods = getMethods(clazz, superIf, methodName,
                args.length);
        
        for (Method method : invocableMethods) {
            Class<?>[] paramTypes = method.getParameterTypes();
            boolean matched = true;
            if (!method.isVarArgs()) {
                // 通常のケース
                for (int i = 0; i < args.length; i++) {
                    if (!isAssignable(args[i], paramTypes[i])) {
                        matched = false;
                        break;
                    }
                }
                if (matched) return method;
            } else {
                // 可変長引数を持つ場合
                if (args.length == paramTypes.length - 1) {
                    // 通常引数部のみの代入可能性をチェックをすればよい
                    for (int i = 0; i < args.length; i++) {
                        if (!isAssignable(args[i], paramTypes[i])) {
                            matched = false;
                            break;
                        }
                    }
                    if (matched) return method;
                } else if (args.length == paramTypes.length) {
                    // 可変長引数部は、2通りの解釈があるのでそれぞれをチェックする必要がある
                    int i = 0;
                    // 通常引数部
                    for (; i < args.length - 1; i++) {
                        if (!isAssignable(args[i], paramTypes[i])) {
                            matched = false;
                            break;
                        }
                    }
                    // 可変長引数部
                    if (matched) {
                        // paramTypesの要素として互換な場合（通常）
                        if (isAssignable(args[i], paramTypes[i].getComponentType())) {
                            return method;
                        }
                        // paramTypesがそのまま互換な場合（配列としてパックされている）
                        if (isAssignable(args[i], paramTypes[i])) {
                            logger.debug("packed VarArgs case");
                            return method;
                        }
                    }
                } else {
                    // 通常引数部と可変長引数部を別々にチェック
                    for (int i = 0; i < args.length; i++) {
                        Class<?> paramType = null;
                        if (i < paramTypes.length - 1) {
                            // 通常引数部
                            paramType = paramTypes[i];
                        } else {
                            // 可変長引数部
                            paramType = paramTypes[paramTypes.length - 1]
                                    .getComponentType();
                        }
                        if (!isAssignable(args[i], paramType)) {
                            matched = false;
                            break;
                        }
                    }
                    if (matched) return method;
                }
            }
        }
        return null;
    }

    /**
     * Note: the specified method and its declared class should have 
     * the public modifier.
     * 
     * @param target target object
     * @param methodName method name
     * @param args the arguments used for the method call
     * @return the result of invoking the method
     * @throws NoSuchMethodException if a matching method is not found
     * @throws InvocationTargetException if the underlying method
     *          throws an exception.
     */
    public static Object invoke(Object target, String methodName,
            Object... args) throws NoSuchMethodException,
            InvocationTargetException {
        return invoke(target, null,methodName, args);
    }
    
    private static void checkRemoteCallable(Method method) {
        RemoteCallable anno = method
                .getAnnotation(RemoteCallable.class);
        if (anno == null) {
            throw new IllegalRPCAccessException(
                    "could not call remotely without RemoteCallable annotation");
        }
    }
    
    public static Object invoke(Object target, Class<?> superIf,
            String methodName, Object... args) throws NoSuchMethodException,
            InvocationTargetException {
        return invoke(target,superIf,true,methodName,args);
    }
    
    /**
     * Note: the specified method and its declared class should have 
     * the public modifier.
     * 
     * @param target target object
     * @param superIf super interface
     * @param localCall called from local peer
     * @param methodName method name
     * @param args the arguments used for the method call
     * @return the result of invoking the method
     * @throws NoSuchMethodException if a matching method is not found
     * @throws InvocationTargetException if the underlying method
     *          throws an exception.
     */
    public static Object invoke(Object target, Class<?> superIf, boolean localCall,
            String methodName, Object... args) throws NoSuchMethodException,
            InvocationTargetException {
        // argsがnullで呼ばれることがあるため
        Object[] _args = args == null ? new Object[] {} : args;
        Method method = getMethod(target.getClass(), superIf, methodName, _args);
        if (method == null) {
            throw new NoSuchMethodException(methodName + " in " 
                    + target.getClass().getName());
        }
        try {
            if (!localCall) {
                // Remote callの場合は、RemoteCallableであることをチェック
                checkRemoteCallable(method);
            }
            /*
             *  可変長引数が配列にpackされている時は可変長引数であっても、次の処理は呼ばない。
             *  java.lang.reflect.Proxyを使ってメソッドinvokeする場合は、このケースに相当する
             */
            if (method.isVarArgs()) {
                /*
                 * 可変数引数を扱う場合、配列として扱うため、引数要素が代入可能であっても
                 * 配列としてcastできない。このため、配列を別に用意し、互換性を保つ
                 */
                Class<?>[] ptypes = method.getParameterTypes();
                int arrIx = ptypes.length - 1;
                if (arrIx == _args.length - 1 && 
                        ptypes[arrIx].isAssignableFrom(_args[arrIx].getClass())) {
                    /*
                     * 可変数引数の箇所が配列としてpackされている場合
                     */
                    return method.invoke(target, _args);
                }
                Object arr = Array.newInstance(
                        ptypes[arrIx].getComponentType(),
                        _args.length - arrIx);
                for (int i = 0; i < _args.length - arrIx; i++) {
                    Array.set(arr, i, _args[i + arrIx]);
                }
                Object[] args1 = new Object[arrIx + 1];
                for (int i = 0; i < arrIx; i++) {
                    args1[i] = _args[i];
                }
                args1[arrIx] = arr;
                return method.invoke(target, args1);
            }
            return method.invoke(target, _args);
        } catch (IllegalArgumentException e) {
            logger.error("", e);
            throw e;
        } catch (IllegalAccessException e) {
            logger.error("", e);
            throw new NoSuchMethodException(methodName + " in "
                    + target.getClass().getName());
        }
    }

    /**
     * 
     * オブジェクトの呼び出すべきメソッドを探しだす。
     * <p>
     * 実引数の型互換と可変長引数部の配列化が済んでいる前提で処理が簡素化
     * されている。
     * 
     * @param targetClass class of target object
     * @param superIf super interface
     * @param methodName method name
     * @param args the arguments used for the method call
     * @return method to invoke
     * @throws NoSuchMethodException if a matching method is not found
     */
    public static Method strictGetMethod(Class<?> targetClass, Class<?> superIf,
            String methodName, Object... args) throws NoSuchMethodException {
        // argsがnullで呼ばれることがあるため
        Object[] _args = args == null ? new Object[] {} : args;
        Method[] invocableMethods = getMethods(targetClass, superIf,
                methodName, _args.length);
        if (invocableMethods.length == 1) {
            // 候補が一つに決まる場合は、そのまま返す
            return invocableMethods[0];
        } else if (invocableMethods.length > 1) {
            // 候補が複数ある場合は、実引数の互換性をチェックする
            for (Method method : invocableMethods) {
                Class<?>[] paramTypes = method.getParameterTypes();
                // 引数の数が異なる場合は除外
                if (_args.length != paramTypes.length)
                    continue;
                // 実引数と互換かチェック
                boolean matched = true;
                for (int i = 0; i < _args.length; i++) {
                    if (!isAssignable(_args[i], paramTypes[i])) {
                        matched = false;
                        break;
                    }
                }
                if (matched) {
                    return method;
                }
            }
        }
        // 互換なmethodがない場合（通常はありえない）
        throw new NoSuchMethodException(methodName + " in "
                + targetClass.getName());
    }
    
    /**
     * オブジェクトのメソッドを呼び出す。
     * <p>
     * 但し、invokeと違って、実引数の型互換と可変長引数部の配列化が済んでいる前提で処理が簡素化
     * されている。
     * 例えば、reflect.Proxyを使って、InvocationHandler.invokeから実引数を取得する
     * RPCInvokerの場合はこのメソッドを呼び出す方が効率的である。
     * 
     * @param target target object
     * @param superIf super interface
     * @param methodName method name
     * @param args the arguments used for the method call
     * @return the result of invoking the method
     * @throws NoSuchMethodException if a matching method is not found
     * @throws InvocationTargetException if the underlying method
     *          throws an exception.
     */
    public static Object strictInvoke(Object target, Class<?> superIf, boolean localCall,
            String methodName, Object... args) throws NoSuchMethodException,
            InvocationTargetException {
        // argsがnullで呼ばれることがあるため
        Method method = strictGetMethod(target.getClass(),superIf,methodName,args);
        try {
            if (!localCall) {
                // Remote callの場合は、RemoteCallableであることをチェック
                checkRemoteCallable(method);
            }
            return method.invoke(target, args);
        } catch (IllegalArgumentException e) {
            logger.error("", e);
            throw e;
        } catch (IllegalAccessException e) {
            logger.error("", e);
            throw new NoSuchMethodException(methodName + " in "
                    + target.getClass().getName());
        }
    }
}
