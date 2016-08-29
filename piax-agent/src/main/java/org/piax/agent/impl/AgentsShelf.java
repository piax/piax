/*
 * AgentsShelf.java - A helper class for searching agents
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
 * $Id: AgentsShelf.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.piax.agent.AgentId;
import org.piax.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AgentsShelf は、AgentHomeImplの補助として機能するAgent検索用のクラスである。
 * 登録したAgentを id, name, class により検索することができる。
 * Shelf（本棚）というネーミングはこの機能から来ている。
 * ここで、class指定 は、指定された class を含んで subclass の関係にある
 * class を型として持つすべての Agent を探し出すために用いる。
 * <p>
 * AgentsShelf では、class の管理を ClassLoader の情報と併せて行っている。
 * これは、他のピアから移動してくる Agent の class を管理するためにも必要な
 * 機能となる。class 名が同じであっても異なる ClassLoader を持つ Agent
 * は異なる Agent とみなされる。
 * <p>
 * AgentsShelf は Agent を AgentContainer を通して管理する。
 * 便宜上、AgentId によるメソッドインタフェースを用意している。
 * <p>
 * AgentsShelf は内部実装用 class であるため、メソッド呼び出しにおいて、
 * 引数のチェックは行っていない。
 */
public class AgentsShelf {
    /*
     * TODO Think!
     * パッケージ内に隠蔽させるクラスとして設計したが、結果的に、
     * パッケージ外に公開（public）せざるえないメソッドがたくさん
     * 出現している。
     */

    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(AgentsShelf.class);

    private static final int MAP_INIT_CAPACITY = 4;     // default 16
    
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();
    
    private final Map<AgentId, AgentContainer> idMap;
    private final Map<String, Set<AgentContainer>> nameMap;
    private final Map<ClassLoader, Map<Class<?>, Set<AgentContainer>>> classMap;
    
    AgentsShelf() {
        idMap = new HashMap<AgentId, AgentContainer>(MAP_INIT_CAPACITY);
        nameMap = new HashMap<String, Set<AgentContainer>>(MAP_INIT_CAPACITY);
        classMap = new HashMap<ClassLoader, Map<Class<?>, Set<AgentContainer>>>(MAP_INIT_CAPACITY);
    }
    
    // public as called by musasabi
    public AgentContainer get(AgentId agId) {
        r.lock();
        try {
            return idMap.get(agId);
        } finally {
            r.unlock();
        }
    }
    
    Set<AgentContainer> get(String name) {
        r.lock();
        try {
            Set<AgentContainer> matches = new HashSet<AgentContainer>();
            Set<AgentContainer> agcs = nameMap.get(name);
            if (agcs == null) {
                return matches;
            }
            matches.addAll(agcs);
            return matches;
        } finally {
            r.unlock();
        }
    }
    
    Set<AgentContainer> getAll() {
        r.lock();
        try {
            Set<AgentContainer> all = new HashSet<AgentContainer>();
            all.addAll(idMap.values());
            return all;
        } finally {
            r.unlock();
        }
    }
    
    Set<AgentId> getAllById() {
        r.lock();
        try {
            return idMap.keySet();
        } finally {
            r.unlock();
        }
    }
    
    private boolean canMatch(AgentContainer agc, String clazz) {
        ClassLoader loader = agc.getAgentClassLoader();
        Class<?> baseCls;
        try {
            baseCls = Class.forName(clazz, false, loader);
        } catch (ClassNotFoundException e) {
            return false;
        } catch (LinkageError  e) {
            logger.warn("linkage error occurred in loading " + clazz);
            return false;
        }
        return ClassUtil.isSub(agc.getAgentClass(), baseCls);
    }
    
    private Set<AgentContainer> matches0(Set<AgentContainer> agcs, String clazz) {
        Set<AgentContainer> matches = new HashSet<AgentContainer>();

        // if agcs is null, suppose all
        if (agcs == null) {
            return matches0(clazz);
        }
        
        // if clazz is null, all agents matches
        if (clazz == null || clazz.equals("")) {
            matches.addAll(agcs);
            return matches;
        }
        
        for (AgentContainer agc : agcs) {
            if (canMatch(agc, clazz)) {
                matches.add(agc);
            }
        }
        return matches;
    }
    
    private Set<AgentContainer> matches0(String clazz) {
        Set<AgentContainer> matches = new HashSet<AgentContainer>();
        
        // if clazz is null, all agents matches
        if (clazz == null || clazz.equals("")) {
            matches.addAll(idMap.values());
            return matches;
        }
        
        Set<ClassLoader> loaders = classMap.keySet();
        for (ClassLoader loader : loaders) {
            Class<?> baseCls;
            try {
                baseCls = Class.forName(clazz, false, loader);
            } catch (ClassNotFoundException e) {
                continue;
            } catch (LinkageError  e) {
                logger.warn("linkage error occurred in loading " + clazz);
                continue;
            }
            Map<Class<?>, Set<AgentContainer>> clmap = classMap.get(loader);
            Set<Class<?>> clss = clmap.keySet();
            for (Class<?> cls : clss) {
                if (ClassUtil.isSub(cls, baseCls)) {
                    matches.addAll(clmap.get(cls));
                }
            }
        }
        return matches;
    }
    
    Set<AgentContainer> matches(String clazz) {
        r.lock();
        try {
            return matches0(clazz);
        } finally {
            r.unlock();
        }
    }
    
    Set<AgentContainer> filtered(Set<AgentContainer> agcs, String clazz) {
        r.lock();
        try {
            return matches0(agcs, clazz);
        } finally {
            r.unlock();
        }
    }

    // public as called by musasabi
    public boolean add(AgentContainer agc) {
        w.lock();
        try {
            // by agentId
            AgentId agId = agc.getAgentId();
            if (idMap.containsKey(agId)) {
                return false;
            }
            idMap.put(agId, agc);
            
            // by agent name
            String agName = agc.getAgentName();
            Set<AgentContainer> agcs = nameMap.get(agName);
            if (agcs == null) {
                agcs = new HashSet<AgentContainer>(MAP_INIT_CAPACITY);
                nameMap.put(agName, agcs);
            }
            agcs.add(agc);
    
            // by loader and agent Class
            ClassLoader loader = agc.getAgentClassLoader();
            Map<Class<?>, Set<AgentContainer>> clmap = classMap.get(loader);
            if (clmap == null) {
                clmap = new HashMap<Class<?>, Set<AgentContainer>>(MAP_INIT_CAPACITY);
                classMap.put(loader, clmap);
            }
            Class<?> agClass = agc.getAgentClass();
            agcs = clmap.get(agClass);
            if (agcs == null) {
                agcs = new HashSet<AgentContainer>(MAP_INIT_CAPACITY);
                clmap.put(agClass, agcs);
            }
            agcs.add(agc);
            return true;
        } finally {
            w.unlock();
        }
    }

    private boolean remove0(AgentId agId, AgentContainer agc) {
        if (agc == null) {
            return false;
        }
        idMap.remove(agId);
        
        // by agent name
        String agName = agc.getAgentName();
        Set<AgentContainer> agcs = nameMap.get(agName);
        if (agcs != null) {
            agcs.remove(agc);
            if (agcs.size() == 0) {
                nameMap.remove(agName);
            }
        }
        
        // by loader and agent Class
        ClassLoader loader = agc.getAgentClassLoader();
        Class<?> agClass = agc.getAgentClass();
        Map<Class<?>, Set<AgentContainer>> clmap = classMap.get(loader);
        if (clmap != null) {
            agcs = clmap.get(agClass);
            if (agcs != null) {
                agcs.remove(agc);
                if (agcs.size() == 0) {
                    clmap.remove(agClass);
                }
            }
            if (clmap.size() == 0) {
                classMap.remove(loader);
            }
        }
        return true;
    }
    
    // public as called by musasabi
    public boolean remove(AgentId agId) {
        w.lock();
        try {
            // by agentId
            AgentContainer agc = idMap.get(agId);
            return remove0(agId, agc);
        } finally {
            w.unlock();
        }
    }

    // public as called by musasabi
    public boolean remove(AgentContainer agc) {
        w.lock();
        try {
            // by agentId
            AgentId agId = agc.getAgentId();
            return remove0(agId, agc);
        } finally {
            w.unlock();
        }
    }
}
