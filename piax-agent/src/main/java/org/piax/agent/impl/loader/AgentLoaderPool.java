/*
 * AgentLoaderPool.java - An agent loader pool.
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
 * $Id: AgentLoaderPool.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent.impl.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

import org.piax.agent.impl.asm.ByteCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An agent loader pool
 */
public class AgentLoaderPool {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(AgentLoaderPool.class);

    /*--- constants ---*/
    private static final Object NULL_VALUE = new Object();

    /*--- instance fields ---*/
    private final WeakHashMap<AgentLoader, Object> pool
        = new WeakHashMap<AgentLoader, Object>();
    
    final ClassLoader parentLoader;

    /*--- constructors ---*/

    public AgentLoaderPool(ClassLoader parent) {
        logger.trace("ENTRY:");
        parentLoader = parent;
    }
    
    /*--- instance methods ---*/

    private void add(AgentLoader loader) {
        logger.trace("ENTRY:");
        pool.put(loader, NULL_VALUE);
    }
    
    public AgentLoader getAccecptableLoaderAsAdded(
            String className, ByteCodes codes) {
        logger.trace("ENTRY:");

        synchronized (pool) {
            // search just acceptable loader
            List<AgentLoader> retryList = new ArrayList<AgentLoader>();
            Set<AgentLoader> loaders = pool.keySet();
            for (AgentLoader loader : loaders) {
                int level = loader.acceptableLevel(className, codes);
                switch (level) {
                case -1:
                    break;
                case 0:
                    retryList.add(loader);
                    break;
                case 1:
                    logger.trace("EXIT:");
                    return loader;
                }
            }
            for (AgentLoader loader : retryList) {
                if (loader.addCodesIfAcceptable(className, codes)) {
                    logger.trace("EXIT:");
                    return loader;
                }
            }
            // If no acceptable loader, return new one.
            // As AgentLoader increase its ByteCodes table,
            // so should make a clone of the specified ByteCodes instance.
            AgentLoader newLoader = new AgentLoader(parentLoader, className,
                    (ByteCodes) codes.clone());
            add(newLoader);
            return newLoader;
        }        
    }
    
    public int size() {
        synchronized (pool) {
            return pool.size();
        }        
    }
}
