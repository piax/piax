/*
 * DefaultAgentLoader.java - A default agent loader.
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
 * $Id: DefaultAgentLoader.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent.impl.loader;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.piax.agent.impl.asm.ByteCodes;
import org.piax.agent.impl.asm.DependencyGather;
import org.piax.util.Cache;
import org.piax.util.ClassPathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default agent loader.
 */
public class DefaultAgentLoader extends URLClassLoader {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(DefaultAgentLoader.class);

    private static final int MAX_NUM_CACHE_ENT = 30;

    /*--- instance fields ---*/

    private DependencyGather gather;

    /** cache of the accessed ByteCodes instances */
    private Cache<String, ByteCodes> codesCache;
    
    /*--- constructors ---*/

    DefaultAgentLoader(ClassLoader parent, File... paths) {
        super(ClassPathUtil.toURLs(paths), parent);
        logger.trace("ENTRY:");
        gather = new DependencyGather(this);
        gather.setLevel(DependencyGather.LEV_APPL, DependencyGather.LEV_APPL);
        codesCache = new Cache<String, ByteCodes>(MAX_NUM_CACHE_ENT);
    }
    
    /*--- instance methods ---*/

    public ByteCodes getClassByteCodes(String className) {
        logger.trace("ENTRY:");
        if (className == null) {
            throw new IllegalArgumentException("Class name is null");
        }

        // check cache
        ByteCodes bcodes = codesCache.get(className);
        if (bcodes != null) {
            logger.trace("EXIT:");
            return bcodes;
        }
        
        Set<String> errs = new HashSet<String>();
        bcodes = gather.referredClassCodes(className, errs);
        try {
            bcodes.put(className, gather.getByteCode(className));
        } catch (Exception e) {
            // ignore
        }
        if (errs.size() != 0) {
            // list up the error class names to log
            for (String err : errs) {
                logger.warn("failure class: " + err);
            }
        }

        // save to cache
        codesCache.put(className, bcodes);
        logger.trace("EXIT:");
        return bcodes;
    }

    /* (non-Javadoc)
     * @see java.net.URLClassLoader#findResource(java.lang.String)
     */
    @Override
    public URL findResource(String name) {
        URL url = super.findResource(name);
        if (url == null) {
            logger.debug("URL findResource failed: {}", name);
            url = ClassLoader.getSystemClassLoader().getResource(name);
        }
        return url;
    }
    
    /* (non-Javadoc)
     * @see java.net.URLClassLoader#findResources(java.lang.String)
     */
    @Override
    public Enumeration<URL> findResources(String name) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    /* (non-Javadoc)
     * @see java.net.URLClassLoader#findClass(java.lang.String)
     */
    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
            return super.findClass(name);
        } catch (ClassNotFoundException e) {
            logger.debug("findClass failed:{}", name);
            return Class.forName(name, false, ClassLoader.getSystemClassLoader());
        }
    }
}
