/*
 * AgentLoader.java - An agent loader
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
 * $Id: AgentLoader.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent.impl.loader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.SecureClassLoader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.piax.agent.impl.asm.ByteCodes;
import org.piax.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Class loader for Agents which loads the byte code from the specified
 * ByteCodes instance.
 */
public class AgentLoader extends SecureClassLoader {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(AgentLoader.class);

    /*--- instance fields ---*/
    
    protected ByteCodes bcodes;
    
    /** registered class table includes referred classes */
    private final Map<String, Set<String>> classTable = 
        new HashMap<String, Set<String>>();
    
    /** checked and safe class list */
    private Set<String> safeClassList = new HashSet<String>();

    /*--- constructors ---*/
    
    public AgentLoader(ClassLoader parent, String className, ByteCodes bcodes) {
        super(parent);
        logger.trace("ENTRY:");
        this.bcodes = bcodes;
        logger.debug("ByteCodes entries {}", bcodes);
        classTable.put(className, bcodes.getClassNames());
        logger.trace("EXIT:");
    }

    /*--- instance methods ---*/

    synchronized int acceptableLevel(String className, ByteCodes codes) {
        Set<String> referred = classTable.get(className);
        
        if (referred == null) {
            return 0; // acceptable but not just
        }
        // inconsistent if referred classes are different
        if (!referred.equals(codes.getClassNames())) {
            return -1; // inconsistent
        }
        
        if (!this.bcodes.acceptable(codes)) {
            return -1; // inconsistent
        }
        return 1; // just acceptable
    }
    
    synchronized boolean addCodesIfAcceptable(String className, ByteCodes codes) {
//        Set<String> referred = classTable.get(className);
//        
//        // inconsistent if referred classes are different
//        if (!refered.equals(bcodes.getClassNames())) {
//            return false;
//        }
        
        boolean acceptable = this.bcodes.addCodesIfAcceptable(codes);
        if (acceptable) {
            // if className and bcodes pair is consistent, register them
            classTable.put(className, codes.getClassNames());
        }
        return acceptable;
    }

    protected boolean sameInSystemCL(String cname, byte[] code) {
        if (safeClassList.contains(cname)) 
            return true;
        
        String cfile = cname.replace('.', '/') + ".class";
        InputStream in = ClassLoader.getSystemResourceAsStream(cfile);
        if (in == null) {
            return false;
        }
        byte[] bcode;
        try {
            bcode = ByteUtil.stream2Bytes(in);
        } catch (IOException e) {
            return false;
        }
        if (ByteUtil.equals(code, bcode)) {
            safeClassList.add(cname);
            return true;
        }
        return false;
    }
    
    /* (non-Javadoc)
     * @see java.lang.ClassLoader#findResource(java.lang.String)
     */
    @Override
    protected URL findResource(String name) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see java.net.URLClassLoader#findResources(java.lang.String)
     */
    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    /* (non-Javadoc)
     * @see java.lang.ClassLoader#findClass(java.lang.String)
     */
    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        byte[] data = bcodes.get(name);
        if (data == null || sameInSystemCL(name, data)) {
            logger.debug("findClass failed:{}", name);
            return Class.forName(name, false, ClassLoader.getSystemClassLoader());
        }
        logger.debug("findClass {}", name);
        return defineClass(name, data, 0, data.length);
    }
}
