/*
 * DependencyGather.java - Gather dependencies of a class
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
 * $Id: DependencyGather.java 718 2013-07-07 23:49:08Z yos $
 */
package org.piax.agent.impl.asm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.ClassReader;
import org.piax.util.ByteUtil;
import org.piax.util.Cache;
import org.piax.util.JarUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gather dependencies of a class   
 */
public class DependencyGather {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(DependencyGather.class);

    /*--- constants ---*/

    public static final int LEV_NODEF = -1;
    public static final int LEV_BOOTSTRAP = 0;
    public static final int LEV_SYSTEM = 1;
    public static final int LEV_APPL = 2;

    private static final int MAX_NUM_CACHE_ENT = 100;

    private static final String POSTFIX_IO_ERR = "<IO error>";
    private static final String POSTFIX_NODEF_ERR = "<no class def>";
    private static final String POSTFIX_FORMAT_ERR = "<format error>";
    private static final String POSTFIX_INIT_ERR = "<init error>";
    
    /*--- instance fields ---*/
    
    private final ClassLoader loader;
    private int listupLevel = LEV_SYSTEM;
    private int parseLevel = LEV_APPL;
    
    /** the accessed byte codes cache */
    private Cache<String, byte[]> bcCache;
    
    /*--- constructors ---*/

    public DependencyGather(ClassLoader loader) {
        this.loader = loader;
        bcCache = new Cache<String, byte[]>(MAX_NUM_CACHE_ENT);
    }
    
    public DependencyGather() {
        this(ClassLoader.getSystemClassLoader());
    }
    
    /*--- instance methods ---*/

    public void setLevel(int listupLevel, int parseLevel) {
        if (LEV_NODEF > listupLevel
                || listupLevel > parseLevel
                || parseLevel > LEV_APPL) {
            throw new IllegalArgumentException("Invalid level setting");
        }
        this.listupLevel = listupLevel;
        this.parseLevel = parseLevel;
    }
    
    public byte[] getByteCode(String className) 
            throws ClassNotFoundException, IOException {
        if (className == null) {
            throw new IllegalArgumentException("Class name is null");
        }

        // check cache
        byte[] bcode = bcCache.get(className);
        if (bcode != null) {
            return bcode;
        }
        String cfile = className.replace('.', '/') + ".class";
        InputStream in = loader.getResourceAsStream(cfile);
        if (in == null) {
            throw new ClassNotFoundException("Undefined class");
        }
        bcode = ByteUtil.stream2Bytes(in);
        // save to cache
        bcCache.put(className, bcode);
        return bcode;
    }

    public Set<String> directReferredClasses(final byte[] clazz)
            throws InvalidClassException {
        if (clazz == null) {
            throw new IllegalArgumentException("Class byte[] is null");
        }

        int magic = ByteUtil.bytes2Int(clazz, 0);
        if (magic != 0xCAFEBABE) {
            throw new InvalidClassException("Invalid class file format");
        }
        
        DependencyVisitor dv = new DependencyVisitor();
        new ClassReader(clazz).accept(dv, 0);
        return dv.getReferedClasses();
    }
    
    public Set<String> directReferredClasses(File classFile) 
            throws InvalidClassException, 
            FileNotFoundException, IOException {
        if (classFile == null) {
            throw new IllegalArgumentException("Class File is null");
        }

        byte[] clazz = ByteUtil.file2Bytes(classFile);
        return directReferredClasses(clazz);
    }
    
    public Set<String> directReferredClasses(InputStream classIn)
            throws InvalidClassException, IOException {
        if (classIn == null) {
            throw new IllegalArgumentException("Class stream is null");
        }

        byte[] clazz = ByteUtil.stream2Bytes(classIn);
        return directReferredClasses(clazz);
    }
    
    public Set<String> directReferredClasses(String className)
            throws ClassNotFoundException, InvalidClassException, IOException {
        logger.trace("ENTRY:");
        Set<String> ret =  directReferredClasses(getByteCode(className));
        logger.trace("EXIT:");
        return ret;
    }

    public Set<String> directReferredClasses(String className, 
            Set<String> errs) {
        Set<String> classes = new HashSet<String>();
        classes.add(className);
        return directReferredClasses(classes, errs);
    }

    public Set<String> directReferredClasses(Set<String> classNames, 
            Set<String> errs) {
        if (classNames == null) {
            throw new IllegalArgumentException("Class name list is null");
        }
        
        Set<String> referred = new HashSet<String>();
        for (String cname : classNames) {
            try {
                referred.addAll(directReferredClasses(cname));
            } catch (ClassNotFoundException e) {
                if (errs != null) 
                    errs.add(cname + " " + POSTFIX_NODEF_ERR);
            } catch (InvalidClassException e) {
                if (errs != null) 
                    errs.add(cname + " " + POSTFIX_FORMAT_ERR);
            } catch (IOException e) {
                logger.warn("Unexpected IOException in loading class: " + cname);
                if (errs != null) 
                    errs.add(cname + " " + POSTFIX_IO_ERR);
            }
        }
        return referred;
    }

    /**
     * pool exor bcodes is valid
     * no argument check
     * 
     * @param className
     * @param pool
     * @param bcodes
     * @param errs
     */
    private void referredClasses0(String className, 
            Set<String> pool, ByteCodes bcodes, Set<String> errs) {

        Set<String> curReferreds;
        try {
            curReferreds = directReferredClasses(className);
        } catch (ClassNotFoundException e) {
            if (errs != null) 
                errs.add(className + " " + POSTFIX_NODEF_ERR);
            return;
        } catch (InvalidClassException e) {
            if (errs != null) 
                errs.add(className + " " + POSTFIX_FORMAT_ERR);
            return;
        } catch (IOException e) {
            logger.warn("Unexpected IOException in loading class: " + className);
            if (errs != null) 
                errs.add(className + " " + POSTFIX_IO_ERR);
            return;
        }
        for (String cname : curReferreds) {
            logger.debug("curReferred: {}", cname);
            if (pool != null && pool.contains(cname)) {
                continue;
            }
            if (bcodes != null && bcodes.contains(cname)) {
                continue;
            }
            
            // classify the referred class
            int loadLevel;
            if (cname.startsWith("java.")) {
                loadLevel = LEV_BOOTSTRAP;
            } else {
                try {
                    Class<?> cls = Class.forName(cname, false, loader);
                    if (cls.getClassLoader() == null) {
                        // if class is loaded by bootstrap loader
                        loadLevel = LEV_BOOTSTRAP;
                    } else if (cls.getClassLoader() 
                            == ClassLoader.getSystemClassLoader()) {
                        loadLevel = LEV_SYSTEM;
                    } else {
                        loadLevel = LEV_APPL;
                    }
                } catch (ClassNotFoundException e) {
                    if (errs != null) {
                        errs.add(cname + " " + POSTFIX_NODEF_ERR);
                    }
                    loadLevel = LEV_NODEF;
                } catch (NoClassDefFoundError e) {
                    logger.warn("Unexpected NoClassDefFoundError in loading class: " 
                            + cname);
                    if (errs != null) {
                        errs.add(cname + " " + POSTFIX_INIT_ERR);
                    }
                    loadLevel = LEV_NODEF;
                }
            }
            if (loadLevel >= listupLevel) {
                if (pool != null) {
                    pool.add(cname);
                }
                if (bcodes != null) {
                    try {
                        bcodes.put(cname, getByteCode(cname));
                    } catch (Exception e) {
                        // ignore
                    }
                }
                if (loadLevel >= parseLevel) {
                    referredClasses0(cname, pool, bcodes, errs);
                }
            }
        }
    }
    
    public Set<String> referredClasses(String className, Set<String> errs) {
        Set<String> referred = new HashSet<String>();
        referredClasses0(className, referred, null, errs);
        return referred;
    }
    
    public Set<String> referredClasses(Set<String> classNames, Set<String> errs) {
        if (classNames == null) {
            throw new IllegalArgumentException("class name set is null");
        }
        
        Set<String> referred = new HashSet<String>();
        for (String className : classNames) {
            referredClasses0(className, referred, null, errs);
        }
        return referred;
    }

    public ByteCodes referredClassCodes(String className, Set<String> errs) {
        ByteCodes referredCodes = new ByteCodes();
        referredClasses0(className, null, referredCodes, errs);
        return referredCodes;
    }

    public ByteCodes referredClassCodes(Set<String> classNames, Set<String> errs) {
        if (classNames == null) {
            throw new IllegalArgumentException("class name set is null");
        }
        
        ByteCodes referredCodes = new ByteCodes();
        for (String className : classNames) {
            referredClasses0(className, null, referredCodes, errs);
        }
        return referredCodes;
    }

    public Set<String> referredClassesInJar(String jar, Set<String> errs) 
            throws IOException {
        return referredClasses(JarUtil.classNamesInJar(jar), errs);
    }
    
}
