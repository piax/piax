/*
 * JarUtil.java - A utility for jar handling.
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
 * $Id: JarUtil.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JarUtil {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(JarUtil.class);

    /*--- constants ---*/

    public static final String MANIFEST_VERSION_NUM = "1.0";

    /*--- nested classes ---*/
    
    public static class Parcel {
        public Attributes attribs = new Attributes();
        // use TreeMap for ordering
        public Map<String, byte[]> classes = new TreeMap<String, byte[]>();
        public Map<String, byte[]> others = new TreeMap<String, byte[]>();
    }
    
    public static byte[] wrap(Parcel par) {
        logger.trace("ENTRY:");
        
        // prepare the manifest
        Manifest mf = new Manifest();
        Attributes att = mf.getMainAttributes();
        att.putAll(par.attribs);
        att.put(Attributes.Name.MANIFEST_VERSION, MANIFEST_VERSION_NUM);
        logger.debug("manifest entry={}", mf.getMainAttributes().size());
        
        byte[] data = null;
        JarOutputStream jarOut = null;
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        
        try {
            // write META-INF/MANIFEST.MF
            jarOut = new JarOutputStream(bOut, mf);
            
            // write codes
            if (par.classes != null) {
                logger.debug("class entry={}", par.classes.size());
                Set<Map.Entry<String, byte[]>> ents = par.classes.entrySet();
                for (Map.Entry<String, byte[]> ent : ents) {
                    String fileName = ent.getKey().replace('.', '/') + ".class";
                    jarOut.putNextEntry(new JarEntry(fileName));
                    jarOut.write(ent.getValue());
                }
            }
            
            // write others
            if (par.others != null) {
                logger.debug("other entry={}", par.others.size());
                Set<Map.Entry<String, byte[]>> ents = par.others.entrySet();
                for (Map.Entry<String, byte[]> ent : ents) {
                    jarOut.putNextEntry(new JarEntry(ent.getKey()));
                    jarOut.write(ent.getValue());
                }
            }
            
            jarOut.closeEntry();
            jarOut.close();     //** this order is significant
            data = bOut.toByteArray();

        } catch (ZipException e) {
            logger.error("Unexpected ZipException occured");
        } catch (IOException e) {
            logger.error("Unexpected IOException occured");
        } finally {
            try {
                if (data == null)
                    jarOut.close();
                bOut.close();
            } catch (IOException e) {}
        }
        logger.trace("EXIT:");
        return data;
        
    }
    public static Parcel unwrap(byte[] data) {
        logger.trace("ENTRY:");

        // construct the Parcel
        Parcel par = new Parcel();

        ByteArrayInputStream bIn = new ByteArrayInputStream(data);
        JarInputStream jarIn = null;
        try {
            jarIn = new JarInputStream(bIn);
            
            // read META-INF/MANIFEST.MF
            Manifest mf = jarIn.getManifest();
            if (mf != null) {
                par.attribs.putAll(mf.getMainAttributes());
                logger.debug("manifest entry={}", mf.getMainAttributes().size());
            }
            
            JarEntry entry;
            while ((entry = jarIn.getNextJarEntry()) != null) {
                String entryName = entry.getName();
                
                // entry data to byte[]
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                byte[] buf = new byte[1024];
                int size;
                while ((size = jarIn.read(buf)) != -1) {
                    b.write(buf, 0, size);
                }
                byte[] entryData = b.toByteArray();
                
                if (entryName.endsWith(".class")) {
                    String className = entryName.substring(0, entryName
                            .length() - 6).replace('/', '.');
                    par.classes.put(className, entryData);
                } else {
                    par.others.put(entryName, entryData);
                }
            }
            
            logger.debug("class entry={}", par.classes.size());
            logger.debug("other entry={}", par.others.size());
        } catch (ZipException e) {
            logger.error("Unexpected ZipException occured");
        } catch (IOException e) {
            logger.error("Unexpected IOException occured");
        } finally {
            try {
                jarIn.close();
                bIn.close();
            } catch (IOException e) {}
        }
        logger.trace("EXIT:");
        return par;
    }

    public static Set<String> classNamesInJar(String jarFile) 
            throws IOException {
        if (jarFile == null) {
            throw new IllegalArgumentException("Jar file is null");
        }
        
        ZipFile f = new ZipFile(jarFile);
        Set<String> cnames = new HashSet<String>();

        Enumeration<? extends ZipEntry> enu = f.entries();
        while (enu.hasMoreElements()) {
            ZipEntry e = enu.nextElement();
            String entry = e.getName();
            if (entry.endsWith(".class")) {
                String cname = entry.substring(0, entry.length() - 6)
                    .replace('/', '.');
                cnames.add(cname);
            }
        }
        return cnames;
    }

}
