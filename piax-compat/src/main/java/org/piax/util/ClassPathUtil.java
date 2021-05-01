/*
 * ClassPathUtil.java - A utility for class path handling.
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
 * $Id: ClassPathUtil.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * 
 */
public class ClassPathUtil {

    public static File[] toFiles(String classPath) 
            throws IllegalArgumentException {
        String[] paths = classPath.split(File.pathSeparator);
        File[] files = new File[paths.length];
        for (int i = 0; i < paths.length; i++) {
            files[i] = new File(paths[i]);
            if (!files[i].exists()) {
                throw new IllegalArgumentException("Unknown path:" + paths[i]);
            }
        }
        return files;
    }
    
    public static URL[] toURLs(File[] paths) throws IllegalArgumentException {
        URL[] urls = new URL[paths.length];
        for (int i = 0; i < paths.length; i++) {
            try {
                urls[i] = paths[i].toURI().toURL();
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid File:" + paths[i]);
            }
        }
        return urls;
    }

    public static File[] toFiles(URL[] urls) throws URISyntaxException {
        File[] files = new File[urls.length];
        for (int i = 0; i < urls.length; i++) {
            files[i] = new File(urls[i].toURI());
        }
        return files;
    }

    public static long lastModified(File dir) {
        long mtime = 0;
        File[] subFiles = dir.listFiles();
        if (subFiles == null) {
            return 0;
        }
        for (File sub : subFiles) {
            if (sub.isDirectory()) {
                mtime = Math.max(mtime, lastModified(sub));
            } else {
                if (sub.getName().endsWith(".class") || 
                        sub.getName().endsWith(".CLASS")) {
                    mtime = Math.max(mtime, sub.lastModified());
                }
            }
        }
        return mtime;
    }

    public static long lastModified(File[] paths) {
        long mtime = 0;
        for (File path : paths) {
            if (path.isDirectory()) {
                mtime = Math.max(mtime, lastModified(path));
            } else {
                if (path.getName().endsWith(".jar") || 
                        path.getName().endsWith(".JAR")) {
                    mtime = Math.max(mtime, path.lastModified());
                }
            }
        }
        return mtime;
    }
    
}
