/*
 * StatusRepo.java - An implementation of persistent repository.
 * 
 * Copyright (c) 2012-2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: StatusRepo.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.gtrans.GTransConfigValues;
import org.piax.util.ByteUtil;
import org.piax.util.SerializingUtil;

/**
 * 状態保存用のRepositoryを実現するクラス。
 * 通常は、user.home システムプロパティにより指定された HOMEディレクトリの
 * 直下の .piax ディレクトリの配下に作成される。
 * user.home とは異なるディレクトリをHOMEにする場合は、piax.user.home に
 * そのPATHを書くようにする。
 */
public class StatusRepo {

    public static boolean ON_MEMORY = false;
    private static String PROP_FILE = ".prop";
    private static String LOCK_FILE = ".lock";

    private final File namePath;
    private final Properties prop;
    private final ConcurrentHashMap<String, byte[]> memRepo;
    private final FileOutputStream lockOut;

    public StatusRepo(String name) throws IllegalArgumentException, IOException {
        prop = new Properties();
        
        // calc the path of StatusRepo
        String userHome = System.getProperty("user.home");
        String piaxUserHome = System.getProperty("piax.user.home", userHome);
        namePath = new File(piaxUserHome + File.separator + GTransConfigValues.REPO_PATH
                + File.separator + name);
        if (ON_MEMORY) {
            memRepo = new ConcurrentHashMap<String, byte[]>();
            lockOut = null;
            return;
        }
        memRepo = null;
        if (!namePath.isDirectory()) {
            if (!namePath.mkdirs()) {
                throw new IOException();
            }
        }
        File propPath = new File(namePath, PROP_FILE);
        if (propPath.isFile()) {
            FileInputStream in = new FileInputStream(propPath);
            prop.load(in);
            in.close();
        }
        // get lock
        lockOut = new FileOutputStream(new File(namePath, LOCK_FILE));
        if (lockOut.getChannel().tryLock() == null) {
            // lock failed --> name already used!!
            throw new IllegalArgumentException("The specified name already used");
        }
    }
    
    public void fin() {
        try {
            if (lockOut != null) lockOut.close();
            new File(namePath, LOCK_FILE).delete();
        } catch (IOException ignore) {
        }
    }
    
    public File getPath() {
        return namePath;
    }
    
    private void removeSubs(File path) {
        File[] subs = path.listFiles();
        if (subs != null) {
            for (File subpath : subs) {
                if (subpath.isDirectory()) {
                    removeSubs(subpath);
                }
                subpath.delete();
            }
        }
    }
    
    public void cleanUp() {
        removeSubs(namePath);
        prop.clear();
        if (memRepo != null) {
            memRepo.clear();
        }
    }

    public String getProp(String key) {
        return prop.getProperty(key);
    }
    
    public void setProp(String key, String value) {
        prop.setProperty(key, value);
    }
    
    public void removeProp(String key) {
        prop.remove(key);
    }

    public void saveProp() throws IOException {
        if (memRepo != null) {
            return;
        }
        File propPath = new File(namePath, PROP_FILE);
        FileOutputStream out = new FileOutputStream(propPath);
        prop.store(out, null);
        out.flush();
        out.close();
    }

    public void saveBytes(String fileName, byte[] bytes) 
    throws FileNotFoundException, IOException {
        if (memRepo != null) {
            memRepo.put(fileName, bytes);
            return;
        }
        File file = new File(namePath, fileName);
        file.getParentFile().mkdirs();
        ByteUtil.bytes2File(bytes, file);
    }

    public byte[] restoreBytes(String fileName)
        throws FileNotFoundException, IOException {
        if (memRepo != null) {
            return memRepo.get(fileName);
        }
        File file = new File(namePath, fileName);
        return ByteUtil.file2Bytes(file);
    }
    
    public boolean delete(String fileName) {
        if (memRepo != null) {
            return (memRepo.remove(fileName) != null);
        }
        File file = new File(namePath, fileName);
        return file.delete();
    }

    public void saveData(String fileName, Serializable data) 
    throws FileNotFoundException, IOException {
        byte[] bytes = SerializingUtil.serialize(data);
        saveBytes(fileName, bytes);
    }
    
    public Object restoreData(String fileName) 
    throws FileNotFoundException, IOException, ClassNotFoundException {
        byte[] bytes = restoreBytes(fileName);
        if (bytes == null) {
            return null;
        }
        return SerializingUtil.deserialize(bytes);
    }
}
