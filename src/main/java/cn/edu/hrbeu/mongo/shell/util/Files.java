/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hrbeu.mongo.shell.util;


import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wu on 2017/5/25.
 */
public class Files {

    public static String getFileExtension(String filename) {
        if (filename.endsWith("/")) {
            return "";
        }
        String ext;
        int index = filename.lastIndexOf(".");
        if (index >= 0) {
            int index2 = filename.lastIndexOf("/");
            int index3 = filename.lastIndexOf("\\");
            if (index < index2 || index < index3) {
                return "";
            }
            ext = filename.substring(index, filename.length());
        } else {
            ext = "";
        }
        return ext;
    }

    public static void close(Closeable... closeables) {
        if (closeables != null) {
            for (Closeable closeable : closeables) {
                if (closeable != null) {
                    try {
                        closeable.close();
                    } catch (IOException ex) {
                    }
                }
            }
        }
    }

    public static byte[] readBytesFromFile(File file) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            byte[] fileData = new byte[(int) file.length()];
            //convert file into array of bytes
            fis.read(fileData);
            return fileData;
        } catch (FileNotFoundException e) {
        } catch (Exception e) {
        } finally {
            close(fis);
        }
        return null;
    }
    
    public static String readStringFromFile(File file) {
        try {
            byte[] bytes = readBytesFromFile(file);
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
        }
        return null;
    }

    public static byte[] readBytesFromResource(String resouceUrl) {
        InputStream is = null;
        try {
            is = D.class.getResourceAsStream(resouceUrl);
            byte[] bytes = new byte[is.available()];
            is.read(bytes);
            return bytes;
        } catch (Exception e) {
        } finally {
            close(is);
        }
        return null;
    }

    public static boolean writeBytesToFile(byte[] bytes, File file) {
        try {
            com.google.common.io.Files.write(bytes, file);
            return true;
        } catch (IOException ex) {
        }
        return false;
    }
    
    // 确保文件路径没有问题，如果文件存在则删除
    static public void ensurePath(File file) {
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new RuntimeException("file is dir");
            }
            file.delete();
        } else {
            File dir = file.getParentFile();
            if (dir != null && !dir.exists()) {
                dir.mkdirs();
            }
        }
    }
    
    // 确保文件路径没有问题，如果文件存在则删除
    static public boolean mkdirs(File dir) {
        if (!dir.exists()) {
            return dir.mkdirs();
        }
        return true;
    }    
 

    static public File[] getSubDirectories(File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            return new File[0];
        }
        return dir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });
    }

    static public File[] getSubFiles(File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            return new File[0];
        }
        return dir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return !pathname.isDirectory();
            }
        });
    }

    static public List<File> getAllSubFiles(File dir) {
        List<File> list = new ArrayList<File>();
        for (File d : getSubDirectories(dir)) {
            list.addAll(getAllSubFiles(d));
        }
        list.addAll(Arrays.asList(getSubFiles(dir)));
        return list;
    }
    
    
    
}
