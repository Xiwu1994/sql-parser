package com.sql.parse.util;

import java.io.File;
import java.util.LinkedList;

public class TraverseFolder {
    public static LinkedList traverseFolder(String path) {
        LinkedList<String> file_list = new LinkedList<String>();
        File file = new File(path);
        if (file.exists()) {
            LinkedList<File> list = new LinkedList<File>();

            File[] files = file.listFiles();
            for (File file2 : files) {
                if (file2.isDirectory()) {
                    //System.out.println("文件夹:" + file2.getAbsolutePath());
                    if(! file2.getAbsolutePath().contains("data_migration")) {
                        list.add(file2);
                    }
                } else {
                    if(file2.getAbsolutePath().contains(".sql")) {
                        file_list.add(file2.getAbsolutePath());
                    }
                    //System.out.println("文件:" + file2.getAbsolutePath());
                }
            }
            File temp_file;
            while (!list.isEmpty()) {
                temp_file = list.removeFirst();
                files = temp_file.listFiles();
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        //System.out.println("文件夹:" + file2.getAbsolutePath());
                        list.add(file2);
                    } else {
                        file_list.add(file2.getAbsolutePath());
                        //System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
        return file_list;
    }
}
