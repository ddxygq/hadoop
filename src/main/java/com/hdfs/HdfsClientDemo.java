package com.hdfs;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Administrator on 2018/3/3.
 */
public class HdfsClientDemo {
    private static FileSystem fs = null;
    private static Configuration conf;

    public static void init(){
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master1.hadoop:9000");
    }

    // 上传文件
    public static void putFile(){
        String localDir = "D:\\logs\\data.txt";
        String hdfDir = "/hadoop/data.txt";
        // conf.set("dfs.replication","5");
        try {
            Path localPath = new Path(localDir);
            Path hdfsPath = new Path(hdfDir);
            fs = FileSystem.get(conf);
            fs.copyFromLocalFile(localPath,hdfsPath);
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // 下载文件
    public static void getFile(){
        String localDir = "D:\\data.txt";
        String hdfDir = "/hadoop/data.txt";
        try {
            Path localPath = new Path(localDir);
            Path hdfsPath = new Path(hdfDir);
            fs = FileSystem.get(URI.create(hdfDir),conf);

            InputStream is = fs.open(hdfsPath);
            IOUtils.copyBytes(is,new FileOutputStream(new File(localDir)),2048,true);

            System.out.println("下载完成...");
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //打印配置
    public static void confPrinter(){
        Iterator<Map.Entry<String,String>> it = conf.iterator();
        while (it.hasNext()){
            Map.Entry<String,String> ent = it.next();
            System.out.println(ent + " : " +ent.getValue());
        }
    }

    // 创建文件
    public static void mkdir(){
        try {
            fs = FileSystem.get(conf);
            boolean mkdirs = fs.mkdirs(new Path("/mkdir/Test"));
            System.out.println("创建：" + mkdirs);
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void lsTest(){
        try {
            fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);
            while (listFiles.hasNext()){
                LocatedFileStatus fileStatus = listFiles.next();
                System.out.println("------------------" + fileStatus.getPath().getName() + "--------------");
                System.out.println("blocksize:" + fileStatus.getBlockSize());
                System.out.println("group:" + fileStatus.getGroup());
                System.out.println("replication:" + fileStatus.getReplication());
                System.out.println("owner:" + fileStatus.getOwner());
                BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                for(BlockLocation blockLocation :blockLocations){
                    System.out.println("块起始偏移量：" + blockLocation.getOffset());
                    System.out.println("块长度：" + blockLocation.getLength());
                    String[] hosts = blockLocation.getHosts();
                    for (String host:hosts){
                        System.out.println("host:" + host);
                    }
                }
                System.out.println("===================================");
            }

            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // 实现 hdfs dfs -ls / 功能
    public static void lsTest2(){
        try {
            fs = FileSystem.get(conf);
            FileStatus[] listStatus = fs.listStatus(new Path("/"));
            for(FileStatus fileStatus:listStatus){
                System.out.println("name:" + fileStatus.getPath().getName());
                System.out.println("-----------------------");
            }
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        init();

        // putFile();

        // getFile();

        // confPrinter();

        //mkdir();

        // lsTest();

        lsTest2();
    }
}
