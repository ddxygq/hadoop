package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by Administrator on 2018/3/4.
 * 通过流的方式访问 hdfs
 */
public class HdfsStreamAccess {
    private FileSystem fs = null;
    private Configuration conf;

    public void init(){
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master1.hadoop:9000");
    }

    @Test
    public void testUpload() throws IOException {
        init();
        fs = FileSystem.get(conf);

        FSDataOutputStream outputStream = fs.create(new Path("/README.md"));
        FileInputStream inputStream = new FileInputStream("D:\\readme.MD");

        IOUtils.copyBytes(inputStream,outputStream,1024);
        System.out.println("文件上传完成...");

        fs.close();
    }

    @Test
    public void testDownload() throws IOException {
        init();
        fs = FileSystem.get(conf);

        // 先获取一个文件的输入流---针对hdfs上的
        FSDataInputStream inputStream = fs.open(new Path("/README.md"));
        // 再获取一个文件的输出流--针对本地的
        FileOutputStream outputStream = new FileOutputStream("D:\\readme.MD");

        // 再将输入流中数据传输到输出流
        IOUtils.copyBytes(inputStream,outputStream,4096);
        System.out.println("文件下载完成...");

        fs.close();
    }

    @Test
    public void testRandomAccess() throws IOException {
        init();
        fs = FileSystem.get(conf);

        FSDataInputStream inputStream = fs.open(new Path("/README.md"));
        inputStream.seek(12);

        FileOutputStream outputStream = new FileOutputStream("D:\\readme2.MD");

        IOUtils.copyBytes(inputStream,outputStream,1024);

        System.out.println("文件下载完成...");
        fs.close();
    }

    @Test
    public void testCat() throws IOException {
        init();
        fs = FileSystem.get(conf);

        FSDataInputStream inputStream = fs.open(new Path("/README.md"));

        // 拿到文件信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("README.md"));

        // 获取这个文件的所有block信息
        BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatuses[0],0,fileStatuses[0].getLen());

        // 第一个block的长度
        long length = blockLocations[0].getLength();
        // 第一个block的起始偏移量
        long offset = blockLocations[0].getOffset();

        System.out.println("length:" + length);
        System.out.println("offset:" + offset);

        // 获取第一个block写入输出流
        IOUtils.copyBytes(inputStream,System.out,1000);
        byte[] b = new byte[1024];

        FileOutputStream outputStream = new FileOutputStream(new File("d:\\block0"));
        while (inputStream.read(offset,b,0,1024) != -1){
            outputStream.write(b);
            offset = offset + 1024;
            if(offset >= length){
                return;
            }
        }

        outputStream.flush();
        outputStream.close();
        inputStream.close();
        fs.close();
    }

}
