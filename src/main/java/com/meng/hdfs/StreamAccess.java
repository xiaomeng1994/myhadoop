package com.meng.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * 通过流的方式处理hdfs
 */
public class StreamAccess {
    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hd1:9000"), conf, "root");
    }

    /**
     * 通过流的方式上传文件到hdfs
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {
        FSDataOutputStream outputStream = fs.create(new Path("/angelababy.love"), true);
        FileInputStream inputStream = new FileInputStream("c:/angelababy.love");
        IOUtils.copyBytes(inputStream, outputStream,fs.getConf());
    }

    @Test
    public void testDownLoadFileToLocal() throws Exception{
        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));
        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File("c:/jdk.tar.gz"));
        //再将输入流中数据传输到输出流
        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度
     * 用于上层分布式运算框架并发处理数据
     *
     * @throws Exception
     */
    @Test
    public void testRandomAccess() throws Exception {
        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path("/iloveyou.txt"));
        //可以将流的起始偏移量进行自定义
        in.seek(22);
        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File("c:/iloveyou.line.2.txt"));
        IOUtils.copyBytes(in, out, 19L, true);
    }

    /**
     * 显示hdfs上文件的内容
     *
     * @throws Exception
     */
    @Test
    public void testCat() throws Exception {
        FSDataInputStream in = fs.open(new Path("/iloveyou.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }
}