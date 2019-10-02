package com.hs.bigdata.hadoopinaction;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private static Logger LOG  = LoggerFactory.getLogger(HDFSClient.class);
    String HDFS_URL = "hdfs://hserver1:9000";
    String HDFS_USER = "hadoop";

    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void put() throws Exception{

        FileSystem fileSystem = FileSystem.get(URI.create(HDFS_URL), new Configuration(), HDFS_USER);
        fileSystem.copyFromLocalFile(new Path("input/wordcount.txt"),
                new Path("/output/wordcount.txt"));
        fileSystem.close();
        LOG.info("测试上传成功");
    }

    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void testMkdirs() throws Exception{

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI(HDFS_URL), configuration, HDFS_USER);

        // 2 创建目录
        boolean flags = fs.mkdirs(new Path("/test/testmkdirs/test1"));


        LOG.info("flags =" + flags);
        // 3 关闭资源
        fs.close();
    }


}
