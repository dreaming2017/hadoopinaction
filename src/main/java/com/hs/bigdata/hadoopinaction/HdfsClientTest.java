package com.hs.bigdata.hadoopinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HdfsClientTest {
    private static FileSystem fs;
    private static Configuration configuration;
    private String HDFS_URL = "hdfs://hserver1:9000";
    private String HDFS_USER = "hadoop";

    private static Logger LOG = LoggerFactory.getLogger(HdfsClientTest.class);

    /**
     * 第一步：创建对象
     *
     * @throws Exception
     * @throws InterruptedException
     */
    @Before
    public void before() throws Exception {
        configuration = new Configuration();
        fs = FileSystem.get(URI.create(HDFS_URL), configuration, HDFS_USER);
    }

    /**
     * 最后一步：关闭资源
     *
     * @throws Exception
     */
    @After
    public void after() throws Exception {
        LOG.info(">>>程序执行完毕且没报错，关闭资源");
        fs.close();
    }


    /**
     * 创建目录
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        boolean mkdirsFlag = fs.mkdirs(new Path("/test/testmkdirs/test2"));
        LOG.info("mkdirsFlag = {}", mkdirsFlag);
    }

    /**
     * 上传文件
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocal() throws IOException {
        fs.copyFromLocalFile(new Path("input/wordcount.txt"), new Path("/test/wordcount.txt"));
    }

    /**
     * 文件重命名
     *
     * @throws IOException
     */
    @Test
    public void renameFile() throws IOException {
        boolean renameFlag = fs.rename(new Path("/test/wordcount.txt"), new Path("/test/wordcount_rename.txt"));
        LOG.info("renameFlag = {}", renameFlag);
    }

    /**
     * 查看文件信息:遍历文件名称、权限、长度、块信息
     *
     * @throws IOException
     */
    @Test
    public void listFile() throws IOException {
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus next = remoteIterator.next();
            //路径名及文件名
            String name = next.getPath().getName();
            LOG.info(name + "\t");
            //文件长度
            LOG.info(next.getLen() + "\t");
            //权限和分组
            LOG.info(next.getPermission() + " Group:" + next.getGroup());

            //获取存储块信息并遍历
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                //获取存储主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    LOG.info(name + "的主机节点名：" + host);
                }
            }
        }
    }

    /**
     * 判断文件类型是文件还是文件夹（软连接）
     *
     * @throws IOException
     */
    @Test
    public void fileType() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        //遍历文件
        for (FileStatus fst : fileStatuses) {
            //如果是文件
            if (fst.isFile()) LOG.info(fst.getPath().getName() + "是文件。");
                //如果是文件夹或软连接
            else LOG.info(fst.getPath().getName() + "是目录或者链接");
        }
    }

    /**
     * 使用IO流上传文件
     * @throws IOException
     */
    @Test
    public void putIO() throws IOException {
        //创建IO流对象
        /*FileInputStream fi = new FileInputStream(new File("input/04-代码.rar"));
        FSDataOutputStream fo = fs.create(new Path("/test/p04-代码.rar"));
        //使用IOUtils进行流对拷
        IOUtils.copyBytes(fi, fo, configuration);
        //关闭资源
        IOUtils.closeStream(fo);
        IOUtils.closeStream(fi);*/
    }

    /**
     * 下载第一块
     * @throws Exception
     */
    @Test
    public void readFileSeek1() throws Exception{
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/p04-代码.rar"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("f:/bigdata/hadoopinaction/output/p04-代码.rar.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 下载第二块已寄后面的所有
     * @throws Exception
     */
    @Test
    public void readFileSeek2() throws Exception{

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/test/p04-代码.rar"));

        // 3 定位输入数据位置
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("f:/bigdata/hadoopinaction/output/p04-代码.rar.part2"));
        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 读取第二个块的数据，并输出到hdfs上
     * @throws Exception
     */
    @Test
    public void downFileSeek2() throws Exception {
        Path path = new Path("/test/p04-代码.rar");
        //listfiles中可以获取到块的信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path, false);
        LocatedFileStatus next = listFiles.next();
        BlockLocation[] bl = next.getBlockLocations();

        long offset = bl[1].getOffset();//获取偏移量
        long length = bl[1].getLength();

        //输入流
        FSDataInputStream fis = fs.open(path);
        //设置偏移量
        fis.seek(offset);
        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("f:/bigdata/hadoopinaction/output/p04-代码.rar.part2"));
        // 5 流的对拷
        IOUtils.copyBytes(fis, fos,length,true);


    }





}
