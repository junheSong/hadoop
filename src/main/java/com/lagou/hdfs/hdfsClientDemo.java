package com.lagou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfsClientDemo {
    FileSystem fs = null;
    Configuration configuration = null;

    @Before
    public void Init() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        configuration = new Configuration();
        //指定副本数量
        //configuration.set("dfs.replication","2");
        //2 根据configuration获取filesystem对象
        fs = FileSystem.get(new URI("hdfs://192.168.80.150:9000"), configuration, "root");
    }

    @After
    public void destory() throws IOException {
        fs.close();
    }

    //在HDFS上创建文件夹
    @Test
    public void testMkdirs() throws  IOException {
        //3使用filesystem对象创建一个测试目录
        fs.mkdirs(new Path("/api_test1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          "));
    }

    //从本地上传文件至HDFS上
    @Test
    public void coyfromLocalToHdfs() throws IOException {
        //上传文件
        //src：源文件目录：本地目录
        //dst：目标文件目录，hdfs路径
        fs.copyFromLocalFile(new Path("e:/BS_70145_SPT_150000.txt"),new Path("/BS_70145_SPT_150000.txt"));

    }

    //从hdfs上下载文件至本地
    @Test
    public void downloadFromHdfs() throws IOException {
        //bool：是否删除原文件
        //src:hdfs路径
        //dst:目标路径
        fs.copyToLocalFile(true,new Path("/BS_70145_SPT_150000.txt"),new Path("e:/BS_70145_SPT_150000_copy.txt"));

    }

    //删除HDFS上文件
    @Test
    public void deleteFile() throws IOException {

        fs.delete(new Path("/api_test1"),true);
    }

    //遍历hdfs的根目录得到文件及文件夹的信息
    @Test
    public void listFiles() throws IOException {
        //得到一个迭代器：装有指定目录下的所有文件信息
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        //遍历迭代器
        while (remoteIterator.hasNext())
        {
            LocatedFileStatus fileStatus = remoteIterator.next();
            //文件名称
            final String fileName = fileStatus.getPath().getName();
            //长度
            final long len = fileStatus.getLen();
            //权限
            final FsPermission permission = fileStatus.getPermission();
            //分组
            final String group = fileStatus.getGroup();
            //用户
            final String owner = fileStatus.getOwner();
            //块信息
            final BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                final String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("主机名称 ：" + host);
                }
            }
            System.out.println(fileName + "\t" + len + "\t" + permission + "\t" + group + "\t" + owner);
        }
    }

    @Test
    public void TestListStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile())
                System.out.println(fileStatus.getPath().getName()+ "是文件");
            else if (fileStatus.isDirectory())
                System.out.println(fileStatus.getPath().getName()+ "是文件夹");
        }
    }


    //使用IO流操作HDFS
    //上传文件：准备输入流读取本地文件，使用HDFS的输出流写数据到HDFS
    @Test
    public void upLoadFile() throws IOException
    {
        //读取本地的输入流
        final FileInputStream fileInputStream = new FileInputStream(new File("E:/工作文档/集成自动化测试软件用户使用手册.docx"));
        //准备写数据的输出流
        final FSDataOutputStream outputStream = fs.create(new Path("/集成自动化测试软件用户使用手册.docx"));
        //输入六数据拷贝到输出流
        IOUtils.copyBytes(fileInputStream,outputStream,configuration);
        //可以再次关闭流
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(outputStream);

    }

    //使用IO流下载文件
    @Test
    public void dowmloadFile() throws IOException
    {
        //读取HDFS的输入流
        final FSDataInputStream in = fs.open(new Path("/集成自动化测试软件用户使用手册.docx"));
        //准备本地文件的输出流
        final FileOutputStream outputStream = new FileOutputStream(new File("/集成自动化测试软件用户使用手册.docx"));
        //输入六数据拷贝到输出流
        IOUtils.copyBytes(in,outputStream,configuration);
        //可以再次关闭流
        IOUtils.closeStream(in);
        IOUtils.closeStream(outputStream);

    }

    //seek定位读取:使用IO流去读文件文件内容输出两次
    @Test
    public void readFileSeek() throws IOException {
        //创建一个读取HDFS文件的输入流
        final FSDataInputStream in = fs.open(new Path("/BS_70145_SPT_150000_copy.txt"));
        //控制台数据
        //实现流拷贝
        IOUtils.copyBytes(in,System.out,4096,false);
        //再次读取文件
        in.seek(0);//定位从0偏移量开始
        IOUtils.copyBytes(in,System.out,4096,true);

        IOUtils.closeStream(in);
    }
}
