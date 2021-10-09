package com.itcast.hdfsdemo;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.*;

public class HDFS_CRUD {
    FileSystem fs = null;

    // before是一个用于Junit单元测试框架中控制程序最先执行的注解，可以保证init()最先执行
    @Before
    public void init() throws Exception{
        // 构造一个配置参数对象，设置一个参数：到要访问的HDFS的URI
        Configuration conf = new Configuration();
        // 指定使用HDFS
        conf.set("fs.defaultFS", "hdfs://master:9000");
        // 通过方法来进行客户端身份的设置
        System.setProperty("HADOOP_USER_NAME", "root");
        // 通过filesystem的静态方法获取文件系统客户端对象
        fs = FileSystem.get(conf);
    }

    @Test
    public void testAddFileToHdfs() throws IOException{
        // 上传文件的本地路径
        Path src = new Path("D:/t.txt");
        // 上传到 hdfs 的路径
        Path dst = new Path("/testFile");
        // 上传方法
        fs.copyFromLocalFile(src, dst);
        // close
        fs.close();
    }

    @Test
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException{
        // download
        fs.copyToLocalFile(new Path("/testFile"), new Path("D:/"));
        fs.close();
    }

    // 目录操作
    @Test
    public void testMkdirAndDeleteAndRename() throws Exception{
        // 创建目录
        fs.mkdirs(new Path("/a/b/c"));
        fs.mkdirs(new Path("/a1/b1/c1"));
        // 重命名文件夹或文件
        fs.rename(new Path("/a"), new Path("/a3"));
        // delete
        fs.delete(new Path("/a1"), true); // true表示递归删除
    }

    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException{
        // 获取迭代对象
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            // 打印当前文件名
            System.out.println("Filename : " + fileStatus.getPath().getName());
            // 打印当前文件块大小
            System.out.println("FileBlockSize : " + fileStatus.getBlockSize());
            // 打印文件权限
            System.out.println("FileMode : " + fileStatus.getPermission());
            // 打印文件内容长度
            System.out.println("FileLength : " + fileStatus.getLen());

            // 获取文件块信息（包含长度、数据块、DataNode的信息）
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for(BlockLocation bl : blockLocations){
                System.out.println("block-length:" + bl.getLength() + " -- " + "block-offset:" + bl.getOffset());

                String[] hosts = bl.getHosts();
                for (String host : hosts){
                    System.out.println("host : " + host);
                }
            }

            System.out.println("\n-----------分割线-----------\n");
        }
    }
}
