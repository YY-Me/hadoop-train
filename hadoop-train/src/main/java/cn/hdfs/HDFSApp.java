package cn.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * java api操作hadoop hdfs
 * Copyright (c) 2017 by 
 * @ClassName: HDFSApp.java
 * @Description: java api操作hadoop hdfs
 * 
 * @author: 余勇
 * @version: V1.0  
 * @Date: 2018年8月28日 上午9:52:31
 */
public class HDFSApp {

    FileSystem fileSystem = null;

    Configuration configuration = null;

    String HDFS_PATH = "hdfs://192.168.31.134:8020";

    String USER = "root";

    /**
     * 创建目录
     * @Title: mkdir
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void mkdir() {
        try {
            fileSystem.mkdirs(new Path("/javaapi/test"));
            System.out.println("mkdir sucess");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件并写内容
     * @Title: createFile
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void createFile() {
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fileSystem.create(new Path("/javaapi/test/test.txt"));
            outputStream.write("hello，i am from java api".getBytes());
            outputStream.flush();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(outputStream);
        }
    }

    /**
     * 查看hdfs文件的内容
     * @Title: cat
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void cat() {
        FSDataInputStream inputStream = null;
        try {
            inputStream = fileSystem.open(new Path("/javaapi/test/test.txt"));
            IOUtils.copyBytes(inputStream, System.out, 1024);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }

    /**
     * 重命名
     * @Title: rename
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void rename() {
        Path oldPath = new Path("/javaapi/test/test.txt");
        Path newPath = new Path("/javaapi/test/test_.txt");
        try {
            boolean b = fileSystem.rename(oldPath, newPath);
            System.out.println("重命名：" + String.valueOf(b));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件到hdfs
     * @Title: copyFromLocalFile
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void copyFromLocalFile() {
        Path localPath = new Path("d:\\PanDownload.exe");
        Path hdfsPath = new Path("/javaapi/test");
        try {
            fileSystem.copyFromLocalFile(localPath, hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件到hdfs,进度条
     * @Title: copyFromLocalFile
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void copyFromLocalFileProgress() {
        File file = new File("d:\\1024.zip");
        //输入输出流
        BufferedInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            inputStream = new BufferedInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        try {
            outputStream = fileSystem.create(new Path("/javaapi/test/1024.zip"), new Progressable() {
                public void progress() {
                    System.out.print(">");
                }
            });
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //写文件
        try {
            IOUtils.copyBytes(inputStream, outputStream, 4096);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
    }

    /**
     * 下载hdfs文件到本地
     * @Title: copyToLocalFile
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void copyToLocalFile() {
        Path localPath = new Path("d:\\PanDownload_.exe");
        Path hdfsPath = new Path("/javaapi/test/PanDownload.exe");
        try {
            fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
            System.out.println("下载成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列举目录文件
     * @Title: listFiles
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void listFiles() {
        try {
            FileStatus[] listStatus = fileSystem.listStatus(new Path("/javaapi/test"));
            for (FileStatus fileStatus : listStatus) {
                boolean isDir = fileStatus.isDirectory();
                //副本系数(java api本地没有手工设置副本系数，hadoop会采用自己的副本系数)
                short replication = fileStatus.getReplication();
                long len = fileStatus.getLen();
                String path = fileStatus.getPath().toString();
                System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件
     * @Title: delete
     * @param 
     * @return void
     * @throws
     */
    @Test
    public void delete() {
        try {
            //默认采用递归删除
            fileSystem.delete(new Path("/javaapi/test/test_.txt"), true);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() {
        System.out.println("hdfs start...");
        configuration = new Configuration();
        try {
            fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, USER);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }

    @After
    public void tearDown() {
        fileSystem = null;
        configuration = null;
        System.out.println();
        System.out.print("hdfs end...");
    }

}
