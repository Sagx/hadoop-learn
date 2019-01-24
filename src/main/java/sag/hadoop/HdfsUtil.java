package sag.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class HdfsUtil{

    private static FileSystem fileSystem;

    static {
        try {
            //跟HDFS建立连接，配置NameNode地址，模拟root用户，以防权限问题造成读写失败
            fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.18.217:9000"),new Configuration(),"root");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Init hdfs fileSystem fail!");
        }
    }

    //下载文件
    public static void getFile(String hdfsFilePath, String localDir) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(hdfsFilePath));
        OutputStream out = new FileOutputStream(localDir);
        IOUtils.copyBytes(in,out,1024,true);
        System.out.println(hdfsFilePath + " => " + localDir + " Success!");
    }

    //上传文件
    public static void putFile(String localFilePath, String hdfsDir) throws IOException {
        InputStream in = new FileInputStream(localFilePath);
        FSDataOutputStream out = fileSystem.create(new Path(hdfsDir));
        IOUtils.copyBytes(in,out,1024,true);
        System.out.println(localFilePath + " => " + hdfsDir + " Success!");
    }

    //删除文件
    public static boolean delFile(String hdfsFilePath) throws IOException {
        boolean flag = fileSystem.delete(new Path(hdfsFilePath), true);
        String res = flag ? "Success!" : "Fail!";
        System.out.println("Delete " + hdfsFilePath + " " + res);
        return flag;
    }

    //test
    public static void main( String[] args ) throws IOException {
        HdfsUtil.putFile("d://tmp//sag.txt", "/sag.txt");
        HdfsUtil.getFile("/LICENSE.txt", "d://tmp//text.txt");

        //hadoop自己的上传下载
        fileSystem.copyToLocalFile(new Path("d://tmp//sag.txt"), new Path("/sag.txt"));
        fileSystem.copyFromLocalFile(new Path("/LICENSE.txt"), new Path("d://tmp//text.txt"));

        HdfsUtil.delFile("/LICENSE.txt");
    }

    //Flume已封装hadoopAPI

}
