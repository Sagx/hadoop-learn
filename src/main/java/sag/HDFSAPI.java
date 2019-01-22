package sag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class HDFSAPI
{
    public static void main( String[] args ) throws Exception {
        //跟HDFS建立连接，知道NameNode的地址即可
        Configuration config = new Configuration();
        config.set("fs.defaultFS","hdfs://192.168.1.111:9000");
        FileSystem fileSystem = FileSystem.get(config);
        //打开一个输入流
        FSDataInputStream in = fileSystem.open(new Path("/test-hdfs.txt"));
        //打开本地输出流
        OutputStream out = new FileOutputStream("d://tmp//hdfs.txt");
        //in -> out
        IOUtils.copyBytes(in,out,1024,true);
        System.out.println("Done!");
    }
}
