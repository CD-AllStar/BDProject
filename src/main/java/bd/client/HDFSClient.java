package bd.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class HDFSClient {

        /**上传文件到HDFS上去*/
    private static void uploadToHdfs(String local,String dst) throws FileNotFoundException,IOException {
//        String localSrc = "d://qq.txt";
//        String dst = "hdfs://hadoop001:8020/user";
        InputStream in = new BufferedInputStream(new FileInputStream(local));
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**从HDFS上读取文件*/
    private static void readFromHdfs(String dst,String local) throws FileNotFoundException,IOException {
//        String dst = "hdfs://hadoop001:8032/user/zhangzk/qq.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(dst));
//        String local = "d:/qq-hdfs.txt";
        OutputStream out = new FileOutputStream(local);
        byte[] ioBuffer = new byte[1024];
        int readLen = hdfsInStream.read(ioBuffer);
        while(-1 != readLen){
            out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        out.close();
        hdfsInStream.close();
        fs.close();
    }

    /**以append方式将内容添加到HDFS上文件的末尾;注意：文件更新，需要在hdfs-site.xml中添<property><name>dfs.append.support</name><value>true</value></property>*/
    public static void appendToHdfs(String path,String content) throws FileNotFoundException,IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.append(new Path(path));
        out.writeUTF(content);
        out.close();
        fs.close();
    }

    public static void writeToHdfs(String path,String content) throws FileNotFoundException,IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(path));
        out.writeUTF(content);
        out.close();
        fs.close();
    }

    /**从HDFS上删除文件*/
    public static void deleteFromHdfs(String path) throws FileNotFoundException,IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(new Path(path));
        fs.close();
    }

    /**遍历HDFS上的文件和目录*/
    private static void getDirectoryFromHdfs(String dst) throws FileNotFoundException,IOException {
//        String dst = "hdfs://hadoop001:8032";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FileStatus fileList[] = fs.listStatus(new Path(dst));
        int size = fileList.length;
        for(int i = 0; i < size; i++){
            System.out.println("name:" + fileList[i].getPath().getName() + "/t/tsize:" + fileList[i].getLen());
        }
        fs.close();
    }
}


