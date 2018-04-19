package bd.constant;

import bd.client.HDFSClient;
import bd.util.JDBClUtil;
import com.spreada.utils.chinese.ZHConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.nlpcn.commons.lang.tire.domain.Value;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Constant {
//    public static final StanfordCoreNLP corenlp = new StanfordCoreNLP("StanfordCoreNLP-chinese.properties");
    public static final ZHConverter converter = ZHConverter.getInstance(ZHConverter.SIMPLIFIED);
    public static final List<Value> values=getValues();
    public static final List<String> stopWords=getStopWords();
    public static final List<String[]> synonyms=getSynonyms();



    public static void initDic() {
        JDBClUtil mysql = new JDBClUtil();
        String sql1 = "select * from whitename";
        String path1 = "/user/wordCount/dic/whitename.dic";
        String sql2 = "select * from blackname";
        String path2 = "/user/wordCount/dic/blackname.dic";
        String sql3 = "SELECT word,GROUP_CONCAT(synonym SEPARATOR ',') synonyms FROM synonyms group by word";
        String path3 = "/user/wordCount/dic/synonyms.dic";
        mysql.getConnection();
        try {
            List<Map<String,Object>> whitenames = mysql.findModeResult(sql1,null);
            StringBuffer stringBuffer1 = new StringBuffer();
            for (Map result:whitenames) {
                stringBuffer1.append("\n");
                stringBuffer1.append(result.get("name").toString());
                stringBuffer1.append(",");
                stringBuffer1.append(result.get("type").toString());
                stringBuffer1.append(",");
                stringBuffer1.append(result.get("score").toString());
            }
            HDFSClient.deleteFromHdfs(path1);
            HDFSClient.writeToHdfs(path1,stringBuffer1.toString());

            List<Map<String,Object>> blacknames = mysql.findModeResult(sql2,null);
            StringBuffer stringBuffer2 = new StringBuffer();
            for (Map result:blacknames) {
                stringBuffer2.append("\n");
                stringBuffer2.append(result.get("name").toString());
            }
            HDFSClient.deleteFromHdfs(path2);
            HDFSClient.writeToHdfs(path2,stringBuffer2.toString());

            List<Map<String,Object>> synonyms = mysql.findModeResult(sql3,null);
            StringBuffer stringBuffer3 = new StringBuffer();
            for (Map result:synonyms) {
//                System.out.println(result.get("word").toString());
//                System.out.println(result.get("synonyms").toString());
                stringBuffer3.append("\n");
                stringBuffer3.append(result.get("word").toString());
                stringBuffer3.append(",");
                stringBuffer3.append(result.get("synonyms").toString());
            }
            HDFSClient.deleteFromHdfs(path3);
            HDFSClient.writeToHdfs(path3,stringBuffer3.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<String> getStopWords() {
        Configuration conf = new Configuration();
        List<String> stopWords = new ArrayList<String>();
        String path = "/user/wordCount/dic/blackname.dic";
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(path));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsInStream,"UTF-8"));
            String head = bufferedReader.readLine();
            String line = bufferedReader.readLine();
            while (line != null){
                stopWords.add(line.trim());
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stopWords;
    }


    public  static List<Value> getValues() {
        Configuration conf = new Configuration();
        List<Value> list = new ArrayList<Value>();
        String path = "/user/wordCount/dic/whitename.dic";
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(path));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsInStream,"UTF-8"));
            String head = bufferedReader.readLine();
            String line = bufferedReader.readLine();
            while (line != null){
                String name = line.split(",")[0].trim();
                String type = line.split(",")[1].trim();
                String score = line.split(",")[2].trim();
                list.add(new Value(name,type,score));
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public  static List<String[]> getSynonyms() {
        Configuration conf = new Configuration();
        List<String[]> list = new ArrayList<String[]>();
        String path = "/user/wordCount/dic/synonyms.dic";
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(path));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsInStream,"UTF-8"));
            String head = bufferedReader.readLine();
            String line = bufferedReader.readLine();
            while (line != null){
                list.add(line.split(","));
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

}
