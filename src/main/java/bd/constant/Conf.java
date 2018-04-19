package bd.constant;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hadoop on 18-1-25.
 */
public class Conf {
    Logger logger=Logger.getLogger(Conf.class);
    //hbase
    private String ZKconnect;
    private String ZKport;
    //es
    private String nodes;
    private String index;
    private String type;
    private String cluster_name;
    private String bulkNum;
    private String ConcurrentRequest;
    //ev
    private String log;
    private String rootPath;
    private String aimPath;
    private String file=App.path;
    private Properties props = null;

    public String getZKconnect() {
        return props.getProperty("ZKconnect");
    }

    public String getZKport() {
        return props.getProperty("ZKport");
    }

    public String getTableName() {
        return props.getProperty("tableName");
    }

    public String getScanNum() {
        return props.getProperty("scanNum");
    }

    public String getTempStartRow() {
        return props.getProperty("tempStartRow");
    }

    public String getStopRow() {
        return props.getProperty("stopRow");
    }

    public String getNodes() {
        return props.getProperty("nodes");
    }

    public String getIndex() {
        return props.getProperty("index");
    }

    public String getType() {
        return props.getProperty("type");
    }

    public String getCluster_name() {
        return props.getProperty("cluster_name");
    }

    public String getBulkNum() {
        return props.getProperty("bulkNum");
    }

    public String getConcurrentRequest() {
        return props.getProperty("ConcurrentRequest");
    }

    public String getLog() {
        return props.getProperty("log");
    }
    public String getRootPath() {
        return props.getProperty("rootPath");
    }
    public String getAimPath() {
        return props.getProperty("aimPath");
    }
    public Conf() {
        props = new Properties();
        try {
            InputStream io = new FileInputStream(file);
            props.load(io);
        } catch (FileNotFoundException e) {
            System.out.println("can not read filePath!!! ");
            logger.error("can not read filePath!!! "+e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }
    public static Conf getInstance(){
        return new Conf();
    }
}
