package bd.client;

import bd.constant.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;

/** 
 *
 */
public class HbaseClient {
    static Logger logger=Logger.getLogger(HbaseClient.class);
    private static Configuration configuration;
    public static Connection connection;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("hbase.zookeeper.quorum", Conf.getInstance().getZKconnect());
        configuration.set("hbase.ZooKeeper.property.clientPort", Conf.getInstance().getZKport());
        configuration.setInt("zookeeper.recovery.retry", 3);
        configuration.setInt("hbase.rpc.timeout", 120000);
        configuration.setInt("hbase.client.operation.timeout", 60000);
        configuration.setInt("hbase.client.scanner.timeout.period", 1800000);
    }

    public static HbaseClient getInstance() {
        return new HbaseClient();
    }

    private HbaseClient() {
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("hbase client init faith.");
            logger.error("hbase client init faith.");
            logger.error(e.getMessage());
        }
    }

    public static void close() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("htable client close faith.");
            logger.error("htable client close faith.");
            logger.error(e.getMessage());
        }
    }
}
