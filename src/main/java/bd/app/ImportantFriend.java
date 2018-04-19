package bd.app;

import bd.client.HbaseClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ImportantFriend {
    public void importantFriend(String user){
        HbaseClient.getInstance();
        Get get = new Get(Bytes.toBytes(user));
        List<Put> puts = new ArrayList<Put>();
        Scan scan = new Scan();
        Table users = null;
        Table contacts = null;
        Table messages = null;
        try {
            users = HbaseClient.connection.getTable(TableName.valueOf("USER_NEW"));
            contacts = HbaseClient.connection.getTable(TableName.valueOf("CONTACT_LIST_NEW"));
            messages = HbaseClient.connection.getTable(TableName.valueOf("message_count"));

            scan.setStartRow(user.getBytes());
            scan.setStopRow((user+(byte)256).getBytes());
            ResultScanner results = contacts.getScanner(scan);
            Set<String> user_contacts = new HashSet<String>();
            for (Result result:results){
                user_contacts.add(Bytes.toString(result.getRow()).split("-")[1]);
            }
            results.close();

            for (String contact:user_contacts) {
                int score = 0;
                Scan scan1 = new Scan();
                scan1.setStartRow(contact.getBytes());
                scan1.setStopRow((contact+(byte)256).getBytes());
                ResultScanner results1 = contacts.getScanner(scan1);
                Set<String> contact_contacts = new HashSet<String>();
                for(Result result:results1){
                    contact_contacts.add(Bytes.toString(result.getRow()).split("-")[1]);
                }
                results1.close();
                contact_contacts.retainAll(user_contacts);
                score += contact_contacts.size();
                score += contact_contacts.size()*100/user_contacts.size();

                Get get1 = new Get(Bytes.toBytes(user+"-"+contact));
                Result contact_info = contacts.get(get1);
                int platform_nums = 0;
                for(Cell cell:contact_info.rawCells()){
                    String platform = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if(platform.equals("BEETALK")){
                        platform_nums++;
                    }
                    if(platform.equals("VIBER")){
                        platform_nums++;
                    }
                    if(platform.equals("OOVOO")){
                        platform_nums++;
                    }
                    if(platform.equals("TANGO")){
                        platform_nums++;
                    }
                    if(platform.equals("NIMBUZZ")){
                        platform_nums++;
                    }
                }

                if(platform_nums>1){
                    score += (platform_nums-1)*10;
                }

                Result message = messages.get(get1);
                String msg_count = Bytes.toString(message.getValue(Bytes.toBytes("message_info"),Bytes.toBytes("count")));
                if(msg_count != null){
                    score += Long.parseLong(msg_count)/100;
                }

                if(score>10){
                    Put put1 = new Put(Bytes.toBytes(user+"-"+contact));
                    put1.add(Bytes.toBytes("INFO"),Bytes.toBytes("SCORE"),Bytes.toBytes(String.valueOf(score)));
                    puts.add(put1);
                }

            }
            contacts.put(puts);
            contacts.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
