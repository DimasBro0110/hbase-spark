package ru.dimas.brosalin.hbaseCollaborator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by DmitriyBrosalin on 11/03/2017.
 */
public class HbaseCollaboratorFilteredTable implements Serializable {
    private Table table;
    private static final Logger logger =
            Logger.getLogger(HbaseCollaboratorFilteredTable.class.getName());

    public HbaseCollaboratorFilteredTable
            (Configuration configurationToFilteredTable,
             String tableName){
        Connection connection = null;
        try{
            connection = ConnectionFactory.createConnection(configurationToFilteredTable);
            table = connection.getTable(TableName.valueOf(tableName));
            logger.log(Level.INFO, "Connection successfully established");
        }catch (Exception ex){

        }
    }

    public void sendToHBase(List<String> messages) throws IOException {
        InetAddress address = InetAddress.getLocalHost();
        String key = address.getHostName();
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        List<Put> lstMessages = new ArrayList<>();
        for(String msg: messages){
            Random random = new Random();
            String curTime =
                    String.valueOf(
                            System.currentTimeMillis() +
                                    random.nextLong() + Long.parseLong(pid.split("@")[0]));
            Put put = new Put(Bytes.toBytes(key + ":" + curTime));
            put.add(Bytes.toBytes("filtered"),
                    Bytes.toBytes("urls"),
                    Bytes.toBytes(msg.split(":")[0])
            );
            put.add(Bytes.toBytes("filtered"),
                    Bytes.toBytes("hash"),
                    Bytes.toBytes(msg.split(":")[1])
            );
            lstMessages.add(put);
        }
        table.put(lstMessages);
    }

    public void sendToHBase(String message, Integer keyHash) throws IOException {
        Put put = new Put(Bytes.toBytes("key"));
        put.add(Bytes.toBytes("filtered"), Bytes.toBytes("urls"), Bytes.toBytes(message));
        put.add(Bytes.toBytes("filtered"), Bytes.toBytes("hash"), Bytes.toBytes(keyHash));
        table.put(put);
        logger.log(Level.INFO, "Message Sent To HBase");
    }
}
