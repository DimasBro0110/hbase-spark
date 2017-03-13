package ru.dimas.brosalin;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import ru.dimas.brosalin.babyHazel.BabyHazelCollaborator;
import ru.dimas.brosalin.hbaseCollaborator.HbaseCollaboratorWithCrawledTable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;


/**
 * Created by DmitriyBrosalin on 07/03/2017.
 */


public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "tele2-cdh-nn");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        ClientConfig clientConfig = new ClientConfig();
        clientConfig
                .getNetworkConfig()
                .addAddress("");

        HbaseCollaboratorWithCrawledTable collaboratorHBase =
                new HbaseCollaboratorWithCrawledTable(conf, "DimasTest", clientConfig, "hbase_map");

        Map<String, ArrayList<String>> configMap = new HashMap<>();
        configMap.put("FamilyName", new ArrayList<String>());
        configMap.get("FamilyName").add("url");

        collaboratorHBase.fedHazel(50, 50, configMap);


    }

}
