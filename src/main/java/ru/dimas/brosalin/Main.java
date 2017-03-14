package ru.dimas.brosalin;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import ru.dimas.brosalin.babyHazel.BabyHazelCollaborator;
import ru.dimas.brosalin.hbaseCollaborator.HbaseCollaboratorFilteredTable;
import ru.dimas.brosalin.hbaseCollaborator.HbaseCollaboratorWithCrawledTable;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by DmitriyBrosalin on 07/03/2017.
 */


public class Main {

    private static final Logger logger =
            Logger.getLogger(Main.class.getName());

    public static void main(final String[] args) throws IOException, InterruptedException {

        if(args.length > 4) {
            String tableName = args[0];
            String familyName = args[1];
            String columnName = args[2];
            final String[] adressesViaComma = args[3].split(":");
            String fileInHdfs = args[4];

            SparkConf sparkConf = new SparkConf().setAppName("brosalin-hbase-hazel");
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

            JavaRDD<String> rdd = javaSparkContext.textFile(fileInHdfs);

            final Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "tele2-cdh-nn");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            final ClientConfig clientConfig = new ClientConfig();
            clientConfig
                    .getNetworkConfig();
            for(String str: adressesViaComma){
                clientConfig.addAddress(str);
            }

//            HbaseCollaboratorWithCrawledTable collaboratorHBase =
//                    new HbaseCollaboratorWithCrawledTable(conf, tableName, clientConfig, "hbase_map");

//            final HbaseCollaboratorFilteredTable collaboratorFilteredTable =
//                    new HbaseCollaboratorFilteredTable(conf, "FilteredUrlsDimas");

            Map<String, ArrayList<String>> configMap = new HashMap<>();
            configMap.put(familyName, new ArrayList<String>());
            configMap.get(familyName).add(columnName);

//            collaboratorHBase.fedHazel(50, 50, configMap);

//            BabyHazelCollaborator babyHazel = new BabyHazelCollaborator(clientConfig, "hbase_map");
//            babyHazel.compareWithHazelConsistentence(rdd, collaboratorFilteredTable);

//            final HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);
//            final Map<Integer, String> hazelMap = instance.getMap("hbase_map");

            rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> stringIterator) throws Exception {
                    final ClientConfig clientConfig = new ClientConfig();
                    clientConfig
                            .getNetworkConfig();
                    for(String str: adressesViaComma){
                        clientConfig.addAddress(str);
                    }
                    HazelcastInstance instance =
                            HazelcastClient.newHazelcastClient(clientConfig);
                    Map<Integer, String> hazelMap = instance.getMap("hbase_map");
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", "tele2-cdh-nn");
                    conf.set("hbase.zookeeper.property.clientPort", "2181");
                    HbaseCollaboratorFilteredTable collaboratorFilteredTable =
                            new HbaseCollaboratorFilteredTable(conf, "FilteredUrlsDimas");
                    List<String> filtered = new ArrayList<String>();
                    while(stringIterator.hasNext()){
                        String curLine = stringIterator.next();
                        if(!hazelMap.containsKey(curLine.hashCode())){
                            filtered.add(curLine + ":" + curLine.hashCode());
                        }
                    }
                    collaboratorFilteredTable.sendToHBase(filtered);
                    instance.getLifecycleService().shutdown();
                }
            });

//
//            instance.getLifecycleService().shutdown();
        }
    }

}
