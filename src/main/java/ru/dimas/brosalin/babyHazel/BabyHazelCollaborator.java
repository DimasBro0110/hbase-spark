package ru.dimas.brosalin.babyHazel;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import ru.dimas.brosalin.hbaseCollaborator.HbaseCollaboratorFilteredTable;
import ru.dimas.brosalin.hbaseCollaborator.HbaseCollaboratorWithCrawledTable;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by DmitriyBrosalin on 09/03/2017.
 */
public class BabyHazelCollaborator {

    private static Logger logger = Logger.getLogger(BabyHazelCollaborator.class.getName());
    private ClientConfig clientConfig;
    private String mapName;
    private HazelcastInstance hazelcastInstance;
    private Map<Integer, String> hazelMap;

    public BabyHazelCollaborator
            (ClientConfig clientConfig, String mapName){
        this.clientConfig = clientConfig;
        this.mapName = mapName;
        this.hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
        this.hazelMap = this.hazelcastInstance.getMap(this.mapName);
    }

    public void sendToMap(Map<Integer, String> mapToSend){
        if(this.hazelcastInstance != null){
            for(Map.Entry<Integer, String> entry: mapToSend.entrySet()){
                if(!this.hazelMap.containsKey(entry.getKey())){
                    this.hazelMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

//    public List<String> compareWithHazelConsistentence
//            (JavaRDD<String> rdd,
//             HbaseCollaboratorFilteredTable hbaseCollaboratorFilteredTable) throws IOException {
//        final List<String> lst = new ArrayList<String>();
//        rdd.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s, s.hashCode());
//            }
//        }).map(new Function<Tuple2<String,Integer>, Integer>() {
//            @Override
//            public Integer call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                if(hazelcastInstance != null){
//                    if(!hazelMap.containsKey(stringIntegerTuple2._2())){
//                        lst.add(stringIntegerTuple2._1());
//                    }
//                }
//                return 1;
//            }
//        });
//        hbaseCollaboratorFilteredTable.sendToHBase(lst);
//        return lst;
//    }

}
