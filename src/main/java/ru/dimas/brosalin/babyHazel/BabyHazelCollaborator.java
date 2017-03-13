package ru.dimas.brosalin.babyHazel;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import ru.dimas.brosalin.hbaseCollaborator.HbaseCollaboratorWithCrawledTable;

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

    public BabyHazelCollaborator(ClientConfig clientConfig, String mapName){
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

}
