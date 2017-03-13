package ru.dimas.brosalin.hbaseCollaborator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by DmitriyBrosalin on 09/03/2017.
 */

public class HbaseCollaboratorWithCrawledTable implements Serializable {

    private static Table table;
    private static final Logger logger =
            Logger.getLogger(HbaseCollaboratorWithCrawledTable.class.getName());

    public HbaseCollaboratorWithCrawledTable(Configuration conf, String tableName){
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
            logger.log(Level.INFO, "Connection successfully established");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private Map<String, ArrayList<String>> initializeMapToHazel(
            Map<String, ArrayList<String>> mapFamilyColumnNames){
        if(mapFamilyColumnNames != null && !mapFamilyColumnNames.isEmpty()) {
            logger.log(Level.INFO, "Initializing map configuration of columns per columns family info");
            Map<String, ArrayList<String>> initializedMap = new HashMap<>();
            for (Map.Entry<String, ArrayList<String>> entry : mapFamilyColumnNames.entrySet()) {
                for(String columnName: entry.getValue()) {
                    initializedMap.put(columnName, new ArrayList<String>());
                }
            }
            logger.log(Level.INFO, "Initialization completed successfully");
            return initializedMap;
        }else{
            logger.log(Level.INFO, "The configuration map likely to be empty or null");
            return null;
        }
    }

    public Map<String, ArrayList<String>> fetchDataFromHBaseTable(
            Map<String, ArrayList<String>> familyAndNameColumns,
            int cacheSize,
            int batchSize) throws IOException {
        Map<String, ArrayList<String>> resultFromHbase = initializeMapToHazel(familyAndNameColumns);
//        Scan scan = getInitializedScan(familyAndNameColumns, cacheSize, batchSize);
        Scan scan = new Scan().setCaching(cacheSize).setBatch(batchSize);
        for (Map.Entry<String, ArrayList<String>> entryMapSet : familyAndNameColumns.entrySet()) {
            for (String valueColumn : entryMapSet.getValue()) {
                scan.addColumn(
                        Bytes.toBytes(entryMapSet.getKey()),
                        Bytes.toBytes(valueColumn)
                );
                System.out.println(entryMapSet.getKey());
                System.out.println(valueColumn);
            }
        }
        if(table != null) {
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    String columnName = new String(CellUtil.cloneQualifier(cell));
                    String columnValue = new String(CellUtil.cloneValue(cell));
                    assert resultFromHbase != null;
                    resultFromHbase.get(columnName).add(columnValue);
                }
            }
            return resultFromHbase;
        }else {
            return null;
        }
    }
}
