package org.ttumdt.hadoop.btsmapreducer.generic.mr.generic;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ttumdt.hadoop.btsmapreducer.generic.mr.IGenericBTSLog;
import org.ttumdt.hbase.btshbase.generic.BTSLogTrafficLogTableCreator;
import org.ttumdt.hbase.btshbase.generic.TrafficLogTableLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic Reducer for generic assumed log
 * The Mapper churns out data in the below format :
 ***********************************************
 *  Keys :
 *  Field : Length
 *  BITS_ID :   10
 *  DATE : 8
 *
 *  Values:
 *  Field : Length
 *  IMSI : 15
 *  TIME_STAMP : 6
 ***********************************************
 */
public class GenericBTSIDIMSIReducer
        extends Reducer<Text, Text, Text, Text>
        implements IGenericBTSLog {

    private final int DATE_START_INDEX = 10;
    private final int DATE_END_INDEX = 18;

    @Override
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String keyString = key.toString();

        String dateFromKey = keyString.substring(DATE_START_INDEX,DATE_END_INDEX);

        // create the table
        BTSLogTrafficLogTableCreator tableCreator = new BTSLogTrafficLogTableCreator();
        tableCreator.createTableIfNotCreated(dateFromKey);

        List<String> entries = new ArrayList<String>();
        for (Text value : values) {
                entries.add(value.toString());
        }

        // Load the table in HBase
        TrafficLogTableLoader trafficLogTableLoader = new TrafficLogTableLoader();
        trafficLogTableLoader.loadTable(keyString, entries, context);
    }
}
