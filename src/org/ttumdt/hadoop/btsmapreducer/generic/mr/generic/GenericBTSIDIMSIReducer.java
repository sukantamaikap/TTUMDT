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
 */
public class GenericBTSIDIMSIReducer
        extends Reducer<Text, Text, Text, Text>
        implements IGenericBTSLog {

    @Override
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String keyString = key.toString();
        // Key length is BTSID + DATE = 18 character long
        String dateFromKey = keyString.substring(10,18);
        String btsId = keyString.substring(0,10);

        //ToDo : Investigate -> is this a good idea to populate table from hadoop reducer directly ?
        //ToDo : or will it be good to dump the output of mr in HDFS and load the same to HBase table from HDFS

        // create the table
        BTSLogTrafficLogTableCreator tableCreator = new BTSLogTrafficLogTableCreator();
        tableCreator.createTableIfNotCreated(dateFromKey);

        List<String> entries = new ArrayList<String>();
        for (Text value : values) {
                entries.add(value.toString());
        }
        //ToDo : remove writing if not needed
        //context.write(key,new Text(entries.toString()));

        // Load the table in HBase
        TrafficLogTableLoader trafficLogTableLoader = new TrafficLogTableLoader();
        trafficLogTableLoader.loadTable(dateFromKey, btsId, entries, context);
    }
}
