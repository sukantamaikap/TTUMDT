package org.ttumdt.hbase.btshbase.generic;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.ttumdt.hbase.btshbase.ITrafficLogTable;

import java.io.IOException;
import java.util.List;

/**
 * Class to load records in TrafficLog HBase Table
 * This is called by the reducer
 */
public class TrafficLogTableLoader implements ITrafficLogTable {

    private final int IMSI_START_INDEX = 0;
    private final int IMSI_END_INDEX = 15;
    private final int TIMESTAMP_START_INDEX = 15;
    private final int TIMESTAMP_END_INDEX = 21;


    public TrafficLogTableLoader ()
    {
        LOG.setLevel(Level.ALL);
    }

    public void loadTable (String keyString,
                           List<String> values, Reducer.Context context)
            throws IOException, InterruptedException {
        LOG.info("*********************Loading TrafficLog table for key : " + keyString);

        ImmutableBytesWritable putTable = new ImmutableBytesWritable(Bytes.
                toBytes(TRAFFIC_INFO_TABLE_NAME));
        // i is used as an index for the RowKey
        // ToDo : Figure out a better way to handle this
        int i =0;

        for(String value : values) {
            final String imsi = value.substring(IMSI_START_INDEX,IMSI_END_INDEX);
            final String timeStamp = value.substring(TIMESTAMP_START_INDEX,
                    TIMESTAMP_END_INDEX);

            byte[] putKey = Bytes.toBytes(keyString + i);
            Put put = new Put(putKey);

            byte[] putFamily = Bytes.toBytes(TRAFFIC_INFO_COLUMN_FAMILY);

            // qualifier imsi
            byte[] putQualifier = Bytes.toBytes(COLUMN_IMSI);
            byte[] putValue = Bytes.toBytes(imsi);
            put.add(putFamily, putQualifier, putValue);

            // qualifier brand
            putQualifier = Bytes.toBytes(COLUMN_TIMESTAMP);
            putValue = Bytes.toBytes(timeStamp);
            put.add(putFamily, putQualifier, putValue);

            LOG.info("Loading IMSI : " + imsi);
            LOG.info("Loading TimeStamp : " + timeStamp);

            context.write(putTable, put);
        }
    }
}
