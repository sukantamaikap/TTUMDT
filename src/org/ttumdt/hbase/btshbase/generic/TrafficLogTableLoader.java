package org.ttumdt.hbase.btshbase.generic;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;
import org.ttumdt.hbase.btshbase.ITrafficLogTable;

import java.io.IOException;
import java.util.List;

/**
 * Class to load records in TrafficLog HBase Table
 * This is called by the reducer
 */
public class TrafficLogTableLoader implements ITrafficLogTable {

    public void loadTable (String tableNameSuffix, String key,
                           List<String> values, Reducer.Context context)
            throws IOException, InterruptedException {

        ImmutableBytesWritable putTable = new ImmutableBytesWritable(Bytes.
                toBytes(TRAFFIC_INFO_TABLE_NAME_PREFIX + "_" + tableNameSuffix));

        for(String value : values) {
            final String imsi = value.substring(0,15);
            final String timeStamp = value.substring(16, 21);

            byte[] putKey = Bytes.toBytes(key);
            Put put = new Put(putKey);

            byte[] putFamily = Bytes.toBytes(TRAFFIC_INFO_COLUMN_FAMILY);

            // qualifier btsId
            byte[] putQualifier = Bytes.toBytes(KEY_TRAFFIC_INFO_TABLE);
            byte[] putValue = Bytes.toBytes(key);
            put.add(putFamily, putQualifier, putValue);

            // qualifier imsi
            putQualifier = Bytes.toBytes(COLUMN_IMSI);
            putValue = Bytes.toBytes(imsi);
            put.add(putFamily, putQualifier, putValue);

            // qualifier brand
            putQualifier = Bytes.toBytes(COLUMN_TIMESTAMP);
            putValue = Bytes.toBytes(timeStamp);
            put.add(putFamily, putQualifier, putValue);

            context.write(putTable, put);
        }
    }
}
