package org.ttumdt.hbase.btshbase.generic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class to create HBase table
 */
public class BTSLogTrafficLogTableMapper {
    private final String TRAFFICINFOTABLENAME = "TrafficInfo";
    private final String TRAFFICINFOCOLUMNDESCRIPTOR = "TimeStamp IMSI";

    public void createTableIfNotCreated ()
            throws IOException {
        //Is this a good idea to create separate HBase tables per day ??
        HTableDescriptor htd = new HTableDescriptor(TRAFFICINFOTABLENAME + "_"+
                new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        HColumnDescriptor hcd = new HColumnDescriptor(TRAFFICINFOCOLUMNDESCRIPTOR);
        htd.addFamily(hcd);

        byte[] tableName = htd.getName();

        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        if(!admin.tableExists(tableName)) {
            admin.createTable(htd);
            // ToDo : Exception handling in case the table is not created
        }
        else {
            // log it
        }
    }

}
