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
public class BTSLogTrafficLogTableCreator {
    private final String TRAFFICINFOTABLENAME = "TrafficLog";
    private final String TRAFFICINFOCOLUMNDESCRIPTOR = "TimeStampIMSI";

    public void createTableIfNotCreated (String dateSuffix)
            throws IOException {
        //Is this a good idea to create separate HBase tables per day ??
        HTableDescriptor htd = new HTableDescriptor(TRAFFICINFOTABLENAME + "_"+
                dateSuffix);
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
