package org.ttumdt.hbase.btshbase.generic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 9/6/13
 * Time: 5:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class BTSLogTrafficLogTableMapper {
    private final String TRAFFICINFOTABLENAME = "TrafficInfo";
    private final String TRAFFICINFOCOLUMNDESCRIPTOR = "TimeStamp IMSI";

    public void createTableIfNotCreated ()
            throws IOException {
        //ToDo : Add Date to the table name; this will make querying easier ???
        HTableDescriptor htd = new HTableDescriptor(TRAFFICINFOTABLENAME);
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
