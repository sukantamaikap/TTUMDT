package org.ttumdt.hbase.btshbase.generic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Level;
import org.ttumdt.hbase.btshbase.ITrafficLogTable;

import java.io.IOException;

/**
 * Class to create HBase table
 */
public class BTSLogTrafficLogTableCreator implements ITrafficLogTable {

    public BTSLogTrafficLogTableCreator ()
    {
        LOG.setLevel(Level.ALL);
    }

    public void createTableIfNotCreated (String dateSuffix)
            throws IOException {
        //Is this a good idea to create separate HBase tables per day ??
        //Keeping all data in single table for now
        HTableDescriptor htd = new HTableDescriptor(TRAFFIC_INFO_TABLE_NAME); // + "_"+
                //dateSuffix);
        HColumnDescriptor hcd = new HColumnDescriptor(TRAFFIC_INFO_COLUMN_FAMILY);
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
