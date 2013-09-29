package org.ttumdt.hbase.filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.ttumdt.hbase.btshbase.ITrafficLogTable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Class for all generic filters tied to generic trafficLog
 */
public class BTSTrafficLogTableFilters implements ITrafficLogTable {

    private static Configuration conf = null;
    private final byte[] columnFamily = Bytes.toBytes(TRAFFIC_INFO_COLUMN_FAMILY);
    private final byte[] qualifier= Bytes.toBytes(COLUMN_IMSI);

    public BTSTrafficLogTableFilters () {
        conf = HBaseConfiguration.create();
        //conf.addResource();
    }

    /**
     * This filter will return list of IMSIs for a given btsId and ime interval
     * @param btsId : btsId for which the query has to run
     * @param startTime : start time for which the query has to run
     * @param endTime : end time for which the query has to run
     * @return returns IMSIs as set of Strings
     * @throws IOException
     */
    public Set<String> getInfoPerBTSID(String btsId, String date,
                                       String startTime, String endTime)
            throws IOException {
        Set<String> imsis = new HashSet<String>();

        //ToDo : better exception handling
        HTable table = new HTable(conf, TRAFFIC_INFO_TABLE_NAME);
        Scan scan = new Scan();

        scan.addColumn(columnFamily,qualifier);
        scan.setFilter(prepFilter(btsId, date, startTime, endTime));

        // filter to build where timestamp

        Result result = null;
        ResultScanner resultScanner = table.getScanner(scan);

        while ((result = resultScanner.next())!= null) {
            byte[] obtainedColumn = result.getValue(columnFamily,qualifier);
            imsis.add(Bytes.toString(obtainedColumn));
        }

        resultScanner.close();

        return imsis;
    }

    private Filter prepFilter (String btsId, String date,
                               String startTime, String endTime)
    {
        byte[] tableKey = Bytes.toBytes(KEY_TRAFFIC_INFO_TABLE_BTS_ID);
        byte[] timeStamp = Bytes.toBytes(COLUMN_TIMESTAMP);

        // filter to build -> where BTS_ID = <<btsId>> and Date = <<date>>
        SingleColumnValueFilter singleColumnValueFilterBTSId =
                new SingleColumnValueFilter
                        (columnFamily, tableKey, CompareFilter.CompareOp.EQUAL,
                                Bytes.toBytes(btsId+date));

        // filter to build -> where timeStamp >= startTime
        SingleColumnValueFilter singleColumnValueFilterStartTime =
                new SingleColumnValueFilter(columnFamily, timeStamp,
                        CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(startTime));

        // filter to build -> where timeStamp <= endTime
        SingleColumnValueFilter singleColumnValueFilterEndTime =
                new SingleColumnValueFilter(columnFamily, timeStamp,
                        CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(endTime));

        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays
                .asList((Filter)singleColumnValueFilterBTSId,
                       singleColumnValueFilterStartTime, singleColumnValueFilterEndTime));
        return filter;
    }


    public static void main(String[] args) throws IOException {
        BTSTrafficLogTableFilters flt = new BTSTrafficLogTableFilters();
        Set<String> imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","104092","104095");
        System.out.println(imsis.toString());
    }
}
