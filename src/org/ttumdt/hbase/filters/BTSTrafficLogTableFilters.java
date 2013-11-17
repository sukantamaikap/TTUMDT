package org.ttumdt.hbase.filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.ttumdt.hbase.btshbase.ITrafficLogTable;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Class for all generic filters tied to generic trafficLog
 */
public class BTSTrafficLogTableFilters implements ITrafficLogTable {

    private static Configuration conf = null;
    private final byte[] columnFamily = Bytes.toBytes(TRAFFIC_INFO_COLUMN_FAMILY);
    private final byte[] qualifierIMSI = Bytes.toBytes(COLUMN_IMSI);
    private final byte[] qualifierTimeStamp = Bytes.toBytes(COLUMN_TIMESTAMP);
    public final Logger LOG = Logger.getLogger(MultiTableOutputFormat.class);

    public BTSTrafficLogTableFilters () {
        LOG.setLevel(Level.ALL);
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
    public Map<String, String> getInfoPerBTSID(String btsId, String date,
                                       String startTime, String endTime)
            throws IOException {
        //Set<String> imsis = new HashSet<String>();

        Map<String, String> imsiMap = new HashMap<String, String>();

        //ToDo : better exception handling
        HTable table = new HTable(conf, TRAFFIC_INFO_TABLE_NAME);
        Scan scan = new Scan();

        //scan.addColumn(columnFamily, qualifierIMSI);
        scan.addFamily(columnFamily);
        scan.setFilter(prepFilter(btsId, date, startTime, endTime));

        // filter to build where timestamp

        Result result = null;
        ResultScanner resultScanner = table.getScanner(scan);

        while ((result = resultScanner.next())!= null) {
            //byte[] obtainedColumn = result.getValue(columnFamily, qualifierIMSI);
            //imsis.add(Bytes.toString(obtainedColumn));

            byte[] obtainedColumnIMSI = result.getValue(columnFamily, qualifierIMSI);
            byte[] obtainedColumnTimeStamp = result.getValue(columnFamily,
                    qualifierTimeStamp);

            //imsis.add(Bytes.toString(obtainedColumnTimeStamp));

           imsiMap.put(Bytes.toString(obtainedColumnIMSI),
                    Bytes.toString(obtainedColumnTimeStamp));
        }

        resultScanner.close();
        //return  imsis;
        return imsiMap;
    }

    //ToDo : Figure out how valid is this filter code?? How comparison happens
    // with equal or grater than equal etc


    private Filter prepFilter (String btsId, String date,
                               String startTime, String endTime)
    {
        byte[] tableKey = Bytes.toBytes(KEY_TRAFFIC_INFO_TABLE_BTS_ID);
        byte[] timeStamp = Bytes.toBytes(COLUMN_TIMESTAMP);

        // filter to build -> where BTS_ID = <<btsId>> and Date = <<date>>
        RowFilter keyFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                //new BinaryComparator(Bytes.toBytes(btsId+date)));
                new SubstringComparator(btsId+date));
        // filter to build -> where timeStamp >= startTime
        SingleColumnValueFilter singleColumnValueFilterStartTime =
                new SingleColumnValueFilter(columnFamily, timeStamp,
                        CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(startTime));

        // filter to build -> where timeStamp <= endTime
        SingleColumnValueFilter singleColumnValueFilterEndTime =
                new SingleColumnValueFilter(columnFamily, timeStamp,
                        CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(endTime));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays
                .asList((Filter)keyFilter,
                       singleColumnValueFilterStartTime, singleColumnValueFilterEndTime));
        return filterList;
    }


    public static void main(String[] args) throws IOException {
        BTSTrafficLogTableFilters flt = new BTSTrafficLogTableFilters();
        FileWriter writer = flt.prepCSVFile("op.csv");

        Map<String, String> imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","010000","020000");
        System.out.println(imsis.toString());
        System.out.println("*********IMSI count for 01 to 02 : " + imsis.size());
        flt.appendData(writer, "020000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","020000","030000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 02 AM to 03 AM : " + imsis.size());
        flt.appendData(writer, "030000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","030000","040000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 03 AM to 04 AM : " + imsis.size());
        flt.appendData(writer, "040000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","040000","050000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 04 AM to 05 AM : " + imsis.size());
        flt.appendData(writer, "050000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","050000","060000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 05 AM to 06 AM : " + imsis.size());
        flt.appendData(writer, "060000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","060000","070000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 06 AM to 07 AM : " + imsis.size());
        flt.appendData(writer, "070000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","070000","080000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 07 AM to 08 AM : " + imsis.size());
        flt.appendData(writer, "080000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","080000","090000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 08 AM to 09 AM : " + imsis.size());
        flt.appendData(writer, "090000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","090000","100000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 09 AM to 10 AM : " + imsis.size());
        flt.appendData(writer, "100000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","100000","110000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 10 AM to 11 AM : " + imsis.size());
        flt.appendData(writer, "110000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","110000","120000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 11 AM to 12 Noon : " + imsis.size());
        flt.appendData(writer, "120000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","120000","130000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 12 Noon to 01 PM : " + imsis.size());
        flt.appendData(writer, "130000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","130000","140000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 01 pm to 02 PM : " + imsis.size());
        flt.appendData(writer, "140000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","140000","150000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 02 pm to 03 PM : " + imsis.size());
        flt.appendData(writer, "150000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","150000","160000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 03 pm to 04 PM : " + imsis.size());
        flt.appendData(writer, "160000", ""+imsis.size());


        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","160000","170000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 04 pm to 05 PM : " + imsis.size());
        flt.appendData(writer, "170000", ""+imsis.size());


        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","170000","180000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 05 pm to 06 PM : " + imsis.size());
        flt.appendData(writer, "180000", ""+imsis.size());


        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","180000","190000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 06 pm to 07 PM : " + imsis.size());
        flt.appendData(writer, "190000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","190000","200000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 07 pm to 08 PM : " + imsis.size());
        flt.appendData(writer, "200000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","200000","210000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 08 pm to 09 PM : " + imsis.size());
        flt.appendData(writer, "210000", ""+imsis.size());


        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","210000","220000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 09 pm to 10 PM : " + imsis.size());
        flt.appendData(writer, "220000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","220000","230000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 10 pm to 11 PM : " + imsis.size());
        flt.appendData(writer, "230000", ""+imsis.size());

        imsis= flt.getInfoPerBTSID("AMCD000784", "26082013","230000","240000");
        System.out.println(imsis.toString());
        System.out.println("********IMSI count for 11 pm to 12 midnight : " + imsis.size());
        flt.appendData(writer, "240000", ""+imsis.size());

        flt.close(writer);
    }


    // temporary code for demo to show the D3 part
    // creates a csv file with endTime and count and header

    private FileWriter prepCSVFile ( String fileName) throws IOException {
        FileWriter writer = new FileWriter(fileName);
        writer.append("EndTime");
        writer.append(',');
        writer.append("Count");
        writer.append('\n');

        return writer;
    }

    private FileWriter appendData (FileWriter writer, String endTime, String count) throws IOException {
        writer.append(endTime);
        writer.append(',');
        writer.append(count);
        writer.append("\n");

        return writer;
    }

    private void close (FileWriter writer) throws IOException {
        writer.flush();
        writer.close();
    }
}
