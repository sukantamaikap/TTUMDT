package org.ttmudt.hadoop.btsmapreducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 8/15/13
 * Time: 12:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class BTSIDIMSIMapper extends  Mapper<LongWritable, Text, Text, Text> {
    /**
     * We are assuming the below log format for BTS logs :
     * Header	BTSID	Direction 	Latitude 	Direction 	Longitude 	IMSI 	Date 	TimeStamp 	SignalStrength
     * Length	10	        1	      8	        1	           8	      15	 8	         6	       4
     * Type	AAAADDDDDD 	D[0/1] 	DDDDDDDD 	   D[0/1] 	     DDDDDDDD 	DDDDDDDDDDDDDDD 	DDMMYYYY 	HHMMSS	DDDD
     * Our Key is : BTSID+Date
     * Value : IMSI+TimeStamp
     */

    /**
     * @param key :  BTSID+Date
     * @param value : IMSI+TimeStamp
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    public void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String btsId = line.substring(0,10);
        String date = line.substring(43,51);
        String imsi = line.substring(28,43);
        String timeStamp = line.substring(51,57);
        context.write(new Text(btsId + date), new Text(imsi+timeStamp));
    }
}
