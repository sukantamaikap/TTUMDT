package org.ttumdt.hadoop.btsmapreducer.generic.mr.generic;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.Text;
import org.ttumdt.hadoop.btsmapreducer.generic.errorhandler.GenericErrorHandler;
import org.ttumdt.hadoop.btsmapreducer.generic.mr.IGenericBTSLog;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 8/15/13
 * Time: 12:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenericBTSIDIMSIMapper extends
        Mapper<LongWritable, Text, Text, Text>
        implements IGenericBTSLog {
    /**
     * We are assuming the below log format for BTS logs :
     * Header	BTSID	Direction 	Latitude 	Direction 	Longitude 	IMSI 	Date 	TimeStamp 	SignalStrength
     * Length	10	        1	      8	        1	           8	      15	 8	         6	       4
     * Type	AAAADDDDDD 	D[0/1] 	DDDDDDDD 	   D[0/1] 	     DDDDDDDD 	DDDDDDDDDDDDDDD 	DDMMYYYY 	HHMMSS	DDDD
     * Our Key is : BTSID+Date
     * Value : IMSI+TimeStamp
     */

    public enum GenericError {
        WrongLog,
        WrongBTSID,
        WrongIMSI,
        WrongTimeStamp,
        WrongDate
    }

    private GenericErrorHandler genericErrorHandler = new GenericErrorHandler();

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
        if(genericErrorHandler.checkCompleteLog(line))
        {
            String btsId = line.substring(0,10);
            if(genericErrorHandler.checkValidBTSID(btsId)) {
                String date = line.substring(43,51);
                if(genericErrorHandler.checkValidDate(date)){
                    String imsi = line.substring(28,43);
                    if(genericErrorHandler.checkValidIMSI(imsi)) {
                        String timeStamp = line.substring(51,57);
                        if(genericErrorHandler.checkValidTimeStamp(timeStamp)) {
                            context.write(new Text(btsId + date), new Text(imsi+timeStamp));
                        }
                        else {
                            System.err.println("Wrong timeStamp : " + timeStamp);
                            context.setStatus("Detected possible corrupt timeStamp : see log.");
                            context.getCounter(GenericError.WrongTimeStamp).increment(1);
                        }
                    }
                    else {
                        System.err.println("Wrong IMSI :" + imsi);
                        context.setStatus("Detected possible corrupt imsi : see log.");
                        context.getCounter(GenericError.WrongIMSI).increment(1);
                    }
                }
                else {
                    System.err.println("Wrong date : " + date);
                    context.setStatus("Detected possible corrupt date : see log.");
                    context.getCounter(GenericError.WrongDate).increment(1);
                }

            }
            else {
                System.err.println("Wrong BTSID :" + btsId);
                context.setStatus("Detected possible corrupt BTSID : see log");
                context.getCounter(GenericError.WrongBTSID).increment(1);
            }
        }
        else {
            System.err.println("Wrong log :" + line);
            context.setStatus("Detected possible corrupt log : see log");
            context.getCounter(GenericError.WrongLog).increment(1);
        }
    }
}