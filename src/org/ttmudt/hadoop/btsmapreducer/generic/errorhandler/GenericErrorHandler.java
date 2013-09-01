package org.ttmudt.hadoop.btsmapreducer.generic.errorhandler;

import org.ttmudt.hadoop.btsmapreducer.generic.IBTSLogErrorHandler;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 8/27/13
 * Time: 6:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenericErrorHandler implements IBTSLogErrorHandler {
    /**
     * We are assuming the below log format for BTS logs :
     * Header	BTSID	Direction 	Latitude 	Direction 	Longitude 	IMSI 	Date 	TimeStamp 	SignalStrength
     * Length	10	        1	      8	        1	           8	      15	 8	         6	       4
     * Type	AAAADDDDDD 	D[0/1] 	DDDDDDDD 	   D[0/1] 	     DDDDDDDD 	DDDDDDDDDDDDDDD 	DDMMYYYY 	HHMMSS	DDDD
     * Our Key is : BTSID+Date
     * Value : IMSI+TimeStamp
     */

    private final int COMPLETE_LOG_LENGTH = 61;
    private final int EXPECTED_BTSID_LENGTH = 10;
    private final int EXPECTED_IMSI_LENGTH = 15;

    @Override
    public boolean checkCompleteLog (String log) {
        return log.length() == COMPLETE_LOG_LENGTH;
        // ToDo : implement more
        /**
         * Should not contain any special character
         */
    }

    @Override
    public boolean checkValidBTSID (String btsID) {
        return btsID.length() == EXPECTED_BTSID_LENGTH;
        //ToDo : implement more for BTSID
        /**
         * Should be in format AAAADDDDDD
         */
    }

    @Override
    public boolean checkValidIMSI (String imsi) {
        return (imsi.length() == EXPECTED_IMSI_LENGTH);
        //ToDo : implement more   :
        /**
         * Should contain only numbers
         */
    }

    @Override
    public boolean checkValidTimeStamp (String timeStamp) {
        return (timeStamp.length() == 6) &&
                (Integer.parseInt(timeStamp)>99999) &&
                (Integer.parseInt(timeStamp)<240001);
    }

    @Override
    public boolean checkValidDate (String date)
    {
        return date.length()==8;
        //ToDo Implement more :
        /**
         * Check valid date
         *
         */
    }
}
