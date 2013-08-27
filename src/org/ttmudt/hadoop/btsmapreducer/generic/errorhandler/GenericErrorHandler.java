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
    @Override
    public boolean checkCompleteLogLength(String log, int expectedLength) {
        return log.length() == expectedLength;
    }

    @Override
    public boolean checkValidBTSID(String btsID, int expectedLength) {
        return btsID.length() == expectedLength;
        //ToDo : implement more for BTSID
    }

    @Override
    public boolean checkValidIMSI(String imsi, int expectedLength) {
        return (imsi.length() == expectedLength);
        //ToDo : implement more
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
        //ToDo Implement more
    }
}
