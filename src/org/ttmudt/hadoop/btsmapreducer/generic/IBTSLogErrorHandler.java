package org.ttmudt.hadoop.btsmapreducer.generic;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 8/27/13
 * Time: 7:01 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IBTSLogErrorHandler {
    public boolean checkCompleteLogLength (String log, int expectedLength);
    public boolean checkValidBTSID (String btsID, int expectedLength);
    public boolean checkValidIMSI (String imsi, int expectedLength);
    public boolean checkValidTimeStamp (String timeStamp);
    public boolean checkValidDate (String date);
}
