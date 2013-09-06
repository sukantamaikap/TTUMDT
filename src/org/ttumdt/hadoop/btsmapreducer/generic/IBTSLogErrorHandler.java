package org.ttumdt.hadoop.btsmapreducer.generic;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 8/27/13
 * Time: 7:01 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IBTSLogErrorHandler {
    public boolean checkCompleteLog(String log);
    public boolean checkValidBTSID (String btsID);
    public boolean checkValidIMSI (String imsi);
    public boolean checkValidTimeStamp (String timeStamp);
    public boolean checkValidDate (String date);
}
