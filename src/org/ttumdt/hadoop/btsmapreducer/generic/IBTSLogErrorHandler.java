package org.ttumdt.hadoop.btsmapreducer.generic;

public interface IBTSLogErrorHandler {
    public boolean checkCompleteLog(String log);
    public boolean checkValidBTSID (String btsID);
    public boolean checkValidIMSI (String imsi);
    public boolean checkValidTimeStamp (String timeStamp);
    public boolean checkValidDate (String date);
}
