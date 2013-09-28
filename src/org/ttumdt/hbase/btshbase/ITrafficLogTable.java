package org.ttumdt.hbase.btshbase;

public interface ITrafficLogTable {
    final String TRAFFIC_INFO_TABLE_NAME = "TrafficLog";
    final String TRAFFIC_INFO_COLUMN_FAMILY = "TimeStampIMSI";

    final String KEY_TRAFFIC_INFO_TABLE_BTS_ID = "BTS_ID";
    final String KEY_TRAFFIC_INFO_TABLE_DATE = "DATE";
    final String COLUMN_IMSI = "IMSI";
    final String COLUMN_TIMESTAMP = "TIME_STAMP";
}
