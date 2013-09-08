package org.ttumdt.hbase.btshbase;

public interface ITrafficLogTable {
    final String TRAFFIC_INFO_TABLE_NAME_PREFIX = "TrafficLog";
    final String TRAFFIC_INFO_COLUMN_FAMILY = "TimeStampIMSI";

    final String KEY_TRAFFIC_INFO_TABLE = "BTS_ID";
    final String COLUMN_IMSI = "IMSI";
    final String COLUMN_TIMESTAMP = "TIME_STAMP";
}
