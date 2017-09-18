package de.comdirect.hadoop.logfile.inputformat.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Collects summary of generated test-logfiles.
 * 
 * @author Nikolaus Winter, comdirect bank AG
 * @since 2017
 */
public class LogfileSummary {

    private long recordCount;
    private Map<LogLevel, Long> recordCountByLevel;

    void addRecord(LogLevel level) {
        recordCount++;
        recordCountByLevel.put(level, recordCountByLevel.get(level) + 1);
    }

    public LogfileSummary() {
        recordCountByLevel = new HashMap<>();
        recordCountByLevel.put(LogLevel.INFO, 0L);
        recordCountByLevel.put(LogLevel.WARN, 0L);
        recordCountByLevel.put(LogLevel.ERROR, 0L);
    }

    public LogfileSummary merge(LogfileSummary logfileStatistics) {
        LogfileSummary mergedLogfileStatistics = new LogfileSummary();
        mergedLogfileStatistics.recordCount = this.recordCount + logfileStatistics.recordCount;
        Arrays.stream(LogLevel.values()).forEach(level -> {
            mergedLogfileStatistics.recordCountByLevel.put(level, recordCountByLevel.get(level) + logfileStatistics.recordCountByLevel.get(level));
        });
        return mergedLogfileStatistics;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getRecordCount(LogLevel logLevel) {
        return recordCountByLevel.get(logLevel);
    }

    @Override
    public String toString() {
        StringBuilder toString = new StringBuilder();
        toString.append(String.format("total # of records: %10d%n", recordCount));
        toString.append(String.format("# of INFO:          %10d%n", recordCountByLevel.get(LogLevel.INFO)));
        toString.append(String.format("# of WARN:          %10d%n", recordCountByLevel.get(LogLevel.WARN)));
        toString.append(String.format("# of ERROR:         %10d%n", recordCountByLevel.get(LogLevel.ERROR)));
        return toString.toString();
    }
}
