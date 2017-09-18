package de.comdirect.hadoop.logfile.inputformat.test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.function.Consumer;

import de.comdirect.hadoop.logfile.inputformat.LogfileInputFormat;

/**
 * Generates logfiles for testing of the input {@link LogfileInputFormat}
 * 
 * @author Nikolaus Winter, comdirect bank AG
 * @since 2017
 */
public class LogfileGenerator {

    static Random random = new Random();

    static DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    static String nullPointerExceptionStackTrace = stackTrace();

    public static LogfileSummary generateLogRecords(LocalDateTime start, LocalDateTime end, Consumer<String> consumer) {
        LogfileSummary summary = new LogfileSummary();
        LocalDateTime next = start;
        while (next.isBefore(end)) {
            consumer.accept(createLogEntry(next, summary));
            next = next.plusNanos(5000000L);
        }
        return summary;
    }

    static String createLogEntry(LocalDateTime timestamp, LogfileSummary summary) {
        LogLevel level = LogLevel.random();
        String text = randomText(level);
        summary.addRecord(level);
        return String.format("%s | %s | %s | %s%n", timestamp.format(timestampFormatter), level, randomLoggingClass(), text);
    }

    static String randomText(LogLevel level) {
        switch (level) {
        case INFO:
            return String.format("customer #%05d logged in.", random.nextInt(9000) + 1000);
        case WARN:
            return String.format("customer #%05d failed password attempt.", random.nextInt(9000) + 1000);
        case ERROR:
            return nullPointerExceptionStackTrace;
        default:
            return null;
        }
    }

    static enum LoggingClass {
        A("de.comdirect.hadoop.logfile.inputformat.test.A"),

        B("de.comdirect.hadoop.logfile.inputformat.test.B"),

        C("de.comdirect.hadoop.logfile.inputformat.test.C"),

        D("de.comdirect.hadoop.logfile.inputformat.test.D"),

        E("de.comdirect.hadoop.logfile.inputformat.test.E");

        private String fullyQualifiedClassName;

        private LoggingClass(String fullyQualifiedClassName) {
            this.fullyQualifiedClassName = fullyQualifiedClassName;
        }

        public String getFullyQualifiedClassName() {
            return fullyQualifiedClassName;
        }
    }

    static String randomLoggingClass() {
        return LoggingClass.values()[random.nextInt(5)].getFullyQualifiedClassName();
    }

    @SuppressWarnings("null")
    static String stackTrace() {
        String bomb = null;
        try {
            return bomb.toString();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            return writer.toString();
        }
    }
}
