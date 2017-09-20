package de.comdirect.hadoop.logfile.inputformat.test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.comdirect.hadoop.logfile.inputformat.LogfileInputFormat;
import scala.Tuple2;

/**
 * Logfile type for test of {@link LogfileInputFormat}.
 * 
 * To test the usage with different formats/patterns, we have to use at least two of them ;-)
 *
 * @author comdirect
 * @since 2017
 */
public enum LogfileType {

    A("^(?<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2}\\s[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3})\\s\\|\\s(?<loglevel>INFO|WARN|ERROR)\\s\\|\\s.*"),

    B("^(?<loglevel>INFO|WARN|ERROR)\\s\\|\\s(?<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2}\\s[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3})\\s\\|\\s.*");

    private Pattern firstlinePattern;
    private Pattern matcherPattern;

    private static final DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    private LogfileType(String pattern) {
        this.firstlinePattern = Pattern.compile(pattern);
        this.matcherPattern = Pattern.compile(pattern, Pattern.DOTALL);
    }

    public Pattern getFirstlinePattern() {
        return firstlinePattern;
    }

    public Pattern getMatcherPattern() {
        return matcherPattern;
    }

    public static LogfileType random() {
        return LogfileType.values()[LogfileGenerator.random.nextInt(2)];
    }

    public Tuple2<LocalDateTime, LogLevel> parse(String record) {
        Matcher matcher = getMatcherPattern().matcher(record);
        matcher.matches();
        return new Tuple2<>(LocalDateTime.parse(matcher.group("timestamp"), timestampFormatter), LogLevel.valueOf(matcher.group("loglevel")));
    }
}