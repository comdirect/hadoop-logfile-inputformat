package de.comdirect.hadoop.logfile.inputformat.cli;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.comdirect.hadoop.logfile.inputformat.LogfileInputFormat;
import de.comdirect.hadoop.logfile.inputformat.test.LogLevel;
import de.comdirect.hadoop.logfile.inputformat.test.LogfileGenerator;
import de.comdirect.hadoop.logfile.inputformat.test.LogfileSummary;
import scala.Tuple2;

/**
 * Tests the {@link LogfileInputFormat} in a real-life Hadoop/Spark environment.
 * 
 * As it is pretty hard to simulate the usage of the input format in a unit test, this class has to be run in a hadoop
 * cluster instead.
 * 
 * @author Nikolaus Winter, comdirect bank AG
 * @since 2017
 */
public class Test {

    private static final String REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2}\\s[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3})\\s\\|\\s(INFO|WARN|ERROR)\\s\\|\\s.*";

    private static final Pattern FIRSTLINE_PATTERN = Pattern.compile(REGEX);

    private static final Pattern MATCHER_PATTERN = Pattern.compile(REGEX, Pattern.DOTALL);

    private static DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    private static Options OPTIONS = new Options();

    private static Option DIRECTORY;

    private static String FILE_EXT_LOG = ".log";

    private static String FILE_EXT_GZ = ".log.gz";

    private static FileSystem hdfs;

    private static Path directory;

    private static Path logDir;

    private static Path logDirGz;

    static {
        DIRECTORY = new Option("d", "directory", true,
                "HDFS directory where the temporary files are stored. Directory has to be empty respectively gets created by this test.");
        DIRECTORY.setRequired(true);
        OPTIONS.addOption(DIRECTORY);
    }

    public static void main(String[] args) throws IOException {

        if (!parseArguments(args)) {
            printHelp();
            System.exit(1);
        }

        final Configuration hadoopConfig = new Configuration(true);
        hdfs = FileSystem.get(hadoopConfig);

        if (hdfs.exists(directory)) {
            if (!hdfs.isDirectory(directory)) {
                System.out.printf("'%s' exists in HDFS, but is not a directory!%n", directory);
                System.exit(1);
            }
            FileStatus[] fileStatus = hdfs.listStatus(directory);
            if (fileStatus.length > 0) {
                System.out.printf("'%s' exists in HDFS, but is not empty!%n", directory);
                System.exit(1);
            }
        }

        createDirectories();

        System.out.printf("Creating test data in '%s'. This may take a while...%n", directory.toString());

        LogfileSummary summary = writeLogFiles();

        SparkConf sparkConfig = new SparkConf().setAppName("Testing LogfileInputFormat.");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);

        hadoopConfig.set(LogfileInputFormat.KEY_FIRSTLINE_PATTERN, FIRSTLINE_PATTERN.pattern());

        JavaPairRDD<Tuple2<Path, Long>, Text> rdd;
        JavaRDD<Tuple2<LocalDateTime, LogLevel>> logRecords;

        rdd = sparkContext.newAPIHadoopFile(logDir + "/*" + FILE_EXT_LOG, LogfileInputFormat.class, LogfileInputFormat.KEY_CLASS, Text.class, hadoopConfig);

        logRecords = rdd.map(Test::parse).cache();
        long totalCountLog = logRecords.count();
        long infoCountLog = logRecords.filter(tuple -> tuple._2 == LogLevel.INFO).count();
        long warnCountLog = logRecords.filter(tuple -> tuple._2 == LogLevel.WARN).count();
        long errorCountLog = logRecords.filter(tuple -> tuple._2 == LogLevel.ERROR).count();

        rdd = sparkContext.newAPIHadoopFile(logDirGz + "/*" + FILE_EXT_GZ, LogfileInputFormat.class, LogfileInputFormat.KEY_CLASS, Text.class, hadoopConfig);

        logRecords = rdd.map(Test::parse).cache();
        long totalCountGz = logRecords.count();
        long infoCountGz = logRecords.filter(tuple -> tuple._2 == LogLevel.INFO).count();
        long warnCountGz = logRecords.filter(tuple -> tuple._2 == LogLevel.WARN).count();
        long errorCountGz = logRecords.filter(tuple -> tuple._2 == LogLevel.ERROR).count();

        long totalCountExpected = summary.getRecordCount();
        long infoCountExpected = summary.getRecordCount(LogLevel.INFO);
        long warnCountExpected = summary.getRecordCount(LogLevel.WARN);
        long errorCountExpected = summary.getRecordCount(LogLevel.ERROR);

        System.out.printf("%n%n%n%30s %15s %15s %15s %15s%n%n", "", "expected", "from *.log", "from *.log.gz", "test result");
        System.out.printf("%30s %15d %15d %15d %15s%n", "total # of log records",
                totalCountExpected, totalCountLog, totalCountGz,
                ((totalCountExpected == totalCountLog && totalCountLog == totalCountGz) ? "SUCCESS" : "FAILURE"));
        System.out.printf("%30s %15d %15d %15d %15s%n", "# of INFO level records",
                infoCountExpected, infoCountLog, infoCountGz,
                ((infoCountExpected == infoCountLog && infoCountLog == infoCountGz) ? "SUCCESS" : "FAILURE"));
        System.out.printf("%30s %15d %15d %15d %15s%n", "# of WARN level records",
                warnCountExpected, warnCountLog, warnCountGz,
                ((warnCountExpected == warnCountLog && warnCountLog == warnCountGz) ? "SUCCESS" : "FAILURE"));
        System.out.printf("%30s %15d %15d %15d %15s%n%n%n", "# of ERROR level records",
                errorCountExpected, errorCountLog, errorCountGz,
                ((errorCountExpected == errorCountLog && errorCountLog == errorCountGz) ? "SUCCESS" : "FAILURE"));
    }

    private static void createDirectories() throws IOException {
        hdfs.mkdirs(directory);

        logDir = new Path(directory, "logs/");
        hdfs.mkdirs(logDir);

        logDirGz = new Path(directory, "logs-gz/");
        hdfs.mkdirs(logDirGz);
    }

    private static LogfileSummary writeLogFiles() throws IOException {

        LogfileSummary summary = new LogfileSummary();

        FSDataOutputStream out;
        DataOutputStream outGz;

        YearMonth thisMonth = YearMonth.now();
        YearMonth previousMonth = thisMonth.minusMonths(1);

        LocalDate day = previousMonth.atDay(1);

        while (day.isBefore(previousMonth.atDay(11))) {
            out = hdfs.create(new Path(logDir, getFilename(day, FILE_EXT_LOG)));
            outGz = new DataOutputStream(new GZIPOutputStream(hdfs.create(new Path(logDirGz, getFilename(day, FILE_EXT_GZ))), true));
            summary = summary.merge(LogfileGenerator.generateLogRecords(day.atStartOfDay(), day.plusDays(1).atStartOfDay(), writeAsUtf8To(out, outGz)));
            out.flush();
            out.close();
            outGz.flush();
            outGz.close();
            day = day.plusDays(1);
        }

        return summary;
    }

    private static Consumer<String> writeAsUtf8To(DataOutput out, DataOutput out2) {
        return string -> {
            try {
                byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
                out.write(bytes);
                out2.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Tuple2<LocalDateTime, LogLevel> parse(Tuple2<Tuple2<Path, Long>, Text> tuple) {
        Matcher matcher = MATCHER_PATTERN.matcher(tuple._2.toString());
        matcher.matches();
        return new Tuple2<>(LocalDateTime.parse(matcher.group(1), timestampFormatter), LogLevel.valueOf(matcher.group(2)));
    }

    private static String getFilename(LocalDate day, String extension) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(day) + extension;
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(150);
        formatter.printHelp(Test.class.getName(), OPTIONS);
    }

    private static boolean parseArguments(String[] args) {
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine commandLine = parser.parse(OPTIONS, args);
            directory = new Path(commandLine.getOptionValue(DIRECTORY.getOpt()));
            return true;
        } catch (ParseException | IllegalArgumentException e) {
            return false;
        }
    }
}
