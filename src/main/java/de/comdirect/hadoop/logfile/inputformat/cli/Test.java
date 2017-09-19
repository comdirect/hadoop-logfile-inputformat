package de.comdirect.hadoop.logfile.inputformat.cli;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
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
import org.apache.spark.api.java.function.Function;

import de.comdirect.hadoop.logfile.inputformat.LogfileInputFormat;
import de.comdirect.hadoop.logfile.inputformat.test.LogLevel;
import de.comdirect.hadoop.logfile.inputformat.test.LogfileGenerator;
import de.comdirect.hadoop.logfile.inputformat.test.LogfileSummary;
import de.comdirect.hadoop.logfile.inputformat.test.LogfileType;
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

        final Configuration hadoopConfig = new Configuration(true);
        hdfs = FileSystem.get(hadoopConfig);

        if (!parseArguments(args)) {
            printHelp();
            System.exit(1);
        }

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

        Map<String, LogfileType> logfileTypeByPath = new HashMap<>();

        LogfileSummary summary = writeLogFiles(logfileTypeByPath);

        SparkConf sparkConfig = new SparkConf().setAppName("Testing LogfileInputFormat.");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);

        logfileTypeByPath.forEach((path, type) -> {
            LogfileInputFormat.setPattern(hadoopConfig, path, type.getFirstlinePattern());
        });
        LogfileInputFormat.setPattern(hadoopConfig, LogfileType.A.getFirstlinePattern());

        JavaPairRDD<Tuple2<Path, Long>, Text> rdd;
        JavaRDD<Tuple2<LocalDateTime, LogLevel>> logRecords;

        rdd = sparkContext.newAPIHadoopFile(logDir + "/*" + FILE_EXT_LOG, LogfileInputFormat.class, LogfileInputFormat.KEY_CLASS, Text.class, hadoopConfig);

        Function<Tuple2<Tuple2<Path, Long>, Text>, Tuple2<LocalDateTime, LogLevel>> mappingFunction = mappingFunction(logfileTypeByPath);

        logRecords = rdd.map(mappingFunction).cache();
        long totalCountLog = logRecords.count();
        long infoCountLog = logRecords.filter(tuple -> tuple._2 == LogLevel.INFO).count();
        long warnCountLog = logRecords.filter(tuple -> tuple._2 == LogLevel.WARN).count();
        long errorCountLog = logRecords.filter(tuple -> tuple._2 == LogLevel.ERROR).count();

        rdd = sparkContext.newAPIHadoopFile(logDirGz + "/*" + FILE_EXT_GZ, LogfileInputFormat.class, LogfileInputFormat.KEY_CLASS, Text.class, hadoopConfig);

        logRecords = rdd.map(mappingFunction).cache();
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

        sparkContext.close();
    }

    private static void createDirectories() throws IOException {
        hdfs.mkdirs(directory);

        logDir = new Path(directory, "logs/");
        hdfs.mkdirs(logDir);

        logDirGz = new Path(directory, "logs-gz/");
        hdfs.mkdirs(logDirGz);
    }

    private static LogfileSummary writeLogFiles(Map<String, LogfileType> logfileTypeByPath) throws IOException {

        LogfileSummary summary = new LogfileSummary();

        FSDataOutputStream out;
        DataOutputStream outGz;

        YearMonth thisMonth = YearMonth.now();
        YearMonth previousMonth = thisMonth.minusMonths(1);

        LocalDate day = previousMonth.atDay(1);

        while (day.isBefore(previousMonth.atDay(11))) {
            LogfileType logfileType = LogfileType.random();
            Path pathLogfile = new Path(logDir, getFilename(day, FILE_EXT_LOG));
            out = hdfs.create(pathLogfile);
            Path pathLogfileGz = new Path(logDirGz, getFilename(day, FILE_EXT_GZ));
            outGz = new DataOutputStream(new GZIPOutputStream(hdfs.create(pathLogfileGz), true));
            summary = summary
                    .merge(LogfileGenerator.generateLogRecords(logfileType, day.atStartOfDay(), day.plusDays(1).atStartOfDay(), writeAsUtf8To(out, outGz)));
            out.flush();
            out.close();
            outGz.flush();
            outGz.close();
            logfileTypeByPath.put(pathLogfile.toString(), logfileType);
            logfileTypeByPath.put(pathLogfileGz.toString(), logfileType);
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

    private static Function<Tuple2<Tuple2<Path, Long>, Text>, Tuple2<LocalDateTime, LogLevel>> mappingFunction(final Map<String, LogfileType> pathTypeMapping) {
        return tuple -> {
            return pathTypeMapping.get(tuple._1._1.toString()).parse(tuple._2.toString());
        };
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
            directory = new Path(hdfs.getHomeDirectory(), commandLine.getOptionValue(DIRECTORY.getOpt()));
            return true;
        } catch (ParseException | IllegalArgumentException e) {
            return false;
        }
    }
}
