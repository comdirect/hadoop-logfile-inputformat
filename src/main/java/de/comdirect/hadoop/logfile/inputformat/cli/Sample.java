package de.comdirect.hadoop.logfile.inputformat.cli;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import de.comdirect.hadoop.logfile.inputformat.LogfileInputFormat;
import scala.Tuple2;

/**
 * Analyzes logfiles from HDFS and writes sample lines of the analyzed data back to HDFS.
 * 
 * This class serves as an example for the usage of the {@link LogfileInputFormat}.
 * 
 * @author Nikolaus Winter, comdirect bank AG
 * @since 2017
 */
public class Sample {

    private static Options OPTIONS = new Options();

    private static Option INPUT_PATH;
    private static Option OUTPUT_PATH;
    private static Option PATTERN;
    private static Option SAMPLE_FRACTION;

    private static String inputPath;
    private static Path outputPath;
    private static Pattern pattern;
    private static Double sampleFraction = 0.01;

    static {
        INPUT_PATH = new Option("i", "inputPath", true, "HDFS input path(s)");
        INPUT_PATH.setRequired(true);
        OPTIONS.addOption(INPUT_PATH);

        OUTPUT_PATH = new Option("o", "outputPath", true, "HDFS output path(s)");
        OUTPUT_PATH.setRequired(true);
        OPTIONS.addOption(OUTPUT_PATH);

        PATTERN = new Option("p", "pattern", true, "regex pattern that defines the first line of a log record");
        PATTERN.setRequired(true);
        OPTIONS.addOption(PATTERN);

        SAMPLE_FRACTION = new Option("s", "sampleFraction", true,
                "expected size of the sample as a fraction of total # of records (defaults to 0.01 = 1 percent) [0.0 .. 1.0]");
        SAMPLE_FRACTION.setRequired(false);
        OPTIONS.addOption(SAMPLE_FRACTION);
    }

    public static void main(String[] args) throws IOException {
        boolean argumentsValid = parseArguments(args);

        if (!argumentsValid) {
            printHelp();
            System.exit(1);
        }

        final Configuration hadoopConfig = new Configuration(true);
        final FileSystem hdfs = FileSystem.get(hadoopConfig);

        if (hdfs.exists(outputPath)) {
            System.out.printf("output path '%s' already exists in HDFS!%n", outputPath);
            System.exit(1);
        }

        System.out.printf("reading from:     %s%n", inputPath);
        System.out.printf("writing to:       %s%n", outputPath);
        System.out.printf("pattern:          %s%n", pattern.pattern());
        System.out.printf("sample fraction:  %f%n", sampleFraction);
        System.out.printf("...%n");
        System.out.printf("%n");

        SparkConf sparkConfig = new SparkConf().setAppName(String.format("Reading sample (fraction %f) from '%s'", sampleFraction, inputPath));
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);

        hadoopConfig.set(LogfileInputFormat.KEY_FIRSTLINE_PATTERN, pattern.pattern());

        JavaPairRDD<Tuple2<Path, Long>, Text> rdd = sparkContext.newAPIHadoopFile(inputPath.toString(), LogfileInputFormat.class, LogfileInputFormat.KEY_CLASS,
                Text.class,
                hadoopConfig);

        rdd.sample(false, sampleFraction)
                .map(tuple -> String.format("%s@%016d:%n%n%s%n%n", tuple._1._1.toString(), tuple._1._2, tuple._2.toString()))
                .repartition(1)
                .saveAsTextFile(outputPath.toString());

        sparkContext.close();
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(150);
        formatter.printHelp(Sample.class.getName(), OPTIONS);
    }

    private static boolean parseArguments(String[] args) {
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine commandLine = parser.parse(OPTIONS, args);

            inputPath = commandLine.getOptionValue(INPUT_PATH.getOpt());
            outputPath = new Path(commandLine.getOptionValue(OUTPUT_PATH.getOpt()));
            pattern = Pattern.compile(commandLine.getOptionValue(PATTERN.getOpt()));
            if (commandLine.hasOption(SAMPLE_FRACTION.getOpt())) {
                sampleFraction = Double.valueOf(commandLine.getOptionValue(SAMPLE_FRACTION.getOpt()));
            }

            if (sampleFraction < 0 || sampleFraction > 1) {
                return false;
            }

            return true;
        } catch (ParseException | IllegalArgumentException e) {
            return false;
        }
    }

}
