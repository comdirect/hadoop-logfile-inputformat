package de.comdirect.hadoop.logfile.inputformat;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Hadoop input format to read logfiles with log records that may consist of multiple lines.
 * 
 * The <b>key</b> of each log record is a {@link Tuple2} consisting of the HDFS {@link Path} and the position at which
 * the log record starts.
 * 
 * The <b>value</b> is a {@link Text} containing the actual log record which can consist of multiple lines.
 * 
 * To determine the first line of a log record, a regex ({@link Pattern}) must be provided under the key
 * {@link #KEY_FIRSTLINE_PATTERN} of the Hadoop {@link Configuration}.
 * 
 * To use this input format in Apache Spark, please use
 * {@link JavaSparkContext#newAPIHadoopFile(String, Class, Class, Class, Configuration)}. In addition to path (1st
 * param) and Hadoop {@link Configuration} (5th param), you have to provide three class objects to tell Spark which
 * inputformat to use and which are the classes for key and value.
 * 
 * Use <code>LogfileInputFormat.class</code> as the format (2nd param) and <code>Text.class</code> as the value (4th
 * param). As the key format is a {@link Tuple2} (parameterized type defined in Scala), it is not that easy to provide
 * the class. As a convenient option, please use {@link #KEY_CLASS} as 3rd param.
 * 
 * @author Nikolaus Winter, comdirect bank AG
 * @since 2017
 */
public class LogfileInputFormat extends FileInputFormat<Tuple2<Path, Long>, Text> {

    private Logger LOG = LogManager.getLogger(LogfileInputFormat.class);

    private static final String FIRSTLINE_PATTERN_DEFAULT = "de.comdirect.hadoop.logfile.inputformat.PatternResolver_FIRSTLINE_PATTERN_DEFAULT";
    private static final String FIRSTLINE_PATTERN_HDFS_PATH = "de.comdirect.hadoop.logfile.inputformat.PatternResolver_FIRSTLINE_PATTERN_HDFS_PATH_%s";

    /**
     * Defines a type class for the key of the produced pairs.
     */
    @SuppressWarnings("unchecked")
    public static Class<Tuple2<Path, Long>> KEY_CLASS = (Class<Tuple2<Path, Long>>) ((Class<?>) Tuple2.class);

    /**
     * Sets the general pattern to recognize the first line of a log record.
     * 
     * This pattern is used if no special pattern is defined on a per-file basis (see
     * {@link #setPattern(Configuration, Path, Pattern)}.
     * 
     * @param hadoopConfig
     *            Hadoop configuration into which the pattern should be set.
     * @param pattern
     *            pattern to recognize the first line of a log record
     */
    public static void setPattern(Configuration hadoopConfig, Pattern pattern) {

        hadoopConfig.setPattern(FIRSTLINE_PATTERN_DEFAULT, pattern);
    }

    /**
     * Sets the pattern to recognize the first line of a log record on a per file basis.
     * 
     * @param hadoopConfig
     *            Hadoop configuration into which the pattern should be set.
     * @param path
     *            HDFS path for which this pattern should be used
     * @param pattern
     *            pattern to recognize the first line of a log record
     */
    public static void setPattern(Configuration hadoopConfig, String path, Pattern pattern) {

        hadoopConfig.setPattern(String.format(FIRSTLINE_PATTERN_HDFS_PATH, path), pattern);
    }

    /**
     * Determines the pattern that should be used for the given HDFS file.
     * 
     * @param hadoopConfig
     *            Hadoop configuration from which the pattern should be retrieved.
     * @param path
     *            HDFS file
     */
    static Pattern getPattern(Configuration hadoopConfig, String path) {

        return hadoopConfig.getPattern(String.format(FIRSTLINE_PATTERN_HDFS_PATH, path), hadoopConfig.getPattern(FIRSTLINE_PATTERN_DEFAULT, null));
    }

    @Override
    public RecordReader<Tuple2<Path, Long>, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("create LogfileRecordReader");
        }
        return new LogfileRecordReader();
    }

    // copied from org.apache.hadoop.mapreduce.lib.input.TextInputFormat.isSplitable(JobContext, Path)
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }
}
