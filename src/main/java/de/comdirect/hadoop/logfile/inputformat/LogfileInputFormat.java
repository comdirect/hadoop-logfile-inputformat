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

    /**
     * Defines a type class for the key of the produced pairs.
     */
    @SuppressWarnings("unchecked")
    public static Class<Tuple2<Path, Long>> KEY_CLASS = (Class<Tuple2<Path, Long>>) ((Class<?>) Tuple2.class);

    /**
     * Key under which the regex for the first line of a log record has to be stored in the Hadoop configuration.
     * 
     * @see {@link Configuration}
     * @see {@link Configuration#set(String, String)}
     */
    public static final String KEY_FIRSTLINE_PATTERN = "de.comdirect.hadoop.logfile.inputformat.firstline_pattern";

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
