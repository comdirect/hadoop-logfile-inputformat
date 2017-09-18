package de.comdirect.hadoop.logfile.inputformat;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import scala.Tuple2;

/**
 * Hadoop record reader to read logfiles with log records that may consist of multiple lines.
 * 
 * As this reader is used via its corresponding input format, details are described in the {@link LogfileInputFormat}.
 * 
 * As of now, this reader only supports either compressed or splittable files, not (yet) the combination of both.
 * 
 * @author Nikolaus Winter, comdirect bank AG
 * @since 2017
 */
public class LogfileRecordReader extends RecordReader<Tuple2<Path, Long>, Text> {

    private Logger LOG = LogManager.getLogger(LogfileRecordReader.class);

    /**
     * Start position of current split.
     * 
     * For compressed files: As of now always 0, as there is only one split.
     * 
     * For uncompressed files: start position [chars]
     */
    private long start;

    /**
     * End position of current split [bytes].
     * 
     * For compressed files: filesize in [bytes], as there is only one split.
     * 
     * For uncompressed files: start position of next split [chars]
     */
    private long end;

    /**
     * Current reading position [chars].
     * 
     * This value always represents the reading position measured by char count, regardless of the actual reading
     * position in the file. The latter might differ from the character count due to compression.
     */
    private long pos;

    /**
     * Starting position of last line that was read [chars].
     * 
     * @see #line
     * @see #lineReader
     */
    private long lastReadPos;

    /**
     * Has end of file been reached?
     */
    private boolean eof = false;

    /**
     * Has end of split been reached?
     */
    private boolean eos = false;

    /**
     * Previously fetched line.
     * 
     * @see #fetchLine()
     */
    private String line;

    /**
     * Reads HDFS file line by line.
     */
    private LineReader lineReader;

    /**
     * Key of current pair.
     * 
     * @see #getCurrentKey()
     */
    private Tuple2<Path, Long> key;

    /**
     * Value of current pair.
     * 
     * @see #getCurrentValue()
     */
    private Text value;

    /**
     * Pattern to detect first line of a log record.
     * 
     * Must be provided as a regex String in Hadoop {@link Configuration} under the key
     * {@link LogfileInputFormat#KEY_FIRSTLINE_PATTERN}.
     */
    private Pattern firstlinePattern;

    /**
     * HDFS Path of current file/split.
     */
    private Path hdfsPath;

    /**
     * Is the current input file a compressed file?
     * 
     * As of now, this is mutually exclusive to {@link #isSplittable}.
     */
    private boolean isCompressed;

    /**
     * Is the current input file a splittable file?
     * 
     * As of now, this is mutually exclusive to {@link #isCompressed}.
     */
    private boolean isSplittable;

    /**
     * File input stream.
     */
    private FSDataInputStream inputStream;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        Configuration jobConfiguration = context.getConfiguration();

        try {
            firstlinePattern = Pattern.compile(jobConfiguration.get(LogfileInputFormat.KEY_FIRSTLINE_PATTERN));
        } catch (NullPointerException | PatternSyntaxException e) {
            String message = String.format("The regex to detect the first line of a log record must be supplied under key '%s'.",
                    LogfileInputFormat.KEY_FIRSTLINE_PATTERN);
            LOG.error(message, e);
            throw new IOException(message, e);
        }

        final FileSplit fileSplit = (FileSplit) split;
        hdfsPath = fileSplit.getPath();

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        pos = start;

        CompressionCodec codec = new CompressionCodecFactory(jobConfiguration).getCodec(hdfsPath);
        LOG.info(String.format("compression codec for input file '%s' is '%s'", key, codec));

        if (codec instanceof SplittableCompressionCodec) {
            throw new RuntimeException("The LogfileInputFormat does not (yet?) support splittable compressed input.");
        }

        final FileSystem fileSystem = hdfsPath.getFileSystem(jobConfiguration);
        inputStream = fileSystem.open(fileSplit.getPath());

        if (null != codec) {
            isCompressed = true;
            lineReader = new LineReader(codec.createInputStream(inputStream, CodecPool.getDecompressor(codec)), jobConfiguration);
        } else {
            isSplittable = true;
            lineReader = new LineReader(inputStream, jobConfiguration);
        }

        jumpToStart();
    }

    /**
     * Jumps to the start of the first line that has to be written.
     * 
     * If the current split does not start at 0, it has to be split # 2..n of a splittable source.
     * 
     * In this case, chances are high that the start position is not the start of a line.
     * 
     * To assure that this reader reads whole lines, the (remainder of the) first line is read and then thrown away.
     * 
     * In the unlikely case that the start actually *is* the start of a line, we might throw away one line too many. To
     * prevent that, we decrement the pointer by 1 before reading the line.
     * 
     * The first line (which is not completely inside this split) is taken care of by the split n-1, which reads beyond
     * its upper end if a line continues in the next split.
     * 
     * Otherwise, there is no need to jump at all.
     * 
     * @throws IOException
     */
    private void jumpToStart() throws IOException {
        if (start == 0) {
            return;
        }
        inputStream.seek(--pos);
        pos += lineReader.readLine(new Text());
    }

    /**
     * Gets the current reading position.
     * 
     * This position is usually smaller than {@link #pos} if the file is compressed.
     * 
     * @return current reading position.
     * @throws IOException
     */
    private long posInFile() throws IOException {
        if (isCompressed && null != inputStream) {
            return inputStream.getPos();
        }
        return pos;
    }

    /**
     * Fetches next line from file.
     * 
     * The line is stored in {@link #line} for further processing. The fields {@link #pos} and {@link #lastReadPos} are
     * updated accordingly.
     * 
     * If the end of the file has been reached, {@link #line} is set to <code>null</code> and the flag {@link eof} is
     * set to {@code true}.
     * 
     * @throws IOException
     */
    private void fetchLine() throws IOException {

        if (isSplittable && posInFile() >= end) {
            eos = true;
        }
        Text text = new Text();
        int length = lineReader.readLine(text);
        if (length == 0) {
            eof = true;
            line = null;
            return;
        }
        lastReadPos = pos;
        pos += length;
        line = text.toString();
    }

    /**
     * Sets the current pair ({@link #key} and {@link #value}) to {@code null} and returns {@code false}.
     * 
     * This method is used by {@link #nextKeyValue()} to signal that no more pairs are found.
     * 
     * @return always {@code false}
     */
    private boolean noMorePairs() {
        hdfsPath = null;
        key = null;
        value = null;
        return false;
    }

    /**
     * Is the given line a 'first line' of a record?
     * 
     * @param line
     *            line to test
     * @return Is the given line a 'first line' of a record?
     */
    private boolean isFirstLine(String line) {
        return line != null && firstlinePattern.matcher(line).matches();
    }

    /**
     * Finds the first line that matches the {@link #firstlinePattern}.
     * 
     * All skipped lines that do not match the given pattern are thrown away. They are taken care of by the reader of
     * the previous split.
     * 
     * @return Has a 'first line' been found?
     * @throws IOException
     */
    private boolean findFirstFirstLine() throws IOException {
        fetchLine();
        while (!isFirstLine(line) && posInFile() <= end && !eof) {
            fetchLine();
        }
        return isFirstLine(line);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (eof || eos) {
            return noMorePairs();
        }

        if (line == null && !findFirstFirstLine()) {
            return noMorePairs();
        }

        key = new Tuple2<>(hdfsPath, Long.valueOf(lastReadPos));

        Text text = new Text(line);

        fetchLine();

        while (!isFirstLine(line) && !eof) {
            byte[] bytes = (System.lineSeparator() + line).getBytes();
            text.append(bytes, 0, bytes.length);
            fetchLine();
        }

        value = new Text(text.toString());

        return true;
    }

    @Override
    public Tuple2<Path, Long> getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        }
        return Math.min(1.0f, (posInFile() - start) / (float) (end - start));
    }

    @Override
    public void close() throws IOException {
        if (lineReader != null) {
            lineReader.close();
        }
    }

}
