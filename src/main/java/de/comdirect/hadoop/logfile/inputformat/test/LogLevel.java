package de.comdirect.hadoop.logfile.inputformat.test;

import java.util.Random;

import de.comdirect.hadoop.logfile.inputformat.LogfileInputFormat;

/**
 * Logging Level for test of {@link LogfileInputFormat}
 *
 * @author comdirect
 * @since 2017
 */
public enum LogLevel {

    INFO, WARN, ERROR;

    static Random random = new Random();

    /**
     * Randomly selects a logging level.
     * 
     * The probability of {@link #INFO} and {@link #WARN} to be selected is 500 times higher than {@link #ERROR}'s.
     * 
     * @return randomly selected logging level
     */
    static LogLevel random() {
        return LogLevel.values()[random.nextInt(1001) / 500];
    }
}