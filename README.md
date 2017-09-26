# hadoop-logfile-inputformat
Hadoop input format for (possibly gzipped) logfiles with multiline log statements.

## Process / Motivation

### Logfiles with multi-line log records
If you have logfiles where single log records may consist of multiple lines, the built-in input formats of Hadoop cannot help you to process these files as they read line by line. Due to parallelization you will most probably not end up with complete log records. What you need is this:

![the process](https://raw.githubusercontent.com/comdirect/hadoop-logfile-inputformat/documentation/img/LogfileInputFormat_Process.png)


