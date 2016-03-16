package dev.weblog.sk.analysis;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/*
 * custom hadoop file format for Weblog files, provides a generic splitting mechanism for HDFS file
 */
public class WeblogInputFormatm extends FileInputFormat<Text, WeblogWritablem> {

  @Override
  public RecordReader<Text, WeblogWritablem> createRecordReader(
      InputSplit arg0, TaskAttemptContext arg1) throws IOException,
      InterruptedException {
    return new WeblogRecordReaderm();
  }

}
