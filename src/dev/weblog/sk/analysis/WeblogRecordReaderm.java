package dev.weblog.sk.analysis;

//public class WeblogRecordReader {

//}

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * custom record reader for Weblog events, using LineRecordReader as base
 */
public class WeblogRecordReaderm extends RecordReader<Text, WeblogWritablem> {
  LineRecordReader lineReader;
  WeblogWritablem value;
  Text key;

  /*
   * read lines of text
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext attempt)
      throws IOException, InterruptedException {
    lineReader = new LineRecordReader();
    lineReader.initialize(inputSplit, attempt);

  }

  /*
   * custom parsing of the log entries of the input data
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!lineReader.nextKeyValue())
    {
      return false;
    }

    //Pattern httpLogPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[(.*)\\] \"([^\\s]+) (/[^\\s]*) HTTP/[^\\s]+\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"$");
       // Pattern webLogPattern = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|.) \"([^\"]*)\" \"([^\"]*)\"");
    
    String LogPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3})[\\s?](?:(\\d+)|\\S) \"([^\"]+)\" \"([^\"]+)\"";
    
    Pattern webLogPattern = Pattern.compile(LogPattern);

    Matcher matcher = webLogPattern.matcher(lineReader.getCurrentValue().toString());
    
    if (!matcher.matches()) {
      System.out.println("Bad Record:"+ lineReader.getCurrentValue());
      return nextKeyValue();
    }
    
    /*System.out.println("group(1)  : " + matcher.group(1));
    System.out.println("group(2)  : " + matcher.group(2));
    System.out.println("group(3)  : " + matcher.group(3));
    System.out.println("group(4)  : " + matcher.group(4));
    System.out.println("group(5)  : " + matcher.group(5));
    System.out.println("group(6)  : " + matcher.group(6));
    System.out.println("group(7)  : " + matcher.group(7));
    System.out.println("group(8)  : " + matcher.group(8));
    System.out.println("group(9)  : " + matcher.group(9));*/
    
/*
    System.out.println("IP Address: " + matcher.group(1));
    System.out.println("Date&Time: " + matcher.group(4));
    System.out.println("Request: " + matcher.group(5));
    System.out.println("Response: " + matcher.group(6));
    System.out.println("Bytes Sent: " + matcher.group(7));
   // if (!matcher.group(8).equals("-"))
    System.out.println("Referer: " + matcher.group(8));
    System.out.println("Browser: " + matcher.group(9));*/
    
    
   String Referer = "";
    String IPadd = matcher.group(1);
    String Datetime = matcher.group(4);
    String Request = matcher.group(5);
    int Response   = Integer.parseInt( matcher.group(6));
    int Bytesent   = Integer.parseInt( matcher.group(7));
   
 //   if (!matcher.group(8).equals("-"))
    Referer =  matcher.group(8);
    
    String Browser = matcher.group(9);
   
 
    
    key = new Text(IPadd);
    value = new WeblogWritablem();
    value.set(IPadd, Datetime, Request, Referer, Browser,Response, Bytesent);
    
    //System.out.println("Key  :" + key);
   // System.out.println("Value  :" + value);
    
    return true;
    
    
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public WeblogWritablem getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }
}
