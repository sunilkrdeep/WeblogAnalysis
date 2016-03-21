package dev.weblog.sk.analysis;

//public class WeblogRecordReader {

//}

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
    String UrlPattern = "(http|https)://(.*?)[/\\)\\?\\s$]";
    String searchPattern = "(.*?)q=(.*?)(?:&(.*?)|$)";
    String datePattern = "^(.*?):(.*?)";
    
    Pattern webLogPattern = Pattern.compile(LogPattern);

    Pattern webUrlPattern = Pattern.compile(UrlPattern);
    
    Pattern webSearchPattern = Pattern.compile(searchPattern);
    
    Pattern webDatePattern = Pattern.compile(datePattern);
    
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
    

    /*System.out.println("IP Address: " + matcher.group(1));
    System.out.println("Date&Time: " + matcher.group(4));
    System.out.println("Request: " + matcher.group(5));
    System.out.println("Response: " + matcher.group(6));
    System.out.println("Bytes Sent: " + matcher.group(7));
    if (!matcher.group(8).equals("-"))
       System.out.println("Referer: " + matcher.group(8));
    System.out.println("Browser: " + matcher.group(9));*/
    
   String Datestr ="";
   String Referer = "";
    String IPadd = matcher.group(1);
    
    String Datetime = matcher.group(4);   
    
   // System.out.println("Datetime :" + Datestr);
    
    Matcher dateMatcher = webDatePattern.matcher(Datetime);
    
    if(dateMatcher.find()){
    	
     // Datestr = dateMatcher.group(1);
    
     SimpleDateFormat simpleDF = new SimpleDateFormat ("dd/MMM/yyyy");  //07/Aug/2009
	 SimpleDateFormat newSimpleDF = new SimpleDateFormat ("yyyy-MM-dd"); //2009-Aug-07
	 
	 try {
			Date Datestr1 = simpleDF.parse(dateMatcher.group(1).toString());
			
			Datestr= newSimpleDF.format(Datestr1).toString();
			
		//	newSimpleDF.format(Datestr1).toString())
			
		//	 System.out.println("Date :" + Datestr);
			 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     // System.out.println("Date  : " + Date);
    }
    
    String Request = matcher.group(5);
    int Response   = Integer.parseInt( matcher.group(6));
    String responseByte = matcher.group(7);
    
    int Bytesent;
    if(responseByte == null){
    	Bytesent   = 0;
    } else {
    	Bytesent   = Integer.parseInt(responseByte);	
    }
    
    String Urlstr="";
    String Searchkey="";
    
    String Browser = matcher.group(9);
     
    
   
  if (!matcher.group(8).equals("-")){
	  Referer =  matcher.group(8);
	  
	  Matcher urlMatcher = webUrlPattern.matcher(Referer);
	  Matcher searchMatcher = webSearchPattern.matcher(Referer);
	  
	  if(urlMatcher.find()){
	 // 	System.out.println("Referer Url :" +urlMatcher.group(2));	
	  	Urlstr = urlMatcher.group(2);
	  }
	  if(searchMatcher.find()){
	      	Searchkey = searchMatcher.group(2);
 	     	System.out.println("Search Key Word from Referer : " + Searchkey);
	    }
	  
  } 
  else {
	  
	  Matcher browserMatcher = webUrlPattern.matcher(Browser);
	  
	   if (browserMatcher.find()){
		//   System.out.println("Browser Url :" + browserMatcher.group(2));
		
		   Urlstr = browserMatcher.group(2);
		   
		   System.out.println("URL from Browser :" + Urlstr);
	   }
	  
	  
  }
	  
  
  /*System.out.println("IP Address: " + IPadd);
  System.out.println("Date&Time: " + Datetime);
  System.out.println("Date :" + Datestr);
  System.out.println("Request: " + Request);
  if (!matcher.group(8).equals("-"))
	     System.out.println("Referer: " + Referer);
  
  System.out.println("Browser: " + Browser);
  System.out.println("Urlstr: " + Urlstr);
  System.out.println("Search Key : " + Searchkey);
  System.out.println("Response: " + Response);
  System.out.println("Bytes Sent: " + Bytesent);
  */
  
    
    key = new Text(IPadd);
    value = new WeblogWritablem();
    value.set(IPadd, Datetime, Datestr, Request, Referer, Browser, Urlstr, Searchkey, Response, Bytesent);
           
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
