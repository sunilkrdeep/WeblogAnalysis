package dev.weblog.sk.analysis;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * custom hadoop key format for weblog events
 */

public class WeblogWritablem implements Writable {

  private Text IPadd, Datetime, Datestr, Request, Referer, Browser, Urlstr, Searchkey;
  private IntWritable Response, Bytesent;

  public WeblogWritablem() {
    this.IPadd = new Text();
    this.Datetime =  new Text();
    this.Datestr = new Text();
    this.Request = new Text();
    this.Referer = new Text();
    this.Browser = new Text();
    this.Urlstr = new Text();
    this.Searchkey = new Text();
    this.Response = new IntWritable();
    this.Bytesent = new IntWritable();
  }

  public void set (String IPadd, String Datetime, String Datestr, String Request, String Referer, String Browser, String UrlStr, String Searchkey, int Response, int Bytesent)
 
  {
    this.IPadd.set(IPadd);
    this.Datetime.set(Datetime);
    this.Datestr.set(Datestr);
    this.Request.set(Request);
    this.Referer.set(Referer);
    this.Browser.set(Browser);
    this.Urlstr.set(Urlstr);
    this.Searchkey.set(Searchkey);
    this.Response.set(Response);
    this.Bytesent.set(Bytesent);
  }

  /*
   * de-serialize the input data and populate the fields of the Writable object
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    IPadd.readFields(in);
    Datetime.readFields(in);
    Datestr.readFields(in);
    Request.readFields(in);
    Referer.readFields(in);
    Browser.readFields(in);
    Urlstr.readFields(in);
    Searchkey.readFields(in);
    Response.readFields(in);
    Bytesent.readFields(in);
  }

  /*
   * write the fields of the Writable object to the underlying stream
   */
  @Override
  public void write(DataOutput out) throws IOException {
    IPadd.write(out);
    Datetime.write(out);
    Datestr.write(out);
    Request.write(out);
    Referer.write(out);
    Browser.write(out);
    Urlstr.write(out);
    Searchkey.write(out);
    Response.write(out);
    Bytesent.write(out);
  }

  
  public int hashCode()
  {
    return Response.hashCode();
  }

   // * Getters & Setters
   

  public Text getIPadd() {
    return IPadd;
  }

  public Text getDatetime(){
  return Datetime;
}
 
  public Text getDatestr(){
	  return Datestr;
  }

  public Text getRequest() {
    return Request;
  }

  public Text getRefere() {
    return Referer;
  }
  
  public Text getBrowser() {
	    return  Browser;
	  }
  public Text getUrlstr() {
	    return Urlstr;
	  }
  
  public Text getSearchkey(){
	  return Searchkey;
  }

  
  public IntWritable getResponse() {
	    return  Response;
	  }

   public IntWritable getBytesent() {
    return Bytesent;
  }
   
   /*public String getDataString() {
	   String str = IPadd + "," + Datetime + "," + Request + "," + Referer + "," + Browser + "," + Response + "," + Bytesent;
	   return str;
   }
*/

	

}
