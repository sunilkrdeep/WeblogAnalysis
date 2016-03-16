package dev.weblog.sk.analysis;

//public class WeblogMapper {

//}

import java.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import dev.weblog.sk.analysis.WeblogWritablem;
//import org.json.simple.JSONObject;


public class WeblogMapper extends Mapper<Text, WeblogWritablem,Text,Text> {
		
	@Override
	protected void map(Text key, WeblogWritablem value, Context context) throws IOException, InterruptedException {
		
	
	//	WeblogWritable weblog =  new WeblogWritable();
		
		String IPaddm = value.getIPadd().toString();
		String Datetimem = value.getDatetime().toString();
		String Requestm = value.getRequest().toString();
		String Refererm = value.getRefere().toString();
		String Browserm = value.getBrowser().toString();
		String Responsem = value.getResponse().toString();
		String Bytesentm = value.getBytesent().toString();
		
	//	String str1="";
		
		String str1 = IPaddm + "~" + Datetimem + "~" + Requestm + "~" + Refererm + "~" + Browserm + "~" + Responsem + "~" + Bytesentm;
		
	//	System.out.println("Concanated String" + str1);
		
	//	System.out.println("Next Record");
		
		/*System.out.println("IP Address :" + IPaddm);
		System.out.println("Date & Time  :" + Datetimem);
		System.out.println("Request  :" + Requestm);
		System.out.println("Referer :"  + Refererm);
		System.out.println("Browser :" + Browserm);
		System.out.println("Response :" + Responsem);
		System.out.println("Bytes Sent :" + Bytesentm);
		*/
		
		/*try {
		JSONObject obj = new JSONObject();

	      obj.put("IP_Add", IPaddm);
	      obj.put("DateTime", Datetimem);
	      obj.put("Request", Requestm);
	      obj.put("Referer", Refererm);
	      obj.put("Browser", Browserm);
	      obj.put("Response", Responsem);
	      obj.put("ByteSent", Bytesentm);
	      
	      System.out.print(obj);
	      
	  //     str1 = obj.toString();
	      
		} catch (JSONException e) {
			// TODO Auto-generated catch blo
			e.printStackTrace();
		}
	 
	 		*/
		
		
	//	context.write(key, new Text(value.getDataString()));
		
		System.out.println("key" + key);
		System.out.println("Concanated String" + str1);
		
		
		context.write(key,new Text(str1));
	}
}
	