package big_data_final;

import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Extract_Tweet_Map extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) {

		String myKey = "";
		String myValue = "";

		String val = value.toString();
		String[] ss = val.split("\t");

		if (val.contains("\"EN\"") && ss[3].contains("/status/")) {

			String s = ss[3].substring(1, ss[3].length() - 1);
			myKey = s;
			Document doc = null;
			try {
				doc = (Document) Jsoup.connect(s).get();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				// System.out.println(s + "!!!");
				// e.printStackTrace();
			}

			try {
				Elements table = doc.getElementsByClass("TweetTextSize TweetTextSize--26px js-tweet-text tweet-text");
				Elements rows = table.tagName("p");
				for (Element row : rows) {
					Elements tds = row.getElementsByTag("p");
					for (Element r : tds) {
						myValue += r.text();						
					}
				}
			} catch (Exception e) {				
			}

			try {
				if (!myValue.contains("Tweet Deleted") && myValue != "") {
					while(myValue.contains("http")){
						int i=myValue.indexOf("http");
						if(i!=-1){
							int j=myValue.indexOf(" ", i);
							if(j!=-1){
								myValue=myValue.substring(0,i)+myValue.substring(j);
							}else{
								int k=myValue.indexOf("#", i);
								if(k!=-1){
									myValue=myValue.substring(0,i)+myValue.substring(k);
								}
								myValue=myValue.substring(0,i);
							}						
						}	
					}					
					
					context.write(new Text(myValue.trim()), new Text());	
				}
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

  
}
