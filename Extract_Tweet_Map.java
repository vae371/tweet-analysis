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

		if (val.contains("\"EN\"")&&ss[3].contains("/status/")) {

			String s = ss[3].substring(1, ss[3].length() - 1);
			myKey=s;
			Document doc = null;
			try {
				doc = (Document) Jsoup.connect(s).get();
			} catch (Exception e) {
				// TODO Auto-generated catch block
//				System.out.println(s + "!!!");
//				e.printStackTrace();
			}

			try {
				Elements table = doc.getElementsByClass("TweetTextSize TweetTextSize--26px js-tweet-text tweet-text");
				Elements rows = table.tagName("p");
				for (Element row : rows) {
					Elements tds = row.getElementsByTag("p");
					for (Element r : tds) {
						myValue = r.text();
						System.out.println(r.text());
						if (r.text().contains("#")) {
							Pattern MY_PATTERN = Pattern.compile("#(\\S+)");
							Matcher mat = MY_PATTERN.matcher(r.text());
							myValue += "\t Hash";
							while (mat.find()) {
								// System.out.println(mat.group(1));
								myValue += mat.group();
								// hashTags.add(mat.group(1));
							}
						}
					}
				}
			} catch (Exception e) {
				myValue = "Tweet Deleted";
				System.out.println("Tweet Deleted");
			}

			try {
				context.write(new Text(myKey), new Text(myValue));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
