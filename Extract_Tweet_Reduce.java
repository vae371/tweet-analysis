package big_data_final;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Extract_Tweet_Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String myValue = "";
        for (Text value : values) {
            myValue = value.toString();

             {

                //String hashTagSeparator[] = myValue.split("Hash");

                //String cleartext = myValue.replaceAll("http.*?\\s", "");

                //Do Sentiment analysis and Topic Modelling on the clearText String
                context.write(new Text(myValue), new Text());
                // context.write(key, value);
            }
        }
    }
}