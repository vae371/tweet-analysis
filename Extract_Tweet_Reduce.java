package big_data_final;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Extract_Tweet_Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String myValue = "";
        for (Text value : values) {
            myValue = value.toString();

            if (myValue.contains("Hash")) {

                String hashTagSeparator[] = myValue.split("Hash");

                String cleartext = hashTagSeparator[0].replaceAll("http.*?\\s", " ");

                //Do Sentiment analysis and Topic Modelling on the clearText String
                context.write(new Text(cleartext), new Text(hashTagSeparator[1]));
                // context.write(key, value);
            }
        }
    }
}