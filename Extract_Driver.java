package big_data_final;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import javax.xml.stream.XMLInputFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author User
 */
public class Extract_Driver {

    /**
     * @param args the command line arguments
     */
    static int count = 0;

    public static void main(String[] args) {
        // TODO code application logic here

        try {

            Configuration conf = new Configuration();

            //conf.setInputFormat();
            Job job = new Job(conf, "Twitter Processing");

            job.setJarByClass(Extract_Driver.class);
            job.setMapperClass(Extract_Tweet_Map.class);
            job.setReducerClass(Extract_Tweet_Reduce.class);
            //job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //  FileInputFormat.addInputPath(job, new Path(inFiles));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //job.setReducerClass(XMLReducer.class);
            job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
            job.waitForCompletion(true);
            job.killJob();

        } catch (Exception e) {
            System.out.println(e.getMessage().toString());
        }
    }

}