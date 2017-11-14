package edu.usfca.cs.mr.lightning;

import edu.usfca.cs.mr.hottemperature.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bharu on 11/2/17.
 */
public class MostLikelyLightningJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.output.textoutputformat.separator","\t");
            Job job = Job.getInstance(conf, "most likely lightning job");
            job.setJarByClass(MostLikelyLightningJob.class);
            job.setMapperClass(MostLikelyLightningMapper.class);
            job.setReducerClass(MostLikelyLightningReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FloatWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            Configuration config = new Configuration();
            config.set("mapreduce.output.textoutputformat.separator","\t");
            Job job2 = Job.getInstance(config, "most likely lightning job2");
            job2.setJarByClass(MostLikelyLightningJob.class);
            job2.setMapperClass(MostLikelyLightningMapper1.class);
            job2.setReducerClass(MostLikelyLightningReducer1.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            job2.waitForCompletion(true);


            Configuration config1 = new Configuration();
            Job job3 = Job.getInstance(config1, "most likely lightning job3");
            job3.setJarByClass(MostLikelyLightningJob.class);
            job3.setMapperClass(MostLikelyLightningMapper2.class);
            job3.setReducerClass(MostLikelyLightningReducer2.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job3, new Path(args[2]));
            FileOutputFormat.setOutputPath(job3, new Path(args[3]));
            System.exit(job3.waitForCompletion(true) ? 0:1);

        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
