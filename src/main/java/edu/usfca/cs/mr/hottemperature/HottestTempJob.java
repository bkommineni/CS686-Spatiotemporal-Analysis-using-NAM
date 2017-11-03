package edu.usfca.cs.mr.hottemperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class HottestTempJob {
    private static final String INTERMEDIATE_OUTPUT = "intermediate_output";
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.output.textoutputformat.separator",",");
            Job job = Job.getInstance(conf, "hottest temp job");
            job.setJarByClass(HottestTempJob.class);
            job.setMapperClass(HottestTempMapper.class);
            job.setReducerClass(HottestTempReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            Configuration config = new Configuration();
            Job job2 = Job.getInstance(config, "hottest temp job2");
            job2.setJarByClass(HottestTempJob.class);
            job2.setMapperClass(HottestTempMapper1.class);
            job2.setReducerClass(HottestTempReducer1.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
