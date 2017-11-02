package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class SnowDepthJob {
        private static final String INTERMEDIATE_OUTPUT = "/tmp/bkommineni/intermediate_output";
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
	        conf.set("mapreduce.textoutputformat.separator", "\t");

            Job job = Job.getInstance(conf, "snow depth job");
            job.setJarByClass(SnowDepthJob.class);
            job.setMapperClass(SnowDepthMapper.class);
            job.setCombinerClass(SnowDepthReducer.class);
            job.setReducerClass(SnowDepthReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE_OUTPUT));
            job.waitForCompletion(true);

            Configuration configuration = new Configuration();
            configuration.set("mapreduce.textoutputformat.separator", "\t");
	        System.out.println("Before starting job 2");
            Job job2 = Job.getInstance(configuration, "snow depth job2");
            job2.setJarByClass(SnowDepthJob.class);
            job2.setMapperClass(SnowDepthMapper1.class);
            job2.setCombinerClass(SnowDepthReducer1.class);
            job2.setReducerClass(SnowDepthReducer1.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE_OUTPUT));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            System.out.println("Before starting job 2 - before blocking method");
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
