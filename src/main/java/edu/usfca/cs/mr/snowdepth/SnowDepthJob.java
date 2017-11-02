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
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job = Job.getInstance(conf, "snow depth job");
            // Current class.
            job.setJarByClass(SnowDepthJob.class);
            // Mapper
            job.setMapperClass(SnowDepthMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            //job.setCombinerClass(SnowDepthReducer.class);
            // Reducer
            job.setReducerClass(SnowDepthReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FloatWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE_OUTPUT));
            // Block until the job is completed.
            job.waitForCompletion(true);

            Configuration configuration = new Configuration();
            configuration.set("mapred.textoutputformat.separator", "\t");
	    System.out.println("Before starting job 2");
            Job job2 = Job.getInstance(configuration, "snow depth job2");
            // Current class.
            job2.setJarByClass(SnowDepthJob.class);
            // Mapper
            job2.setMapperClass(SnowDepthMapper1.class);
            // Combiner. We use the reducer as the combiner in this case.
            //job2.setCombinerClass(SnowDepthReducer1.class);
            // Reducer
            job2.setReducerClass(SnowDepthReducer1.class);
            // Outputs from the Mapper.
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(FloatWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE_OUTPUT));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            // Block until the job is completed.
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
