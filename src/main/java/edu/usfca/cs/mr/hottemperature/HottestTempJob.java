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
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job = Job.getInstance(conf, "hottest temp job");
            // Current class.
            job.setJarByClass(HottestTempJob.class);
            // Mapper
            job.setMapperClass(HottestTempMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(HottestTempReducer.class);
            // Reducer
            job.setReducerClass(HottestTempReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
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
            System.exit(job.waitForCompletion(true) ? 0 : 1);

            Job job2 = Job.getInstance(conf, "hottest temp job2");
            // Current class.
            job.setJarByClass(HottestTempJob.class);
            // Mapper
            job.setMapperClass(HottestTempMapper1.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(HottestTempReducer1.class);
            // Reducer
            job.setReducerClass(HottestTempReducer1.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(INTERMEDIATE_OUTPUT));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
