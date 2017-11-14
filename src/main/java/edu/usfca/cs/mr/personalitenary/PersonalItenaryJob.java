package edu.usfca.cs.mr.personalitenary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bharu on 11/12/17.
 */
public class PersonalItenaryJob {
    public static void main(String[] args) {

        try
        {
            Configuration conf = new Configuration();
            conf.set("mapreduce.output.textoutputformat.separator","\t");
            Job job = Job.getInstance(conf,"Personal Itenary Job");
            job.setJarByClass(PersonalItenaryJob.class);
            job.setMapperClass(PersonalItenaryMapper.class);
            job.setReducerClass(PersonalItenaryReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            Configuration conf1 = new Configuration();
            Job job2 = Job.getInstance(conf1,"Personal Itenary Job2");
            job2.setJarByClass(PersonalItenaryJob.class);
            job2.setMapperClass(PersonalItenaryMapper1.class);
            job2.setReducerClass(PersonalItenaryReducer1.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0:1);
        }
        catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
