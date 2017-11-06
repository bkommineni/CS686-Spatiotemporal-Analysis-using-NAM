package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bharu on 11/6/17.
 */
public class ClimateChartJob {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try
        {
            conf.set("Geohash_prefix",args[2]);
            Job job = Job.getInstance(conf,"Climate chart Job");
            job.setJarByClass(ClimateChartJob.class);
            job.setMapperClass(ClimateChartMapper.class);
            job.setReducerClass(ClimateChartReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0:1);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
