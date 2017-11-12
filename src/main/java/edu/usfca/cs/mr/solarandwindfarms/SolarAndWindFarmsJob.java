package edu.usfca.cs.mr.solarandwindfarms;

import edu.usfca.cs.mr.climatechart.ClimateChartJob;
import edu.usfca.cs.mr.climatechart.ClimateChartMapper;
import edu.usfca.cs.mr.climatechart.ClimateChartReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bharu on 11/7/17.
 */
public class SolarAndWindFarmsJob {
    public static void main(String[] args) {

        try
        {
            Configuration conf = new Configuration();
            conf.set("mapreduce.output.textoutputformat.separator",",");
            Job job = Job.getInstance(conf,"Solar and Wind Farms Job1");
            job.setJarByClass(SolarAndWindFarmsJob.class);
            job.setMapperClass(SolarAndWindFarmMapper.class);
            job.setReducerClass(SolarAndWindFarmReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            conf = new Configuration();
            conf.set("mapreduce.output.textoutputformat.separator","\t");
            Job job2 = Job.getInstance(conf,"Solar and Wind Farms Job2");
            job2.setJarByClass(SolarAndWindFarmsJob.class);
            job2.setMapperClass(SolarAndWindFarmsMapper1.class);
            job2.setReducerClass(SolarAndWindFarmsReducer1.class);
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
