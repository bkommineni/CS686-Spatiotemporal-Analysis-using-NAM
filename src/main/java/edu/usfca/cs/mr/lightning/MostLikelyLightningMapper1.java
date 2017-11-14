package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bharu on 11/2/17.
 */
public class MostLikelyLightningMapper1 extends Mapper<LongWritable,Text,Text,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String geohash = tokens[0];
        String num_of_times_stuck_lightning = tokens[1];
        System.out.println("lightning mapper key: "+geohash.substring(0,4)+" " +"value: "+num_of_times_stuck_lightning);
        context.write(new Text(geohash.substring(0,4)),new Text(geohash+","+num_of_times_stuck_lightning));
    }
}
