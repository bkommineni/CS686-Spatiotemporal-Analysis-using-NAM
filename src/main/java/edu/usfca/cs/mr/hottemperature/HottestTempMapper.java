package edu.usfca.cs.mr.hottemperature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class HottestTempMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] features = value.toString().split("\t");
        System.out.println("tokens length "+features.length);
        context.write(new Text(features[1].substring(0,2)),new Text(features[0]+"::"+features[1]+"::"+features[40]));
    }
}
