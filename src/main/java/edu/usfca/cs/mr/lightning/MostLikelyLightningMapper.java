package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bharu on 11/2/17.
 */
public class MostLikelyLightningMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String geohash = tokens[1];
        String lightning = tokens[22];
        context.write(new Text(geohash),new FloatWritable(Float.parseFloat(lightning)));
    }
}
