package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by bharu on 11/1/17.
 */
public class SnowDepthMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        String gehoash = tokens[0];
        String avg_snow_depth = tokens[1];
        System.out.println("key: "+gehoash.substring(0,2) +" value: "+ gehoash+","+avg_snow_depth);
        context.write(new Text(gehoash.substring(0,2)),new Text(gehoash+","+avg_snow_depth));
    }
}
