package edu.usfca.cs.mr.solarandwindfarms;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bharu on 11/7/17.
 */
public class SolarAndWindFarmsMapper1 extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String geohash = tokens[0];
        String avg_wind_speed = tokens[1];
        String avg_cloud_cover = tokens[2];
        String avg_temperature = tokens[3];
        context.write(new Text("record"),new Text(geohash+"\t"+avg_wind_speed+"\t"+avg_cloud_cover+"\t"+
                                                        avg_temperature));
    }
}
