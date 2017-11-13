package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bharu on 11/6/17.
 */
public class ClimateChartMapper extends Mapper<LongWritable,Text,Text,Text> {
    String geohash_prefix = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] features = value.toString().split("\t");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(features[0]));
        if(features[1].contains(geohash_prefix))
            context.write(new Text(Integer.toString(calendar.get(Calendar.MONTH)+1)),
                    new Text(features[1]+"\t"+features[40]+"\t"+features[55]));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        geohash_prefix = conf.get("Geohash_prefix");
    }
}
