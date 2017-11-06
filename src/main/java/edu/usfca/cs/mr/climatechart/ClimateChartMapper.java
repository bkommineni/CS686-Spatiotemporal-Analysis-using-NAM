package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bharu on 11/6/17.
 */
public class ClimateChartMapper extends Mapper<LongWritable,Text,Text,Text> {
    String filename = null;
    String geohash_prefix = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = filename.split(".");
        String year_month = tokens[0];
        Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])([0-9][0-9])");
        Matcher matcher = pattern.matcher(year_month);
        String year = matcher.group(1);
        String month =  matcher.group(2);
        String[] features = value.toString().split("\t");
        if(features[1].contains(geohash_prefix))
            context.write(new Text(month),new Text(features[1]+"\t"+features[40]+"\t"+features[55]));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
        filename = context.getConfiguration().get(fsFileSplit.getPath().getParent().getName());
        Configuration conf = context.getConfiguration();
        geohash_prefix = conf.get("Geohash_prefix");
    }
}
