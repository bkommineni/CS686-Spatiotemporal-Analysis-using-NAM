package edu.usfca.cs.mr.hottemperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class HottestTempMapper extends Mapper<Text,Text,Text,Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] features = value.toString().split("\t");
        context.write(new Text(features[1].substring(0,2)),new Text(features[0]+"\t"+features[1]+"\t"+features[40]));
    }
}
