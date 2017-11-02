package edu.usfca.cs.mr.hottemperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class HottestTempMapper1 extends Mapper<Text,Text,Text,Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text("record"),value);
    }
}
