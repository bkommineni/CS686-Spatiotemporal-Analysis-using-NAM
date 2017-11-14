package edu.usfca.cs.mr.personalitenary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bharu on 11/13/17.
 */
public class PersonalItenaryMapper1 extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String state_month = tokens[0];
        String[] ts = state_month.split("_");

        context.write(new Text(ts[0]),new Text(ts[1]+"\t"+tokens[1]+"\t"+tokens[2]+"\t"+tokens[3]));

    }
}
