package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/2/17.
 */
public class MostLikelyLightningReducer extends Reducer<Text, FloatWritable, Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(FloatWritable f : values)
        {
            if(f.get() == 1.0)
                count = count +1;
        }
        context.write(key,new IntWritable(count));
    }
}
