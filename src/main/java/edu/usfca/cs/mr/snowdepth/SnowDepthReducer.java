package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class SnowDepthReducer extends Reducer<Text, FloatWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        boolean allGreater = true;
        float sum = 0;
        int count = 0;

        for(FloatWritable f : values)
        {
            if(f.get() > 0)
            {
                sum = sum + f.get();
                count = count + 1;
            }
            else
            {
                allGreater = false;
                break;
            }
        }

        if(allGreater)
        {
            float avg = sum/count;
            context.write(key,new Text(Float.toString(avg)));
        }
    }
}
