package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class SnowDepthReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        boolean allGreater = true;
        float sum = 0;
        int count = 0;

        for(Text t : values)
        {
            float f = Float.parseFloat(t.toString());
            if(f > 0)
            {
                sum = sum + f;
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
