package edu.usfca.cs.mr.driestmonth;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/3/17.
 */
public class DriestMonthReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        float sum = 0;
        int count = 0;
        for(Text t : values)
        {
            String[] tokens = t.toString().split("\t");
            sum = sum + Float.parseFloat(tokens[1]);
            count = count + 1;
        }
        context.write(key,new Text(Float.toString(sum/count)));
    }
}
