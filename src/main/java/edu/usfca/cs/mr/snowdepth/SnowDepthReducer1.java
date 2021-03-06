package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by bharu on 11/1/17.
 */
public class SnowDepthReducer1 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("Before Reducer 1");
        float max = 0;
        String geohash_max = null;

        Iterator<Text> iterator = values.iterator();
        Text t = iterator.next();
        String[] ts = t.toString().split(",");
        max = Float.parseFloat(ts[1]);
        geohash_max = ts[0];

        while (iterator.hasNext())
        {
            Text f = iterator.next();
            String str = f.toString();
            String[] tokens = str.split(",");
            if (Float.parseFloat(tokens[1]) > max) {
                max = Float.parseFloat(tokens[1]);
                geohash_max = tokens[0];
            }
        }
       context.write(new Text(geohash_max),new Text(Float.toString(max)));
    }
}
