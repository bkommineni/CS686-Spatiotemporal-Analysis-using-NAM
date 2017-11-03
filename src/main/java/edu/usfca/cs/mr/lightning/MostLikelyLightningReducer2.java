package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/2/17.
 */
public class MostLikelyLightningReducer2 extends Reducer<Text,Text,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String geohash_max = null;
        int max_lightning = 0;
        for(Text t : values)
        {
            String[] tokens = t.toString().split(",");
            if(Integer.parseInt(tokens[1]) > max_lightning)
            {
                max_lightning = Integer.parseInt(tokens[1]);
                geohash_max = tokens[0];
            }
        }
        context.write(new Text(geohash_max),new IntWritable(max_lightning));
    }
}
