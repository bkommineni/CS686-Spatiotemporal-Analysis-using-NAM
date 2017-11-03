package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/2/17.
 */
public class MostLikelyLightningReducer1 extends Reducer<Text,Text,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String geohash_max = null;
        int max = 0;

        for(Text t : values)
        {
            String[] tokens = t.toString().split(",");
            if(Integer.parseInt(tokens[1]) > max)
            {
                max = Integer.parseInt(tokens[1]);
                geohash_max = tokens[0];
            }
        }
        System.out.println("lightning Reducer1 key: "+" "+"value: "+max);
        context.write(new Text(geohash_max),new IntWritable(max));
    }
}
