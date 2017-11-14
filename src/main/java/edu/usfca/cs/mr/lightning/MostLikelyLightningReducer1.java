package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by bharu on 11/2/17.
 */
public class MostLikelyLightningReducer1 extends Reducer<Text,Text,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<Text> iterator = values.iterator();

        while (iterator.hasNext())
        {
            Text t = iterator.next();
            String[] tokens = t.toString().split(",");
            sum = sum + Integer.parseInt(tokens[1]);
        }
        System.out.println("lightning Reducer1 key: "+" "+"value: "+sum);
        context.write(key,new IntWritable(sum));
    }
}
