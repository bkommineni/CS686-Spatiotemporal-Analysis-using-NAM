package edu.usfca.cs.mr.hottemperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by bharu on 11/1/17.
 */
public class HottestTempReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float max_temp = 0;
        String geo_hash_max = null;
        String timestamp_at_max = null;

        Iterator<Text> iterator = values.iterator();
        Text te = iterator.next();
        String[] ts = te.toString().split(",");
        max_temp = Float.parseFloat(ts[2]);
        geo_hash_max = ts[1];
        timestamp_at_max = ts[0];

        while (iterator.hasNext())
        {
            Text t = iterator.next();
            String[] tokens = t.toString().split(",");
            if (Float.parseFloat(tokens[2]) > max_temp) {
                max_temp = Float.parseFloat(tokens[2]);
                geo_hash_max = tokens[1];
                timestamp_at_max = tokens[0];
            }
        }
        context.write(new Text(geo_hash_max),new Text(timestamp_at_max+","+Float.toString(max_temp)));
    }
}
