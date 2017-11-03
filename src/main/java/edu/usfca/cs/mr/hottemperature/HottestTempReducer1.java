package edu.usfca.cs.mr.hottemperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.transform.Templates;
import java.io.IOException;

/**
 * Created by bharu on 11/1/17.
 */
public class HottestTempReducer1 extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float max_temp = 0;
        String geohash_max = null;
        String timestamp_max = null;

        for(Text t : values)
        {
            String[] tokens = t.toString().split(",");
            if(Float.parseFloat(tokens[2]) > max_temp)
            {
                max_temp = Float.parseFloat(tokens[2]);
                geohash_max = tokens[0];
                timestamp_max = tokens[1];
            }
        }
        context.write(new Text(geohash_max),new Text(timestamp_max+","+Float.toString(max_temp)));
    }
}
