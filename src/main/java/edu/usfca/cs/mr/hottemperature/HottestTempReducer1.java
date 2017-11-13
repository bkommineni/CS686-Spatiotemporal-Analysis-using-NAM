package edu.usfca.cs.mr.hottemperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.transform.Templates;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;

/**
 * Created by bharu on 11/1/17.
 */
public class HottestTempReducer1 extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float max_temp = 0;
        String geohash_max = null;
        String timestamp_max = null;

        Iterator<Text> iterator = values.iterator();
        Text te = iterator.next();
        String[] ts = te.toString().split(",");
        max_temp = Float.parseFloat(ts[2]);
        geohash_max = ts[1];
        timestamp_max = ts[0];

        while (iterator.hasNext())
        {
            Text t = iterator.next();
            String[] tokens = t.toString().split(",");
            if(Float.parseFloat(tokens[2]) > max_temp)
            {
                max_temp = Float.parseFloat(tokens[2]);
                geohash_max = tokens[0];
                timestamp_max = tokens[1];
            }
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(timestamp_max));
        context.write(new Text(geohash_max),new Text(calendar.getTime()+","+Float.toString(max_temp)));
    }
}
