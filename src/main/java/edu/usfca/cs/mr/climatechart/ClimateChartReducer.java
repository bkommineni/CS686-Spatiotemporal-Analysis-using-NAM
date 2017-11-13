package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by bharu on 11/6/17.
 */
public class ClimateChartReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        float max_temp = 0;
        float min_temp = 0;
        float sum_temp = 0;
        float sum_precp = 0;
        int count = 0;

        Iterator<Text> iterator = values.iterator();
        Text te = iterator.next();
        String[] ts = te.toString().split("\t");
        max_temp = Float.parseFloat(ts[1]);
        min_temp = Float.parseFloat(ts[1]);
        sum_temp = sum_temp + Float.parseFloat(ts[1]);
        sum_precp = sum_precp + Float.parseFloat(ts[2]);
        count = count + 1;

        while (iterator.hasNext())
        {
            Text t = iterator.next();
            String[] tokens = t.toString().split("\t");
            String geohash = tokens[0];
            String temperature = tokens[1];
            String precipitation = tokens[2];

            if(Float.parseFloat(temperature) > max_temp)
            {
                max_temp = Float.parseFloat(temperature);
            }

            if(Float.parseFloat(temperature) < min_temp)
            {
                min_temp = Float.parseFloat(temperature);
            }
            sum_temp = sum_temp + Float.parseFloat(temperature);
            sum_precp = sum_precp + Float.parseFloat(precipitation);
            count =count + 1;
        }

        context.write(key,new Text(Float.toString(min_temp)+"\t"+Float.toString(max_temp)+"\t"
                        +Float.toString(sum_temp/count)+"\t"+Float.toString(sum_precp/count)));
    }
}
