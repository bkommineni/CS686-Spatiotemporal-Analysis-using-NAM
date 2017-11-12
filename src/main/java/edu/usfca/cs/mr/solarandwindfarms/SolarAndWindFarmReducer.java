package edu.usfca.cs.mr.solarandwindfarms;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/7/17.
 */
public class SolarAndWindFarmReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double sum_wind_speed = 0;
        float sum_cloud_cover = 0;
        int count = 0;
        for(Text t: values)
        {
            String[] tokens = t.toString().split("\t");
            sum_wind_speed = sum_wind_speed + Double.parseDouble(tokens[0]);
            sum_cloud_cover = sum_cloud_cover + Float.parseFloat(tokens[1]);
            count = count + 1;
        }
        context.write(key, new Text(Double.toString(sum_wind_speed/count) +","
                            +Float.toString(sum_cloud_cover/count)));
    }
}
