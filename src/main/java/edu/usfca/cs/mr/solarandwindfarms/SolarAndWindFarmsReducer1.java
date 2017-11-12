package edu.usfca.cs.mr.solarandwindfarms;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bharu on 11/8/17.
 */
public class SolarAndWindFarmsReducer1 extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        float min_avg_cloud_cover = 0;
        double max_avg_wind_speed = 0;
        int count = 0;
        Iterator<Text> iterator = values.iterator();
        List<Text> values_1 = new ArrayList<>();

        Text te = iterator.next();
        values_1.add(new Text(te.toString()));
        String[] tokense = te.toString().split("\t");
        String avg_wind_speede = tokense[1];
        String avg_cloud_covere = tokense[2];
        min_avg_cloud_cover = Float.parseFloat(avg_cloud_covere);
        max_avg_wind_speed = Float.parseFloat(avg_wind_speede);
        while (iterator.hasNext())
        {
            Text t  = iterator.next();
            values_1.add(new Text(t.toString()));
            String[] tokens = t.toString().split("\t");
            String avg_wind_speed = tokens[1];
            String avg_cloud_cover = tokens[2];

            if(min_avg_cloud_cover > Float.parseFloat(avg_cloud_cover))
            {
                min_avg_cloud_cover = Float.parseFloat(avg_cloud_cover);
            }

            if(max_avg_wind_speed < Double.parseDouble(avg_wind_speed))
            {
                max_avg_wind_speed = Double.parseDouble(avg_wind_speed);
            }
            count = count + 1;
        }

        System.out.println("Min cc: "+min_avg_cloud_cover+"Max ws: "+max_avg_wind_speed);
        for(Text t : values_1)
        {
            System.out.println("for loop2 :"+t);
            double fav_index = 0;
            String[] tokens = t.toString().split("\t");
            String avg_wind_speed = tokens[1];
            String avg_cloud_cover = tokens[2];

            float cc_index = Float.parseFloat(avg_cloud_cover) - min_avg_cloud_cover;
            double ws_index = max_avg_wind_speed - Double.parseDouble(avg_wind_speed);

            fav_index = ws_index + cc_index;
            System.out.println("cc_index: "+cc_index+"ws_index: "+ws_index+"fav_index: "+fav_index);
            context.write(new Text(Double.toString(fav_index)),t);
        }
    }
}
