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
        float max_avg_temperature = 0;
        Iterator<Text> iterator = values.iterator();
        List<Text> values_1 = new ArrayList<>();

        Text te = iterator.next();
        values_1.add(new Text(te.toString()));
        String[] tokense = te.toString().split("\t");
        String avg_wind_speede = tokense[1];
        String avg_cloud_covere = tokense[2];
        String avg_tempe = tokense[3];
        min_avg_cloud_cover = Float.parseFloat(avg_cloud_covere);
        max_avg_wind_speed = Float.parseFloat(avg_wind_speede);
        max_avg_temperature = Float.parseFloat(avg_tempe);

        while (iterator.hasNext())
        {
            Text t  = iterator.next();
            values_1.add(new Text(t.toString()));
            String[] tokens = t.toString().split("\t");
            String avg_wind_speed = tokens[1];
            String avg_cloud_cover = tokens[2];
            String avg_temp = tokens[3];

            if(min_avg_cloud_cover > Float.parseFloat(avg_cloud_cover))
            {
                min_avg_cloud_cover = Float.parseFloat(avg_cloud_cover);
            }

            if(max_avg_wind_speed < Double.parseDouble(avg_wind_speed))
            {
                max_avg_wind_speed = Double.parseDouble(avg_wind_speed);
            }

            if(max_avg_temperature < Float.parseFloat(avg_temp))
            {
                max_avg_temperature = Float.parseFloat(avg_temp);
            }
        }

        System.out.println("Min cc: "+min_avg_cloud_cover+"Max ws: "+max_avg_wind_speed);
        for(Text t : values_1)
        {
            System.out.println("for loop2 :"+t);
            double fav_index_solar_wind = 0;
            double fav_index_solar = 0;
            String[] tokens = t.toString().split("\t");
            String avg_wind_speed = tokens[1];
            String avg_cloud_cover = tokens[2];
            String avg_temp = tokens[3];


            float cc_index = Float.parseFloat(avg_cloud_cover) - min_avg_cloud_cover;
            double ws_index = max_avg_wind_speed - Double.parseDouble(avg_wind_speed);
            float t_index = max_avg_temperature - Float.parseFloat(avg_temp);
            fav_index_solar = cc_index + t_index;

            fav_index_solar_wind = ws_index + cc_index + t_index;
            System.out.println("cc_index: "+cc_index+"ws_index: "+ws_index+ "t_index: "+t_index+"fav_index: "+fav_index_solar_wind);
            context.write(new Text(Double.toString(fav_index_solar_wind)+"\t"+Double.toString(fav_index_solar)),t);
        }
    }
}
