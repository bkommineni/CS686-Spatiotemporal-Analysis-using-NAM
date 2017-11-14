package edu.usfca.cs.mr.personalitenary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bharu on 11/13/17.
 */
public class PersonalItenaryReducer1 extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float min_avg_snow_depth = 0;
        float min_avg_rel_humidity = 0;
        float max_avg_temperature = 0;
        Iterator<Text> iterator = values.iterator();
        List<Text> values_1 = new ArrayList<>();

        Text te = iterator.next();
        values_1.add(new Text(te.toString()));
        String[] tokense = te.toString().split("\t");
        String avg_tempe = tokense[1];
        String avg_rel_humiditye = tokense[2];
        String avg_snow_depthe = tokense[3];
        min_avg_rel_humidity = Float.parseFloat(avg_rel_humiditye);
        min_avg_snow_depth = Float.parseFloat(avg_snow_depthe);
        max_avg_temperature = Float.parseFloat(avg_tempe);

        while (iterator.hasNext())
        {
            Text t  = iterator.next();
            values_1.add(new Text(t.toString()));
            String[] tokens = t.toString().split("\t");
            String avg_temp = tokens[1];
            String avg_rel_humidity = tokens[2];
            String avg_snow_depth = tokens[3];

            if(min_avg_rel_humidity > Float.parseFloat(avg_rel_humidity))
            {
                min_avg_rel_humidity = Float.parseFloat(avg_rel_humidity);
            }

            if(min_avg_snow_depth > Float.parseFloat(avg_snow_depth))
            {
                min_avg_snow_depth = Float.parseFloat(avg_snow_depth);
            }

            if(max_avg_temperature < Float.parseFloat(avg_temp))
            {
                max_avg_temperature = Float.parseFloat(avg_temp);
            }
        }

        for(Text t : values_1)
        {
            System.out.println("for loop2 :"+t);
            double fav_index = 0;
            String[] tokens = t.toString().split("\t");
            String avg_temp = tokens[1];
            String avg_rel_humidity = tokens[2];
            String avg_snow_depth = tokens[3];


            float rh_index = Float.parseFloat(avg_rel_humidity) - min_avg_rel_humidity;
            double sd_index = Float.parseFloat(avg_snow_depth) - min_avg_snow_depth;
            float t_index = max_avg_temperature - Float.parseFloat(avg_temp);
            fav_index = rh_index + sd_index + t_index;

            context.write(new Text(Double.toString(fav_index)+"\t"+key.toString()),t);
        }
    }
}
