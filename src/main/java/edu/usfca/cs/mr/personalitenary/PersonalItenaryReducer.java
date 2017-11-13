package edu.usfca.cs.mr.personalitenary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bharu on 11/12/17.
 */
public class PersonalItenaryReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        float sum_temp = 0;
        float sum_rel_hum = 0;
        float sum_snow_depth = 0;
        float avg_temp = 0;
        float avg_rel_hum = 0;
        float avg_snow_depth = 0;
        int count  = 0;

        float pref_temp_min = 290;
        float pref_temp_max = 302;
        float pref_rel_humidty_max = 80;
        float pref_snow_depth = 5;

        for(Text t : values)
        {
            String[] tokens = t.toString().split("\t");
            float temperature = Float.parseFloat(tokens[1]);
            float rel_humidity = Float.parseFloat(tokens[2]);
            float snowdepth = Float.parseFloat(tokens[3]);

            sum_temp = sum_temp + temperature;
            sum_rel_hum = sum_rel_hum + rel_humidity;
            sum_snow_depth = sum_snow_depth + snowdepth;
            count = count + 1;
        }
        avg_temp = sum_temp /count;
        avg_rel_hum = sum_rel_hum /count;
        avg_snow_depth = sum_snow_depth /count;

        if(((pref_temp_min < avg_temp) && (avg_temp < pref_temp_max)) &&
                (avg_rel_hum < pref_rel_humidty_max) &&
                (avg_snow_depth < pref_snow_depth))
        {
            context.write(key,new Text(avg_temp +"\t"+avg_rel_hum+"\t"+avg_snow_depth));
        }

    }
}
