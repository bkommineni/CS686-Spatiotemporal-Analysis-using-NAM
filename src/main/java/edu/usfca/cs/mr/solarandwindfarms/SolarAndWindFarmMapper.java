package edu.usfca.cs.mr.solarandwindfarms;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by bharu on 11/7/17.
 */
public class SolarAndWindFarmMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] features = value.toString().split("\t");
        String geohash = features[1];
        String u_comp_max_wind = features[53];
        String v_comp_max_wind = features[37];
        String cloud_cover = features[16];
        String land_cover = features[18];
        String temperature = features[40];

        float u_max_wind = Float.parseFloat(u_comp_max_wind);
        float v_max_wind = Float.parseFloat(v_comp_max_wind);
        double mag_wind = Math.sqrt(Math.pow(u_max_wind,2)+Math.pow(v_max_wind,2));

        if((Float.parseFloat(land_cover) == 1.0) && (Float.parseFloat(cloud_cover) >= 0.0))
            context.write(new Text(geohash),new Text(Double.toString(mag_wind)+"\t"+cloud_cover+"\t"+
            temperature));
    }
}
