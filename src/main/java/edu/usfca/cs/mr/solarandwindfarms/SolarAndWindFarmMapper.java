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

    String[] northAmericaDataPoints = {
            "cy","fn","fq","fw","fy","gn","gq","gw",
            "cm","ct","cv","fj","fm","ft","fv","gj","gm",
            "bk","bs","ch","ck","cs","cu","fh","fk","fu",
            "gh","gk","b7","be","bg","c5","c7","ce","cg","f7",
            "fg","g5","c4","c6","cd","cf","f6",
            "c1","c3","c9","cc","f1","f3","f9",
            "c2","c8","cb","f0","f2","9r","9x","9z","dp",
            "9q","9w","9y","dn","9t","9v","9s",
            "9e","9g","d4"
    };
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] features = value.toString().split("\t");
        String geohash = features[1];
        String u_comp_max_wind = features[53];
        String v_comp_max_wind = features[37];
        String cloud_cover = features[16];

        float u_max_wind = Float.parseFloat(u_comp_max_wind);
        float v_max_wind = Float.parseFloat(v_comp_max_wind);
        double mag_wind = Math.sqrt(Math.pow(u_max_wind,2)+Math.pow(v_max_wind,2));
        if(Arrays.asList(northAmericaDataPoints).contains(geohash.substring(0,2)))
            context.write(new Text(geohash),new Text(Double.toString(mag_wind)+"\t"+cloud_cover));
    }
}
