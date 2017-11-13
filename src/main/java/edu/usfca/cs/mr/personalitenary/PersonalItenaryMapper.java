package edu.usfca.cs.mr.personalitenary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;

/**
 * Created by bharu on 11/12/17.
 */
public class PersonalItenaryMapper extends Mapper<LongWritable,Text,Text,Text> {

    String[] arizonaGeohashes = {
        "9qx","9w8","9w9","9wd",
            "9qq","9qr","9w2","9w3","9w6",
            "9qn","9qp","9w0","9w1","9w4",
            "9my","9mz","9tb","9tc","9tf",
            "9mx","9t8","9t9","9td"
    };

    String[] floridaGeohashes = {
        "dj3","dj6","dj7","djk","djm","djq",
            "djj","djn","dhv","dhy","dhw"
    };

    String[] oregonGeohashes = {
      "c0p","c20","c21","c24","c25","c2h",
            "9pz","9rb","9rc","9rf","9rg","9ru",
            "9px","9r8","9r9","9rd","9re","9rs",
            "9pr","9r2","9r3","9r6","9r7","9rk"
    };

    String[] coloradoGeohashes = {
      "9x6","9x7","9xk","9xm","9xq","9xr",
            "9x4","9x5","9xh","9xj","9xn","9xp",
            "9wf","9wg","9wu","9wv","9wy","9wz",
            "9wd","9we","9ws","9wt","9ww","9wx"
    };

    String[] washingtonGeohashes = {
            "c28","c29","c2e","c2s","c0r",
            "c22","c23","c26","c27","c2k",
            "c0p","c20","c21","c24","c25",
            "c2h"
    };

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] features = value.toString().split("\t");
        String geohash = features[1];
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(features[0]));
        String temperature  = features[40];
        String humidity = features[12];
        String snowdepth = features[50];

        if(Arrays.asList(arizonaGeohashes).contains(geohash.substring(0,3)))
        {
            context.write(new Text("Arizona"+Integer.toString(calendar.get(Calendar.MONTH) + 1)),
                    new Text(geohash +"\t"+temperature +"\t"+humidity +"\t"+snowdepth));
        }

        if(Arrays.asList(floridaGeohashes).contains(geohash.substring(0,3)))
        {
            context.write(new Text("Florida"+Integer.toString(calendar.get(Calendar.MONTH) + 1)),
                    new Text(geohash +"\t"+temperature +"\t"+humidity +"\t"+snowdepth));
        }

        if(Arrays.asList(oregonGeohashes).contains(geohash.substring(0,3)))
        {
            context.write(new Text("Oregon"+Integer.toString(calendar.get(Calendar.MONTH) + 1)),
                    new Text(geohash +"\t"+temperature +"\t"+humidity +"\t"+snowdepth));
        }

        if(Arrays.asList(coloradoGeohashes).contains(geohash.substring(0,3)))
        {
            context.write(new Text("Colorado"+Integer.toString(calendar.get(Calendar.MONTH) + 1)),
                    new Text(geohash +"\t"+temperature +"\t"+humidity +"\t"+snowdepth));
        }

        if(Arrays.asList(washingtonGeohashes).contains(geohash.substring(0,3)))
        {
            context.write(new Text("Washington"+Integer.toString(calendar.get(Calendar.MONTH) + 1)),
                    new Text(geohash +"\t"+temperature +"\t"+humidity +"\t"+snowdepth));
        }
    }
}
