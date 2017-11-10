package edu.usfca.cs.mr.driestmonth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bharu on 11/3/17.
 */
public class DriestMonthMapper extends Mapper<LongWritable,Text,Text,Text> {

    String[] bayareaDataPoints = {
            "9qbh",
            "9qbk",
            "9qb7",
            "9qbs",
            "9qbe",
            "9qbd",
            "9qb9",
            "9qb8",
            "9qbg",
            "9qbf",
            "9qbc",
            "9qbb",
            "9q8z",
            "9q8y",
            "9q8v",
            "9qc5",
            "9qc4",
            "9qc1",
            "9qc0",
            "9q9p",
            "9q9n",
            "9q9j",
            "9q9h",
            "9qc6",
            "9qc3",
            "9qc2",
            "9q9r",
            "9q9q",
            "9q9m",
            "9q9k",
            "9q97",
            "9q9t",
            "9q9s",
            "9q9e",
            "9q9d"
    };

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] features = value.toString().split("\t");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(features[0]));
        String geohash = features[1];
        String precipitation = features[55];
        if(Arrays.asList(bayareaDataPoints).contains(geohash.substring(0,4)))
        {
            context.write(new Text(Integer.toString(calendar.get(Calendar.MONTH )+1)),
                    new Text(geohash+"\t"+precipitation));
        }
    }
}
