package edu.usfca.cs.mr.driestmonth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bharu on 11/3/17.
 */
public class DriestMonthMapper extends Mapper<LongWritable,Text,Text,Text> {

    String filename = null;
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
        String[] tokens = filename.split(".");
        String year_month = tokens[0];
        Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])([0-9][0-9])");
        Matcher matcher = pattern.matcher(year_month);
        String year = matcher.group(1);
        String month =  matcher.group(2);
        String[] features = value.toString().split("\t");
        String geohash = features[1];
        String precipitation = features[55];
        if(Arrays.asList(bayareaDataPoints).contains(geohash.substring(0,4)))
        {
            context.write(new Text(month), new Text(geohash+"\t"+precipitation));
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
        filename = context.getConfiguration().get(fsFileSplit.getPath().getParent().getName());
    }
}
