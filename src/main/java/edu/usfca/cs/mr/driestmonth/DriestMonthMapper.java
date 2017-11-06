package edu.usfca.cs.mr.driestmonth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bharu on 11/3/17.
 */
public class DriestMonthMapper extends Mapper<LongWritable,Text,Text,Text> {

    String filename = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = filename.split(".");
        String year_month = tokens[0];
        Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9])([0-9][0-9])");
        Matcher matcher = pattern.matcher(year_month);
        String year = matcher.group(1);
        String month =  matcher.group(2);
        context.write(new Text(year),new Text());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
        filename = context.getConfiguration().get(fsFileSplit.getPath().getParent().getName());
    }
}
