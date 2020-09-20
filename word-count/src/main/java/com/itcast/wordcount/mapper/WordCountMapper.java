package com.itcast.wordcount.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable,Text, Text, LongWritable> {

    private final Text text = new Text();
    private LongWritable longWritable = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
        while (stringTokenizer.hasMoreTokens()) {
            text.set(stringTokenizer.nextToken());
            context.write(text, longWritable);
        }
//        if (StringUtils.isNotBlank(line)) {
//            for (String word : line.split("\t")) {
//                text.set(word);
//                context.write(text, longWritable);
//            }
//        }
    }
}
