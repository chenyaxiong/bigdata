package com.itcast.wordcount.reducer;

import com.itcast.wordcount.sort.WordCountComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<WordCountComparator, LongWritable, Text, LongWritable> {

    private final LongWritable result = new LongWritable();
    private final Text resultKey = new Text();

    @Override
    protected void reduce(WordCountComparator key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        resultKey.set(key.getWord());
        context.write(resultKey, result);
    }
}
