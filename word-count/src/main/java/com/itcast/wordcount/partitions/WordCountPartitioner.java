package com.itcast.wordcount.partitions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable> {

    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        String wordStr = text.toString();
        if (wordStr.length() > 5) {
            return 0;
        } else {
            return 1;
        }
    }
}
