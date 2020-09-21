package com.itcast.wordcount.partitions;

import com.itcast.wordcount.sort.WordCountComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<WordCountComparator, LongWritable> {

    @Override
    public int getPartition(WordCountComparator text, LongWritable longWritable, int i) {
        String wordStr = text.getWord();
        if (wordStr.length() >= 5) {
            return 0;
        } else {
            return 1;
        }
    }
}
