package com.itcast.wordcount.group;

import com.itcast.wordcount.sort.WordCountComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountGroup extends WritableComparator {

    public WordCountGroup() {
        super(WordCountComparator.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WordCountComparator one = (WordCountComparator) a;
        WordCountComparator two = (WordCountComparator) b;
        return one.getWord().compareTo(two.getWord());
    }
}
