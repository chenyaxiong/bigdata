package com.itcast.wordcount.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class WordCountComparator implements WritableComparable<WordCountComparator> {

    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    @Override
    public int compareTo(WordCountComparator other) {
        int compare = Integer.compare(this.word.length(), other.word.length());
//        if (compare == 0) {
//            compare = this.word.compareTo(other.word);
//        }
        return compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordCountComparator that = (WordCountComparator) o;

        return word.equals(that.word);
    }

    @Override
    public int hashCode() {
        return word.hashCode();
    }

    @Override
    public String toString() {
        return "WordCountComparator{" +
                "word='" + word + '\'' +
                '}';
    }
}
