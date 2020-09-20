package com.itcast.wordcount.job;

import com.itcast.wordcount.JobMain;
import com.itcast.wordcount.mapper.WordCountMapper;
import com.itcast.wordcount.partitions.WordCountPartitioner;
import com.itcast.wordcount.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountJob extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Job wordCountJob = Job.getInstance(super.getConf(), "word count job");
        // 设置执行main
        wordCountJob.setJarByClass(JobMain.class);
        // 设置输入数据类型，将该数据解析成k1，v1
        wordCountJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(wordCountJob, new Path("hdfs://node1:8020/wordcount"));

        // 设置map相关信息
        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(LongWritable.class);

        // 设置shuffle相关信息
        // 设置分区信息
        wordCountJob.setPartitionerClass(WordCountPartitioner.class);
        wordCountJob.setNumReduceTasks(2);

        // 涉及reduce相关信息
        wordCountJob.setReducerClass(WordCountReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(LongWritable.class);

        // 设置输入数据类型，也就是k3，v3的数据写入什么文件中
        wordCountJob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(wordCountJob, new Path("hdfs://node1:8020/wordcount_out_partitioner"));

        return wordCountJob.waitForCompletion(true) ? 0 : 1;
    }
}
