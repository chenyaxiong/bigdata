package com.itcast.wordcount;

import com.itcast.wordcount.job.WordCountJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JobMain {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Tool wordCountJob = new WordCountJob();
        int run = ToolRunner.run(configuration, wordCountJob, args);
        System.exit(run);
    }
}
