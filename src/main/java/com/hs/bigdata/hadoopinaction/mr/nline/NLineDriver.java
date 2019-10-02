package com.hs.bigdata.hadoopinaction.mr.nline;

import com.hs.bigdata.hadoopinaction.mr.wordcount.WordcountDriver;
import com.hs.bigdata.hadoopinaction.mr.wordcount.WordcountMapper;
import com.hs.bigdata.hadoopinaction.mr.wordcount.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineDriver {
    public static void main(String[] args) throws  Exception{

        args = new String[] { "input/nlineinput.txt", "output/nline/wordcount.txt" };

        // 1 获取Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar存储位置
        job.setJarByClass(NLineDriver.class);

        // 3 关联Map和Reduce类
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        // 4 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置每个切片InputSplit中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);

        //使用NLineInputFormat处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        // 7 提交job
        // job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);


    }
}
