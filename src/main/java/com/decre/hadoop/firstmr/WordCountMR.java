package com.decre.hadoop.firstmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/6 0006 9:14
 * @since 1.0.0
 * Descirption:
 */
public class WordCountMR {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 集群上，通过命令行获得输入和输出路径，实现动态化的conf配置
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //本地运行
        //String[] otherArgs = new String[]{"input/dream.txt","output"};
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordconut <in> [《in>...]<out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word conut");
        job.setJarByClass(WordCountMR.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);
        // 是否正常退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
