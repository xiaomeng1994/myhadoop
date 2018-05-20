package com.meng.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCount {
    //首先要定义四个泛型的类型
    //keyin:  LongWritable    valuein: Text
    //keyout: Text            valueout:IntWritable
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //map方法的生命周期：  框架每传一行数据就被调用一次
        //key :  这一行的起始点在文件中的偏移量
        //value: 这一行的内容
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到一行数据转换为string
            String line = value.toString();
            //将这一行切分出各个单词
            String[] words = line.split(" ");
            //遍历数组，输出<单词，1>
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //生命周期：框架每传递进来一个kv 组，reduce方法被调用一次
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //定义一个计数器
            int count = 0;
            //遍历这一组kv的所有v，累加到count中
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "F:\\hadoop");
        Configuration conf = new Configuration();
        Job wcjob = new Job(conf,"wordCount");
        //指定我这个job所在的jar包
        //	wcjob.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(WordCount.class);
        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReducer.class);
        //设置我们的业务逻辑Mapper类的输出key和value的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(IntWritable.class);
        //设置我们的业务逻辑Reducer类的输出key和value的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);
        //指定要处理的数据所在的位置
        FileInputFormat.addInputPath(wcjob, new Path("E:\\jetbrains\\project\\idea\\myhadoop\\src\\main\\java\\com\\meng\\wordcount\\input"));
        Path output = new Path("E:\\jetbrains\\project\\idea\\myhadoop\\src\\main\\java\\com\\meng\\wordcount\\output");
        //输出目录存在就删除
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, output);
        //向yarn集群提交这个job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
