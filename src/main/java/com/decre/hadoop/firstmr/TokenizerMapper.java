package com.decre.hadoop.firstmr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Decre
 * @date 2019/4/6 0006 9:27
 * @since 1.0.0
 * Descirption:
 */

/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * <p>
 * KEYIN 是指框架读取到的数据的key的类型，在默认的InputFormat下，读到的key是一行文本的起始偏移量，所以key的类型是Long
 * VALUEIN 是指框架读取到的数据的value的类型,在默认的InputFormat下，读到的value是一行文本的内容，所以value的类型是String
 * KEYOUT 是指用户自定义逻辑方法返回的数据中key的类型，由用户业务逻辑决定，在此wordcount程序中，我们输出的key是单词，所以是String
 * VALUEOUT 是指用户自定义逻辑方法返回的数据中value的类型，由用户业务逻辑决定,在此wordcount程序中，我们输出的value是单词的数量，所以是Integer
 * <p>
 * 但是，String ，Long等jdk中自带的数据类型，在序列化时，效率比较低，hadoop为了提高序列化效率，自定义了一套序列化框架
 * 所以，在hadoop的程序中，如果该数据需要进行序列化（写磁盘，或者网络传输），就一定要用实现了hadoop序列化框架的数据类型
 * <p>
 * Long ----> LongWritable
 * String ----> Text
 * Integer ----> IntWritable
 * Null ----> NullWritable
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    /**
     * LongWritable key : 该key就是value该行文本的在文件当中的起始偏移量
     * Text value ： 就是MapReduce框架默认的数据读取组件TextInputFormat读取文件当中的一行文本
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Java语言中，提供了专门用来分析字符串的类StringTokenizer（位于java.util包中）。
        // 该类可以将字符串分解为独立使用的单词，并称之为语言符号。
        // 语言符号之间由定界符（delim）或者是空格、制表符、换行符等典型的空白字符来分隔。
        // hadoop写死了utf-8格式，中文可能会乱码，需要进行处理
        // value = EncodingUtils.transformTextToUTF8(value, "GBK");
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }

}
