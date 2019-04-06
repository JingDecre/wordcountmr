package com.decre.hadoop.firstmr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/6 0006 9:37
 * @since 1.0.0
 * Descirption:
 */
/**
 * 首先，和前面一样，Reducer类也有输入和输出，输入就是Map阶段的处理结果，输出就是Reduce最后的输出
 * reducetask在调我们写的reduce方法,reducetask应该收到了前一阶段（map阶段）中所有maptask输出的数据中的一部分
 * （数据的key.hashcode%reducetask数==本reductask号），所以reducetaks的输入类型必须和maptask的输出类型一样
 * <p>
 * reducetask将这些收到kv数据拿来处理时，是这样调用我们的reduce方法的： 先将自己收到的所有的kv对按照k分组（根据k是否相同）
 * 将某一组kv中的第一个kv中的k传给reduce方法的key变量，把这一组kv中所有的v用一个迭代器传给reduce方法的变量values
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable result = new IntWritable();

    /**
     * Text key : mapTask输出的key值
     * Iterable<IntWritable> values ： key对应的value的集合（该key只是相同的一个key）
     * reduce方法接收key值相同的一组key-value进行汇总计算
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // key = EncodingUtils.transformTextToUTF8(key, "GBK");
        // 结果汇总
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        result.set(sum);
        // 汇总结果往外输出
        context.write(key, result);
    }

}
