package MapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

// Mapper组件
class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 接收传入进来的一行文本，把数据类型转为String
        String line = value.toString();
        // 内容分隔
        String[] words = line.split(" ");
        // 遍历数组，每出现过一个单词就标记一个数组1，组装<k1,v1>
        for(String word: words){
            // 使用 context，把Map阶段处理的数据发送给Reduce阶段作为输入数据
            // Context context这是可以记录输入的key和value
            // 它是mapper的一个内部类，简单的说顶级接口是为了在map或是reduce任务中跟踪task的状态，
            // 很自然的MapContext就是记录了map执行的上下文，在mapper类中，这个context可以存储一些job conf的信息，
            // 比如习题一中的运行时参数等，我们可以在map函数中处理这个信息，这也是hadoop中参数传递中一个很经典的例子，
            // 同时context作为了map和reduce执行中各个函数的一个桥梁，这个设计和java web中的session对象、application对象很相似。
            context.write(new Text(word), new IntWritable(1));
            // 也就是context这个对象，就是连接各个操作步骤的桥梁
        }
    }
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    // reduce方法作用：将新的K2和V2转为K3和V3，并将其写入上下文对象中（context）
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int cnt = 0;

        // 遍历原本的{key, list{1, 2, ..., num}}，把他们加起来
        for(IntWritable iw: values){
            cnt += iw.get();
        }

        context.write(key, new IntWritable(cnt));
    }
}

class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int cnt = 0;
        for(IntWritable v : values){
            cnt += v.get();
        }
        context.write(key, new IntWritable(cnt));
    }
}

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 通过Job来封装本次MR的相关信息
        Configuration conf = new Configuration();

        // 配置MR的运行模式，使用local表示本地模式，可以省略
        conf.set("mapreduce.framework.name", "local");
        Job wcjob = Job.getInstance(conf);

            // 指定MR Job jar包运行主类
        wcjob.setJarByClass(WordCountDriver.class);

        // 指定本次MR所有的Mapper Reducer类
        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReducer.class);

        // 设置业务逻辑Mapper类的输出和value类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(IntWritable.class);

        // 设置业务逻辑Reducer类的输出key与value类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);

        //使用本地模式所在的数据
        FileInputFormat.setInputPaths(wcjob, "file:/d:/Code/hadoop_java/data/Input/1579E2.txt");

        // 使用本地模式处理完成后的结果保存位置
        FileOutputFormat.setOutputPath(wcjob, new Path("file:/D:/Code/hadoop_java/data/output/"));

        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
