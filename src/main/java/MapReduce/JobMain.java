package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {

    // 该方法指定一个job任务
    public int run(String[] args) throws Exception {
        // 1.创建一个Job对象
        Job wcjob = Job.getInstance(super.getConf(), "wordcount");

        // 2.配置job任务

        // (1)指定任务的读取方式及读取路径
        wcjob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(wcjob, new Path("hdfs://master:9000/wordcount/"));
//        TextInputFormat.addInputPath(wcjob, new Path("file:///d:/Code/hadoop_java/data/Input/1579E2.txt")); // 本机模式

        // (2)指定map阶段的处理方式
        wcjob.setMapperClass(WordCountMapper.class);
        // 设置map阶段的k2类型
        wcjob.setMapOutputKeyClass(Text.class);
        // 设置V2类型
        wcjob.setOutputValueClass(IntWritable.class);

        // (3、4、5、6)shuffle阶段，采用默认处理

        // (7) reduce阶段的处理阶段和数据类型
        wcjob.setReducerClass(WordCountReducer.class);
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);

        // (8) 设置输出类型
        wcjob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(wcjob, new Path("hdfs://master:9000/wordcount/output"));
//        TextOutputFormat.setOutputPath(wcjob, new Path("file:/D:/Code/hadoop_java/data/output/"));

        // 等待任务结束
        boolean res = wcjob.waitForCompletion(true);

        return res ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        // 启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);

        System.exit(run);
    }
}
