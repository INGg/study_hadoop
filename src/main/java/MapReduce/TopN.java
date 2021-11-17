package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.Reader;
import java.util.Comparator;
import java.util.TreeMap;

class TopNMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable>{
    private final TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] nums = line.split(" ");

        for(String num : nums){
            repToRecordMap.put(Integer.parseInt(num), " "); // 读取每行数据写入map中，超过5个就会移除最小的数值
            if(repToRecordMap.size() > 5){
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, NullWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for (Integer i : repToRecordMap.keySet()){
            try{
                context.write(NullWritable.get(), new IntWritable(i));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}

class TopNReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
    private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
        public int compare(Integer o1, Integer o2) {
            return o2 - o1;
        }
    });

    @Override
    protected void reduce(NullWritable key, Iterable<IntWritable> values, Reducer<NullWritable, IntWritable, NullWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for (IntWritable i : values){
            repToRecordMap.put(i.get(), " ");
            if(repToRecordMap.size() > 5){
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        for (Integer i : repToRecordMap.keySet()){
            context.write(NullWritable.get(), new IntWritable(i));
        }
    }
}

public class TopN {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 通过Job来封装本次MR的相关信息
        Configuration conf = new Configuration();

        // 配置MR的运行模式，使用local表示本地模式，可以省略
        conf.set("mapreduce.framework.name", "local");
        Job wcjob = Job.getInstance(conf);

        // 指定MR Job jar包运行主类
        wcjob.setJarByClass(TopN.class);

        // 指定本次MR所有的Mapper Reducer类
        wcjob.setMapperClass(TopNMapper.class);
        wcjob.setReducerClass(TopNReducer.class);

        // 设置业务逻辑Mapper类的输出和value类型
        wcjob.setMapOutputKeyClass(NullWritable.class);
        wcjob.setMapOutputValueClass(IntWritable.class);

        // 设置业务逻辑Reducer类的输出key与value类型
        wcjob.setOutputKeyClass(NullWritable.class);
        wcjob.setOutputValueClass(IntWritable.class);

        //使用本地模式所在的数据
        FileInputFormat.setInputPaths(wcjob, "file:/d:/Code/hadoop_java/data/Input/num.txt");

        // 使用本地模式处理完成后的结果保存位置
        FileOutputFormat.setOutputPath(wcjob, new Path("file:/D:/Code/hadoop_java/data/output/"));

        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
