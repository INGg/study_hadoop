package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

class UnionMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        Text line = new Text();
        line = value;
        context.write(line, NullWritable.get());
    }
}

class UnionReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

public class DataUnion {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        conf.set("DataUnion", "local");
        Job job = Job.getInstance(conf);

        job.setJarByClass(DataUnion.class);

        job.setMapperClass(UnionMapper.class);
        job.setReducerClass(UnionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, "file:/d:/Code/hadoop_java/data/Input/");
        FileOutputFormat.setOutputPath(job, new Path("file:/d:/Code/hadoop_java/data/output/"));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
