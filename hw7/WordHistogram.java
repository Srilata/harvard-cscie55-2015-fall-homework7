package cscie55.hw7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordHistogram extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new WordHistogram(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("WordHistogram");
        job.setJarByClass(WordHistogram.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private LongWritable wordLength = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                wordLength.set(tokenizer.nextToken().length());
                context.write(wordLength, one);
            }
        }

    }

    public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

}
