package cscie55.hw7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class DocWordIndex extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
	int res = ToolRunner.run(new DocWordIndex(), args);
	System.exit(res);
    }

    public int run(String[] args) throws Exception {
	Path inputPath = new Path(args[0]);
	Path outputPath = new Path(args[1]);

	Configuration conf = getConf();
	Job job = new Job(conf, this.getClass().toString());

	FileInputFormat.setInputPaths(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);

	job.setJobName("DocWordIndex");
	job.setJarByClass(DocWordIndex.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();


        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
            int index = -1;
            while (tokenizer.hasMoreTokens()) {
                String str = tokenizer.nextToken();
                word.set(str);
                index = line.indexOf(str, index+1);
                context.write(word, new Text(filePathString+" "+index) );
            }

        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        java.util.Map<String, String> wordByFileMap2 = new HashMap<String, String>();

        for (Text value : values) {
            String str = value.toString();
            StringTokenizer st = new StringTokenizer(str);
            if (st.hasMoreTokens()) {
                String file = st.nextToken();
                String index = st.nextToken();
                if (wordByFileMap2.containsKey(file)) {
                    String value2 = wordByFileMap2.get(file);
                    wordByFileMap2.put(file, index +" "+value2);
                } else {
                    wordByFileMap2.put(file, index);
                }
            }
        }

        java.util.Map<String, String> wordByFileMap = new java.util.TreeMap<String, String>(wordByFileMap2);


        StringBuilder sb = new StringBuilder();
        java.util.Iterator it = wordByFileMap.entrySet().iterator();
        while (it.hasNext()) {
            java.util.Map.Entry pair = (java.util.Map.Entry)it.next();
            sb.append(pair.getKey() + " " + pair.getValue() + " ");
        }

        context.write(key, new Text(sb.toString()));
    }
    }

}
