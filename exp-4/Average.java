import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Average {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        private final static DoubleWritable average = new DoubleWritable(1);
        private Text rollNumber = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizerLine = new StringTokenizer(line, ",");
            while (tokenizerLine.hasMoreTokens()) {


                String rollNumber = tokenizerLine.nextToken();
                //name
                tokenizerLine.nextToken();
                double mark1 = Integer.parseInt(tokenizerLine.nextToken());
                double mark2 = Integer.parseInt(tokenizerLine.nextToken());
                double mark3 = Integer.parseInt(tokenizerLine.nextToken());

                System.out.println(mark1 + " mark1");
                System.out.println(mark2 + " mark2");
                System.out.println(mark3);
                double av = (mark1+mark2+mark3) / 3;

                average.set(av);
                Text k = new Text(rollNumber);
                context.write(k, average);
                // average.set(mark2);
                // context.write(k, average);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count += 1;
                System.out.println("val is " + val.get());
            }
            System.out.println("Sum is " + sum + " " + key);
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Average.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
