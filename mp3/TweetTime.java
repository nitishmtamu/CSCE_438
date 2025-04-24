import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TweetTime {

    // Mapper reads each line; when it sees a line beginning "T ",
    // it parses the timestamp and emits (hour, 1).
    public static class HourMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text hourBin = new Text();
        private SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            // 1) Skip the header line
            if (line.startsWith("total number:")) {
                return;
            }

            // 2) Only process lines whose first character is 'T'
            if (line.length() < 2 || line.charAt(0) != 'T') {
                return;
            }

            // 3) Split on the tab to get the timestamp part
            //    e.g. "T\t2009-06-01 21:43:59"
            String[] parts = line.split("\t", 2);
            if (parts.length < 2) {
                return;
            }
            String ts = parts[1];  // the timestamp

            // count for debugging
            context.getCounter("TweetTime", "T_lines_seen").increment(1);

            try {
                Date d = inFmt.parse(ts);
                String h = hourFmt.format(d);
                hourBin.set(h);
                context.write(hourBin, one);
            } catch (ParseException e) {
                // malformed timestamp—skip
            }
        }
    }

    // Reducer sums the counts for each hour
    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TweetTime <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tweet Time Histogram");
        job.setJarByClass(TweetTime.class);

        job.setMapperClass(HourMapper.class);
        // optional combiner
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
