import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
        // Input timestamp format: "YYYY-MM-DD HH:mm:ss"
        private SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // Hour of day (0-23)
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H"); // 0-23

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // Only process lines starting with "T "
            if (!line.startsWith("T ")) {
                return;
            }
            // Extract the timestamp substring
            // line = "T 2009-06-01 00:00:00"
            String ts = line.substring(2);
            try {
                Date d = inFmt.parse(ts);
                String h = hourFmt.format(d);
                hourBin.set(h);
                context.write(hourBin, one);
            } catch (ParseException e) {
                // skip malformed lines
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
