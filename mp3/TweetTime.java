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

    // Mapper: reads each JSON tweet record (one per line), extracts "created_at",
    // parses out the hour (0-23), and emits (hour, 1).
    public static class HourMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text hourBin = new Text();
        // Input timestamp format, e.g. "Wed Oct 10 20:19:24 +0000 2018"
        private SimpleDateFormat inFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
        // Hour of day (0-23)
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H"); // 0-23

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // crude extraction of created_at field
            int idx = line.indexOf("\"created_at\":\"");
            if (idx < 0) return;
            int start = idx + 14;
            int end = line.indexOf('"', start);
            if (end < 0) return;
            String ts = line.substring(start, end);

            try {
                Date d = inFmt.parse(ts);
                String h = hourFmt.format(d);
                hourBin.set(h);
                context.write(hourBin, one);
            } catch (ParseException e) {
                // ignore malformed timestamp
            }
        }
    }

    // Reducer: sums all the 1's for each hour key
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
        // optional combiner (same as reducer)
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
