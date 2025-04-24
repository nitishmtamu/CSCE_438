import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TweetTime {

    // Mapper: for each "T\tYYYY-MM-DD HH:mm:ss" line, parse hour and emit (hour, 1)
    public static class HourMapper
            extends Mapper<LongWritable, org.apache.hadoop.io.Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable hourBin = new IntWritable();
        private SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H"); // yields "0"–"23"

        @Override
        protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();

            // skip header
            if (line.startsWith("total number:")) return;
            // only lines beginning with 'T'
            if (line.length() < 2 || line.charAt(0) != 'T') return;

            // split on tab to get timestamp
            String[] parts = line.split("\t", 2);
            if (parts.length < 2) return;
            String ts = parts[1];

            try {
                Date d = inFmt.parse(ts);
                int h = Integer.parseInt(hourFmt.format(d));
                hourBin.set(h);
                context.write(hourBin, one);
            } catch (ParseException e) {
                // ignore
            }
        }
    }

    // Reducer: sum counts for each hour
    public static class SumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable total = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
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
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        // now keys are IntWritable for numeric sorting
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
