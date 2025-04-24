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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SleepTime {

    // Mapper: receives exactly 4 lines per record (T, U, W, blank),
    // looks for "sleep" in the W-line, and if found emits (hour, 1).
    public static class SleepMapper
            extends Mapper<LongWritable, org.apache.hadoop.io.Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable hourBin = new IntWritable();
        private SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H"); 

        @Override
        protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] lines = record.split("\n");

            String ts = null;
            String text = "";

            // Parse the 4-line record
            for (String line : lines) {
                line = line.trim();
                if (line.startsWith("T\t")) {
                    String[] parts = line.split("\t", 2);
                    if (parts.length == 2) ts = parts[1];
                } else if (line.startsWith("W\t")) {
                    String[] parts = line.split("\t", 2);
                    if (parts.length == 2) text = parts[1].toLowerCase();
                }
            }

            // Only proceed if we saw both timestamp and a tweet containing "sleep"
            if (ts == null || !text.contains("sleep")) {
                return;
            }

            // Parse timestamp to hour
            try {
                Date d = inFmt.parse(ts);
                int h = Integer.parseInt(hourFmt.format(d));
                hourBin.set(h);
                context.write(hourBin, one);
            } catch (ParseException e) {
                // skip malformed timestamp
            }
        }
    }

    // Reducer: sums counts for each hour
    public static class SumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable total = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            total.set(sum);
            context.write(key, total);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SleepTime <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();

        // Process 4 lines per mapper record
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 4);
        Job job = Job.getInstance(conf, "Sleep Time Histogram");
        job.setJarByClass(SleepTime.class);

        // Use NLineInputFormat with 4 lines per record
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(SleepMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
