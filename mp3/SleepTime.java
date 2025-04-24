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

public class SleepTime {

    public static class SleepMapper
            extends Mapper<LongWritable, org.apache.hadoop.io.Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable hourBin = new IntWritable();

        // For parsing timestamps
        private SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H"); // 0–23

        // Holds the hour for the current tweet record
        private int lastHour = -1;

        @Override
        protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();

            // Skip header
            if (line.startsWith("total number:")) {
                return;
            }

            // T    YYYY-MM-DD HH:mm:ss
            if (line.startsWith("T\t")) {
                String ts = line.substring(2);
                try {
                    Date d = inFmt.parse(ts);
                    lastHour = Integer.parseInt(hourFmt.format(d));
                } catch (ParseException e) {
                    lastHour = -1;
                }
                return;
            }

            // W    tweet text
            if (line.startsWith("W\t") && lastHour >= 0) {
                String text = line.substring(2).toLowerCase();
                if (text.contains("sleep")) {
                    hourBin.set(lastHour);
                    context.write(hourBin, one);
                }
            }

            // Ignore U\t… and blank lines
        }
    }

    public static class SumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable total = new IntWritable();

        @Override
        protected void reduce(IntWritable key,
                              Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            total.set(sum);
            context.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SleepTime <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sleep Time Histogram");
        job.setJarByClass(SleepTime.class);

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
