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

    public static class HourMapper
            extends Mapper<LongWritable, org.apache.hadoop.io.Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable hourBin = new IntWritable();
        private SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H");

        @Override
        protected void map(LongWritable key, org.apache.hadoop.io.Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.startsWith("total number:")) return;
            if (line.length() < 2 || line.charAt(0) != 'T') return;

            String[] parts = line.split("\t", 2);
            if (parts.length < 2) return;
            String ts = parts[1];

            try {
                Date d = inFmt.parse(ts);
                int h = Integer.parseInt(hourFmt.format(d));
                hourBin.set(h);
                context.write(hourBin, one);
            } catch (ParseException e) {
            }
        }
    }

    public static class SumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tweet Time Histogram");
        job.setJarByClass(TweetTime.class);
        job.setMapperClass(HourMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
