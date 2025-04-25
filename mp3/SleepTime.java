import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class SleepTime {

    public static class TweetInputFormat extends FileInputFormat<LongWritable, Text> {
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return true;
        }

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException {
            return new TweetRecordReader();
        }
    }

    public static class TweetRecordReader extends RecordReader<LongWritable, Text> {
        private LineReader in;
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private long start;
        private long end;
        private long pos;
        private int recordNum = 0;

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            Path path = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) genericSplit).getPath();
            Configuration job = context.getConfiguration();
            FSDataInputStream fileIn = path.getFileSystem(job).open(path);
            in = new LineReader(fileIn, job);
            start = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) genericSplit).getStart();
            end = start + ((org.apache.hadoop.mapreduce.lib.input.FileSplit) genericSplit).getLength();
            pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            Text line = new Text();
            StringBuilder record = new StringBuilder();
            boolean inRecord = false;

            while (pos < end) {
                int bytesRead = in.readLine(line);
                pos += bytesRead;
                String l = line.toString().trim();

                if (l.startsWith("T\t")) {
                    if (inRecord) {
                        break;
                    }
                    inRecord = true;
                    record.setLength(0);
                }

                if (inRecord) {
                    record.append(l).append("\n");
                }
            }

            if (record.length() == 0) {
                return false;
            }

            key.set(recordNum++);
            value.set(record.toString());
            return true;
        }

        @Override
        public LongWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            return (pos - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }
    }

    public static class SleepMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable hourBin = new IntWritable();
        private SimpleDateFormat inFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SimpleDateFormat hourFmt = new SimpleDateFormat("H");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");
            int hour = -1;
            String post = "";

            for (String line : lines) {
                if (line.startsWith("T\t")) {
                    String ts = line.substring(2).trim();
                    try {
                        Date d = inFmt.parse(ts);
                        hour = Integer.parseInt(hourFmt.format(d));
                    } catch (ParseException e) {
                        hour = -1;
                    }
                } else if (line.startsWith("W\t")) {
                    post = line.substring(2).toLowerCase();
                }
            }

            if (hour >= 0 && post.contains("sleep")) {
                hourBin.set(hour);
                context.write(hourBin, one);
            }
        }
    }

    public static class SumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
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
        Job job = Job.getInstance(conf, "Sleep Time Histogram");
        job.setJarByClass(SleepTime.class);
        job.setInputFormatClass(TweetInputFormat.class);
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
