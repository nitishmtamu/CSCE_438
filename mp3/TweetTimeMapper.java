import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetTimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text hourBin = new Text();
    private SimpleDateFormat inFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
    private SimpleDateFormat hourFmt = new SimpleDateFormat("H"); // 0–23

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // assuming each record is a JSON string per line with a "created_at" field
        String line = value.toString();
        // crude extraction of created_at timestamp:
        int idx = line.indexOf("\"created_at\":\"");
        if (idx < 0) return;
        int start = idx + 14;
        int end = line.indexOf('"', start);
        if (end < 0) return;
        String ts = line.substring(start, end);
        try {
            Date d = inFmt.parse(ts);
            String h = hourFmt.format(d);
            hourBin.set(h);               // e.g. "0", "1", ..., "23"
            context.write(hourBin, one);
        } catch (ParseException e) {
            // skip malformed dates
        }
    }
}
