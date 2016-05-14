package cz.cvut.bigdata.wordcount;

import com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


@InterfaceAudience.Public
@InterfaceStability.Stable
public class MyInputFormat extends FileInputFormat<LongWritable, Text> {
    public MyInputFormat() {
    }

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if(null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }

        return new LineRecordReader(recordDelimiterBytes);
    }

    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = (new CompressionCodecFactory(context.getConfiguration())).getCodec(file);
        return null == codec?true:codec instanceof SplittableCompressionCodec;
    }
}

