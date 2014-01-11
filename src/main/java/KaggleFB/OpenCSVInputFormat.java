package KaggleFB;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by nic on 12/16/13.
 */
public class OpenCSVInputFormat extends FileInputFormat<LongWritable, TextArrayWritable> {

    @Override
    public RecordReader<LongWritable, TextArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        context.setStatus(split.toString());
        return new OpenCSVRecordReader(split, context);
    }

    @Override
    protected boolean isSplitable(JobContext job, Path p) {
        return false;
    }
}
