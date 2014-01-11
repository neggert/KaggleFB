package KaggleFB;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by nic on 12/16/13.
 */
public class OpenCSVRecordReader extends RecordReader<LongWritable, TextArrayWritable> {
    long start, end;
    FSDataInputStream fileIn;
    InputStreamReader reader;
    CSVReader csv;
    String[] nextVal;
    int nextKey;

    private LongWritable key = null;
    private TextArrayWritable value = null;

    public OpenCSVRecordReader(InputSplit insplit, TaskAttemptContext context) throws IOException, InterruptedException {
        initialize(insplit, context);
    }

    @Override
    public void initialize(InputSplit insplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) insplit;
        Configuration conf = context.getConfiguration();

        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(conf);
        fileIn = fs.open(split.getPath());
        fileIn.seek(start);
        reader = new InputStreamReader(fileIn);
        csv = new CSVReader(reader);

        nextVal = csv.readNext();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return nextVal != null;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(nextKey);
        nextKey++;
        return key;
    }

    @Override
    public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
        if (value == null)
            value = new TextArrayWritable();
        if (nextVal == null)
            throw new IOException();

        Text[] texts = new Text[nextVal.length];
        for (int i = 0; i < nextVal.length; i++) {
            texts[i] = new Text(nextVal[i]);
        }
        value.set(texts);

        nextVal = csv.readNext();
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        long pos = fileIn.getPos();
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        csv.close();
        reader.close();
        fileIn.close();
    }
}
