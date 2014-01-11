package KaggleFB;

/**
 * Created by nic on 12/14/13.
 */

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


public class SentExtractMR extends Configured implements Tool {

    private static InputStream sentModelIn;
    private static SentenceModel sentModel;
    private static SentenceDetector sentDetector;

    static {
        try {
            sentModelIn = SentExtractMR.class.getClassLoader().getResourceAsStream("en-sent.bin");
            sentModel = new SentenceModel(sentModelIn);
            sentDetector = new SentenceDetectorME(sentModel);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    private static String[] getParagraphs(String in) {
        List<String> stringArrayList = new ArrayList<String>();

        Integer i = 0;
        try {
            for (i = 0; i + 3 < in.length(); i++) {
                if (in.substring(i, i + 3).equals("<p>")) {
                    i += 3;
                    StringBuilder current = new StringBuilder();
                    while (i + 4 < in.length() && !in.substring(i, i + 4).equals("</p>")) {
                        current.append(in.charAt(i));
                        i++;
                    }
                    if (i + 4 >= in.length() && !in.substring(i, in.length()).equals("</p>"))
                        current.append(in.substring(i, in.length()));
                    stringArrayList.add(current.toString());
                    i += 4;
                }
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(in.length());
            System.err.println(i);
            e.printStackTrace();
            throw e;
        }

        String[] out = new String[stringArrayList.size()];
        stringArrayList.toArray(out);
        return out;
    }

    static class SentenceSplitMap
            extends Mapper<LongWritable, TextArrayWritable, LongWritable, Text> {

        public void map(LongWritable key, TextArrayWritable value, Context context)
                throws IOException, InterruptedException {

            Text[] fields = (Text[]) value.toArray();

            if (fields.length != 4) return;
            int id;
            try {
                id = Integer.parseInt(fields[0].toString());
            } catch (NumberFormatException e) {
                return;
            }
            String title = fields[1].toString();
            String body = fields[2].toString();
            String tags = fields[3].toString().trim();

            String[] bodyParagraphs = getParagraphs(body);
            if (bodyParagraphs.length == 0)
                return;

            StringBuilder valueString;

            for (String par : bodyParagraphs) {
                for (String line : par.split("[\n\r]")) {
                    for (String sent : sentDetector.sentDetect(line)) {
                        valueString = new StringBuilder();
                        valueString.append("body\t");
                        valueString.append(tags.replace("\t", " "));
                        valueString.append("\t");
                        valueString.append(sent.replace("\t", " "));
                        context.write(new LongWritable(id), new Text(valueString.toString()));
                    }
                }
            }
            for (String sent : sentDetector.sentDetect(title)) {
                valueString = new StringBuilder();
                valueString.append("title\t");
                valueString.append(tags.replace("\t", " "));
                valueString.append("\t");
                valueString.append(sent.replace("\t", " "));
                context.write(new LongWritable(id), new Text(valueString.toString()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SentExtractMR.class);

        job.setInputFormatClass(OpenCSVInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path((args[1])));

        job.setMapperClass(SentenceSplitMap.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)
            throws Exception {

        int res = ToolRunner.run(new Configuration(), new SentExtractMR(), args);
        System.exit(res);
    }
}
