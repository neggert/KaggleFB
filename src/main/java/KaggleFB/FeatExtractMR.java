package KaggleFB;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by nic on 12/21/13.
 */
public class FeatExtractMR extends Configured implements Tool {

    private static InputStream tokenModelIn;
    private static TokenizerModel tokenModel;
    private static Tokenizer tokenizer;

    private enum target {NOTAG, TAGSTART, TAGMID, START}

    static {
        try {
            tokenModelIn = FeatExtractMR.class.getClassLoader().getResourceAsStream("en-token.bin");
            tokenModel = new TokenizerModel(tokenModelIn);
            tokenizer = new TokenizerME(tokenModel);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    static class FeatureMap
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            if (data.length != 4) return;
            String type = data[1];
            try {
                int id = Integer.parseInt(data[0]);
            } catch (NumberFormatException e) {
                return;
            }
            String sentence = data[3];
            String tagString = data[2];
            String[] tagsConcat = tagString.split(" ");
            List<String[]> tags = new ArrayList<String[]>();
            for (String tagConc : tagsConcat) {
                tags.add(tagConc.split("-"));
            }
            String prevWord, nextWord;
            target prevTarget;
            Span[] wordSpans = tokenizer.tokenizePos(sentence);
            String[] allWords = Span.spansToStrings(wordSpans, sentence);
            List<String> wordList = new ArrayList<String>();
            for (String word : allWords) {
                if (word.replaceAll("[^a-zA-Z]", "").length() > 0)
                    wordList.add(word);
            }
            String[] words = new String[wordList.size()];
            wordList.toArray(words);

            target[] wordTargets = new target[words.length];
            Arrays.fill(wordTargets, target.NOTAG);

            // get targets
            int j = 0, end = 0;
            for (String[] tagWords : tags) {
                for (int i = 0; i < words.length; i++) {
                    if (tagWords.length > 0 && words[i].equals(tagWords[0])) {
                        j = 1;
                        while (j < tagWords.length && i + j < words.length && words[i + j].toLowerCase().equals(tagWords[j])) {
                            j++;
                        }
                        end = i + j;
                        if (j == tagWords.length) {
                            wordTargets[i] = target.TAGSTART;
                            for (i++; i < end; i++) {
                                wordTargets[i] = target.TAGMID;
                            }
                        }
                    }
                }
            }

            // get features
            StringBuilder featString;
            for (Integer i = 0; i < words.length; i++) {
                if (words.length == 1) {
                    prevWord = "START";
                    nextWord = "END";
                    prevTarget = target.START;
                } else if (i == 0) {
                    prevWord = "START";
                    nextWord = words[i + 1].toLowerCase();
                    prevTarget = target.START;
                } else if (i + 1 == words.length) {
                    prevWord = words[i - 1].toLowerCase();
                    nextWord = "END";
                    prevTarget = wordTargets[i - 1];
                } else {
                    prevWord = words[i - 1].toLowerCase();
                    nextWord = words[i + 1].toLowerCase();
                    prevTarget = wordTargets[i - 1];
                }
                featString = new StringBuilder();
                featString.append(tagString);
                featString.append("\t");
                featString.append(prevWord);
                featString.append("\t");
                featString.append(nextWord);
                featString.append("\t");
                featString.append(prevTarget.ordinal());
                featString.append("\t");
                featString.append(words[i].toLowerCase());
                featString.append("\t");
                featString.append(Character.isUpperCase(words[i].charAt(0)));
                featString.append("\t");
                featString.append(wordTargets[i].ordinal());
                context.write(new Text(type), new Text(featString.toString()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FeatExtractMR.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path((args[1])));

        job.setMapperClass(FeatureMap.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)
            throws Exception {

        int res = ToolRunner.run(new Configuration(), new FeatExtractMR(), args);
        System.exit(res);
    }
}
