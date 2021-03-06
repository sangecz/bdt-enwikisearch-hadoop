package cz.cvut.bigdata.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import cz.cvut.bigdata.cli.ArgumentParser;

/**
 * WordCount Example, version 1.0
 * 
 * This is a very simple extension of basic WordCount Example implemented using
 * a new MapReduce API.
 */
public class WordCount extends Configured implements Tool
{
    private String VOCABULARY_PATH = "/bigdata/marekp11_task2.txt";

    /**
     * The main entry of the application.
     */
    public static void main(String[] arguments) throws Exception
    {
        System.exit(ToolRunner.run(new WordCount(), arguments));
    }

    /**
     * Sorts word keys in reversed lexicographical order.
     */
    public static class WordCountComparator extends WritableComparator
    {
        protected WordCountComparator()
        {
            super(Text.class, true);
        }
        
        @Override public int compare(WritableComparable a, WritableComparable b)
        {
            // Here we use exploit the implementation of compareTo(...) in Text.class.
            return -a.compareTo(b);
        }
    }

    /**
     * (word, count) pairs are sent to reducers based on the length of the word.
     * 
     * The maximum length of a word that will be processed by
     * the target reducer. For example, using 3 reducers the
     * word length span processed by the reducers would be:
     * reducer     lengths processed
     * -------     -----------------
     *  00000           1 -- 14
     *  00001          15 -- 29
     *  00003          30 -- OO
     */
    public static class WordLengthPartitioner extends Partitioner<Text, StringLineWritable>
    {
        private static final int MAXIMUM_LENGTH_SPAN = 30;
        
        @Override public int getPartition(Text key, StringLineWritable value, int numOfPartitions)
        {
            if (numOfPartitions == 1)
                return 0;
            
            int lengthSpan = Math.max(MAXIMUM_LENGTH_SPAN / (numOfPartitions - 1), 1);
            
            return Math.min(Math.max(0, (key.toString().length() - 1) / lengthSpan), numOfPartitions - 1);
        }
    }

    /**
     * Receives (byteOffsetOfLine, textOfLine), note we do not care about the type of the key
     * because we do not use it anyway, and emits (word, 1) for each occurrence of the word
     * in the line of text (i.e. the received value).
     */
    public static class WordCountMapper extends Mapper<Object, Text, Text, StringLineWritable>
    {
        private final IntWritable ONE = new IntWritable(1);
        private StringLineWritable line = new StringLineWritable();
        private Text word = new Text();
        private Text docId = new Text();
        // filter patterns
        private static final String PATTERN_GOOD_WORDS = "[a-z]{3,25}";
        private static final String PATTERN_BAD_PREFIX = "^[^a-z]+";
        private static final String PATTERN_BAD_POSTFIX = "[^a-z]+$";
        private String DOC_ID_PATTERN = "[^0-9]+$";

        public static final String DOCUMENT_COUNT_HELPER = "aaamojesuperslovickocece";
        private HashSet<String> uniqueWords = new HashSet<>();
        private HashMap<String, Integer> wordIdVocab;
        private HashMap<String, Integer> termsFrequencyPerDoc;
        private int N;

        protected void setup(Context context) throws IOException, InterruptedException {
            wordIdVocab = new HashMap<>();

            URI uri = context.getCacheFiles()[0];
            BufferedReader bfr = new BufferedReader(new FileReader(new File(uri.getPath()).getName()));

            // read number of documents: N
            N = Integer.parseInt(bfr.readLine().split(" ")[1]);

            // read vocabulary & store: word-wordId
            String line;
            int wordId = 0;
            while ((line = bfr.readLine()) != null) {
                String [] words = line.split("\t");
                wordIdVocab.put(words[0].trim(), wordId++);
            }

            bfr.close();
        }

        /**
         *
         * @param key
         * @param value radka textu
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            ArrayList<String> arrayList = new ArrayList<String>(Arrays.asList(value.toString().toLowerCase().split(" ")));

            String docIdStr = arrayList.get(0).replaceFirst(DOC_ID_PATTERN, "");
            termsFrequencyPerDoc = new HashMap<>();

            for (String term : arrayList) {

                // procisti slova se spatnym prefixem a/nebo postfixem + zahrne i prvni slova za docId,
                // ktera jsou od id oddelena tabulatorem a vypadaji takto:  5345    prvniSlovo
                term = term.replaceFirst(PATTERN_BAD_PREFIX, "").replaceFirst(PATTERN_BAD_POSTFIX, "");

                if (term.matches(PATTERN_GOOD_WORDS)) {
                    // DF_i .. document freq. = num. of documents containing term i
                    if (uniqueWords.add(term)) { // add() == true, if set did not contains term
                        word.set(term);
                        // unique term_i vezmu pouze jednou pro dany document, pak v reduce bude indikovat pocet DF_i
//                        context.write(word, ONE);

                        // poprve viden term i, tf_i = 1
                        termsFrequencyPerDoc.put(term, 1);
                    } else {
                        // jiz vlozeno
                        if(termsFrequencyPerDoc.get(term) != null) {
                            int tf = termsFrequencyPerDoc.get(term);
                            termsFrequencyPerDoc.put(term, tf + 1);
                        }
                    }
                }
            }

            ArrayList<String> vector = new ArrayList<>();
            for(Map.Entry<String, Integer> entry : termsFrequencyPerDoc.entrySet()) {
                String t_i = entry.getKey();
                int tf_i = entry.getValue();
                // get term's id
                int id_i;
                if(wordIdVocab.get(t_i) != null) {
                    id_i = wordIdVocab.get(t_i);
                    vector.add(id_i + ":" + tf_i);
                }
            }

            line.set(vector.toString());
            docId.set(docIdStr);
            context.write(docId, line);

            // counts number of docs
            word.set(DOCUMENT_COUNT_HELPER);

//            context.write(word, ONE);
        }
    }

    /**
     * Receives (word, list[1, 1, 1, 1, ..., 1]) where the number of 1 corresponds to the total number
     * of times the word occurred in the input text, which is precisely the value the reducer emits along
     * with the original word as the key. 
     * 
     * NOTE: The received list may not contain only 1s if a combiner is used.
     */
    public static class WordCountReducer extends Reducer<Text, StringLineWritable, Text, StringLineWritable>
    {
        public void reduce(Text text, Iterable<StringLineWritable> values, Context context) throws IOException, InterruptedException
        {
            /*
             TODO N
             TODO Df
             TODO vykopirovany slovnik: smazat nesmyslna slova a stopwords (seradit a podle Df odmazat)
             TODO nakopirovat z5 do hdfs
             TODO
                puvodni wiki:  ID1 radek_clanku
                               ID2 radek_clanku
                vytvorit wiki: ID1 <ID_slova: TF>*
                               ID2 <ID_slova: TF>*

                job.addCacheFile(file) pro pouziti distribuovane cache na vsech nodech
            */

            int sum = 0;

            for (StringLineWritable value : values)
            {
                context.write(text, value);
            }

        }
    }

    /**
     * This is where the MapReduce job is configured and being launched.
     */
    @Override public int run(String[] arguments) throws Exception
    {
        ArgumentParser parser = new ArgumentParser("WordCount");

        parser.addArgument("input", true, true, "specify input directory");
        parser.addArgument("output", true, true, "specify output directory");

        parser.parseAndCheck(arguments);

        Path inputPath = new Path(parser.getString("input"));
        Path outputDir = new Path(parser.getString("output"));

        // Create configuration.
        Configuration conf = getConf();
        
        // Using the following line instead of the previous 
        // would result in using the default configuration
        // settings. You would not have a change, for example,
        // to set the number of reduce tasks (to 5 in this
        // example) by specifying: -D mapred.reduce.tasks=5
        // when running the job from the console.
        //
        // Configuration conf = new Configuration(true);

        // Create job.
        Job job = Job.getInstance(conf, "WordCount");
        job.addCacheFile(new Path(VOCABULARY_PATH).toUri());
        job.setJarByClass(WordCountMapper.class);

        // Setup MapReduce.
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // Make use of a combiner - in this simple case
        // it is the same as the reducer.
        job.setCombinerClass(WordCountReducer.class);

        // Sort the output words in reversed order.
        job.setSortComparatorClass(WordCountComparator.class);
        
        // Use custom partitioner.
        job.setPartitionerClass(WordLengthPartitioner.class);

        // By default, the number of reducers is configured
        // to be 1, similarly you can set up the number of
        // reducers with the following line.
        //
        // job.setNumReduceTasks(1);

        // Specify (key, value).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringLineWritable.class);
//        job.setOutputValueClass(IntWritable.class);

        // Input.
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output.
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem hdfs = FileSystem.get(conf);

        // Delete output directory (if exists).
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute the job.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
