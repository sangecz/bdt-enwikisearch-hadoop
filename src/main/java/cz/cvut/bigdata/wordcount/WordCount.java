package cz.cvut.bigdata.wordcount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.hadoop.security.UserGroupInformation;
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
    private static final String INVERTED_INDEX_FILEPATH = "/bigdata/marekp11_task3.txt";
    private static final String HBAFILE_OUTPUT_PATH = "/bigdata/marekp11_hbase";
    private static final String TABLE_NAME = "marekp11:wiki_index";
    private static String VOCABULARY_PATH = "/bigdata/marekp11_task2.txt";

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
        private Text WORD_id = new Text();
        // filter patterns
        private static final String PATTERN_GOOD_WORDS = "[a-z]{3,25}";
        private static final String PATTERN_BAD_PREFIX = "^[^a-z]+";
        private static final String PATTERN_BAD_POSTFIX = "[^a-z]+$";
        private static final String PATTERN_WHITESPACE = "\\s+";
        private String DOC_ID_PATTERN = "[^0-9]+$";

        public static final String DOCUMENT_COUNT_HELPER = "aaamojesuperslovickocece";
        private HashSet<String> uniqueWords = new HashSet<>();
        private HashMap<String, Integer> wordIdVocab;
        private HashMap<String, Integer> termsFrequencyPerDoc;
        private int N;
        private double[] idfs;

        protected void setup(Context context) throws IOException, InterruptedException {
            wordIdVocab = new HashMap<>();

            ArrayList<String> lines = Util.readLines(context.getCacheFiles()[0]);
            if (lines.isEmpty()) {
                return;
            }

            // read number of documents: N
            String [] firstLine = lines.get(0).split(" ");
            N = Integer.parseInt(firstLine[1]);

            idfs = new double[lines.size()];

            // skip first line ->> N
            // radek: slovo  #vyskytu
            for (int row = 1; row < lines.size(); row++)  {
                String[] words = lines.get(row).split(PATTERN_WHITESPACE);

                wordIdVocab.put(words[0], row);
                idfs[row] = Double.parseDouble(words[1]);
            }

//            System.out.println("=== wordIdVocab.size=" + wordIdVocab.size() + ", N=" + N + ", idfs.len=" + idfs.length);
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

                // procisti slova se spatnym prefixem a/nebo postfixem + zahrne i prvni slova za WORD_id,
                // ktera jsou od id oddelena tabulatorem a vypadaji takto:  5345    prvniSlovo
                term = term.replaceFirst(PATTERN_BAD_PREFIX, "").replaceFirst(PATTERN_BAD_POSTFIX, "");

                if (term.matches(PATTERN_GOOD_WORDS)) {
                    // DF_i .. document freq. = num. of documents containing term i
                    if (uniqueWords.add(term)) { // add() == true, if set did not contains term
                        word.set(term);
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

            for(Map.Entry<String, Integer> entry : termsFrequencyPerDoc.entrySet()) {
                String t_i = entry.getKey();
                int tf_i = entry.getValue();
                // get term's id
                int word_id;
                // potoze ve slovniku nejsou vsechna slova
                if(wordIdVocab.get(t_i) != null) {
                    word_id = wordIdVocab.get(t_i);

                    line.set(docIdStr + ":" + tf_i);
                    WORD_id.set(word_id + "");
                    context.write(WORD_id, line);
                }
            }
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
                puvodni wiki:  DOC_ID1 radek_clanku
                               DOC_ID2 radek_clanku
                vytvorit wiki: DOC_ID1 <ID_slova: TF>*    ...mam
                               DOC_ID2 <ID_slova: TF>*
                vytvorit wiki  ID_slova <DOC_ID1:TF>*

                job.addCacheFile(file) pro pouziti distribuovane cache na vsech nodech
            */

            boolean first = true;
            StringBuilder toReturn = new StringBuilder();
            for (StringLineWritable value : values) {
                if (!first)
                    toReturn.append(", ");
                first=false;
                toReturn.append(value.toString());
            }

            StringLineWritable slv = new StringLineWritable();
            slv.set(toReturn.toString());
            context.write(text, slv);
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

        // HBASE
        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hadoop.tmp.dir", "/tmp");
        hconf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(hconf);
        UserGroupInformation.getUGIFromTicketCache(System.getenv("KRB5CCNAME"), null);



        // Create job.
        Job job = Job.getInstance(hconf, "WordCount");
//        job.addCacheFile(new Path(VOCABULARY_PATH).toUri());
        job.setJarByClass(HbaseMapper.class);

        // Setup MapReduce.
        job.setMapperClass(HbaseMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        FileInputFormat.addInputPath(job, new Path(INVERTED_INDEX_FILEPATH));
        job.setInputFormatClass(StringLineWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(HBAFILE_OUTPUT_PATH));

        Connection connection = ConnectionFactory.createConnection(hconf);
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table hTable = connection.getTable(tableName);
        RegionLocator regionLocator = connection.getRegionLocator(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator);

        FileSystem hdfs = FileSystem.get(conf);

        // Delete output directory (if exists).
        if (hdfs.exists(new Path(HBAFILE_OUTPUT_PATH)))
            hdfs.delete(new Path(HBAFILE_OUTPUT_PATH), true);

        // Execute the job.
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private class HbaseMapper extends Mapper<Text, StringLineWritable, ImmutableBytesWritable, KeyValue> {

        public void map(Text word, StringLineWritable docs_tfs, Context context) throws IOException, InterruptedException
        {
            // String word_id; String doc_id; int tf;

            System.out.println("================XXX " + word.toString());

            byte[] word_id_bytes = Bytes.toBytes(word.toString());
            ImmutableBytesWritable HKey = new ImmutableBytesWritable(word_id_bytes);

            ArrayList<String> arrayList = new ArrayList<String>(Arrays.asList(docs_tfs.toString().toLowerCase().split(" ")));
            System.out.println("================XXX " + arrayList);
            for (String doc_tf : arrayList) {


                String [] tmp = doc_tf.split(":");
                String doc_id = tmp[0];
                String tf = tmp[1];
                byte[] doc_bytes = Bytes.toBytes(doc_id);
                byte[] tf_bytes = Bytes.toBytes(tf);
                byte[] col_family = Bytes.toBytes("doc");

                KeyValue kv = new KeyValue(word_id_bytes, col_family, doc_bytes, tf_bytes);
                context.write(HKey, kv);
            }
        }



    }
}
