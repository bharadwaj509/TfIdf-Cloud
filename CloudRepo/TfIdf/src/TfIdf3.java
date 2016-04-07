import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TfIdf3 {
	/**
	 * WordsInCorpusTFIDFMapper implements the Job 3 specification for the TF-IDF algorithm
	 * @author Marcello de Sales (marcello.desales@gmail.com)
	 */
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
	 
	    /**
	     * @param key is the byte offset of the current line in the file;
	     * @param value is the line from the file
	     * @param output has the method "collect()" to output the key,value pair
	     * @param reporter allows us to retrieve some information about the job (like the current filename)
	     *
	     *     PRE-CONDITION: marcello@book.txt  \t  3/1500
	     *     POST-CONDITION: marcello, book.txt=3/1500
	     */
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] wordAndCounters = value.toString().split("\t");
	        String[] wordAndDoc = wordAndCounters[0].split("@");                 //3/1500
	        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
	    }
	}
	
	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
	 
	    private static final DecimalFormat DF = new DecimalFormat("###.########");
	 
	    /**
	     * @param key is the key of the mapper
	     * @param values are all the values aggregated during the mapping phase
	     * @param context contains the context of the job run
	     *
	     *             PRECONDITION: receive a list of <word, ["doc1=n1/N1", "doc2=n2/N2"]>
	     *             POSTCONDITION: <"word@doc1,  [d/D, n/N, TF-IDF]">
	     */
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        // get the number of documents indirectly from the file-system (stored in the job name on purpose)
	        int numberOfDocumentsInCorpus = Integer.parseInt(context.getJobName());
	        // total frequency of this word
	        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
	        Map<String, String> tempFrequencies = new HashMap<String, String>();
	        for (Text val : values) {
	            String[] documentAndFrequencies = val.toString().split("=");
	            numberOfDocumentsInCorpusWhereKeyAppears++;
	            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
	        }
	        for (String document : tempFrequencies.keySet()) {
	            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
	 
	            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
	            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
	                    / Double.valueOf(wordFrequenceAndTotalWords[1]));
	 
	            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
	            double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;
	 
	            //given that log(10) = 0, just consider the term frequency in documents
	            double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ?
	                    tf : tf * Math.log10(idf);
	 
	            context.write(new Text(key + "@" + document), new Text("[" + numberOfDocumentsInCorpusWhereKeyAppears + "/"
	                    + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
	                    + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]"));
	        }
	    }
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "Word in Corpus, TF-IDF");
 
        job.setJarByClass(TfIdf3.class);
        job.setMapperClass(Map3.class);
        job.setReducerClass(Reduce3.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        //Getting the number of documents from the original input directory.
        Path inputPath = new Path("/user/aryasobn/inputs");
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
 
        //Dirty hack to pass the total number of documents as the job name.
        //The call to context.getConfiguration.get("docsInCorpus") returns null when I tried to pass
        //conf.set("docsInCorpus", String.valueOf(stat.length)) Or even
        //conf.setInt("docsInCorpus", stat.length)
        job.setJobName(String.valueOf(stat.length));
 
        job.waitForCompletion(true);
	}

}
