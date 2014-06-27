package pl.pw.elka.distmatrix;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistMatrixMapper extends Mapper<Text, Text, Text, Text> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Stemmer stemmer;
	private String w ;   
	private ForbWords forbWords;
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
	     while (itr.hasMoreTokens()) {
	    	w = itr.nextToken();
	    	if(stemmer != null){
		        stemmer.add(w);
		    	stemmer.stem();
		    	w =stemmer.toString();
	    	}
	    	if(forbWords != null){
	    		if(forbWords.isWorb(w)){
	    			w = null;
	    		}
	    	}
	    	if(w != null){
		        word.set(w);
		        context.write(word, key);
	    	}
	     }
	   }
	
	public void setStemmer(Stemmer stemmer){
		this.stemmer = stemmer;
	}

	public ForbWords getForbWords() {
		return forbWords;
	}

	public void setForbWords(ForbWords forbWords) {
		this.forbWords = forbWords;
	}
}
