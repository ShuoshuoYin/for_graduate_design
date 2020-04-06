package apriori.tf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

/*
 * different map methods, for which different some tools methods
 * with the TwoWordsTable;
 * may be optimized later..
 * or may be not...
 * */

public class FourWordsTable {
	
	private static Analyzer analyzer = null; 
	private static Set<String> oneWordSet = null;
	
	private static boolean haveIntersection(String s0, String s1, String s2, String s3) {
		for(String s: s0.split("")) {
			if(s1.contains(s) || s2.contains(s) || s3.contains(s))
				return true;
		}
		for(String s: s1.split("")) {
			if(s2.contains(s) || s3.contains(s))
				return true;
		}
		for(String s: s2.split("")) {
			if(s3.contains(s))
				return true;
		}
		return false;
	}
	
	private static void adjustOrder(String[] words) {
		int k;
		String tmp;
		for(int i=0; i<3; i++) {
			k = i;
			for(int j=i+1; j<4; j++) 
				if(words[j].compareTo(words[k]) < 0)
					k = j;	
			tmp = words[i];
			words[i] = words[k];
			words[k] = tmp;
		}
		
		/*
		 
		 for(int i=0; i<3; i++) 
		 	for(int j=0; j<3-i; j++) 
		 		if(words[j+1].compareTo(words[j]) < 0) {
		 			tmp = words[j];
		 			words[j] = words[j+1];
		 			words[j+1] = tmp;
		 		}
		 
		 * */
		
		
	}
	
	// if not use reduce method, we can replace it with map<String, Integer>
	
	// change from readIntoList, as the data structure change to set.
	public static void readIntoSet() {
		oneWordSet = new HashSet<String>();
		InputStream is = null;
		BufferedReader br = null;
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			is = new URL("hdfs", "192.168.145.131", 9000,
					"/apriori/split_1000_gbk/part-r-00000").openStream();
			br = new BufferedReader(
					new InputStreamReader(is));
			String line = null;
			while((line = br.readLine()) != null) { 
				line = new String(line.getBytes(), 0, 
						line.length(), "GBK");
				line = line.split("\t")[0];
				oneWordSet.add(line);
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				is.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String queryStr = new String(value.getBytes(), 0, 
					value.getLength(), "GBK").split("\t")[1];
			
			// using paoding split the query sentence
			TokenStream ts = null;
			Token token = null;
			String word = null;
			String[] words = new String[4];
			int i = 0;
			ts = analyzer.tokenStream("", new StringReader(queryStr));
			while ((token = ts.next()) != null) {
				word = token.termText();
				if(oneWordSet.contains(word)) 
					words[i++] = word;
				if(i == 4) break;
			}
			if(i < 4) return;
			
			if(haveIntersection(words[0], words[1], words[2], words[3]))
				return ;
			adjustOrder(words);
			context.write(
					new Text(words[0]+","
							+words[1]+","
							+words[2]+","
							+words[3]), new Text(""));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int time = 0;
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				it.next();
				time++;
			}
			if(time > 1000)
				context.write(key, new Text(time + ""));
		}
	}

	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		FourWordsTable.readIntoSet();
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.145.131:9000");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		
		analyzer = new PaodingAnalyzer();
		
		Job job = Job.getInstance(conf, "FourWordsTable");
		job.setJarByClass(FourWordsTable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(GBKOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/SogouQ"));
		FileInputFormat.setInputDirRecursive(job, true);
		FileOutputFormat.setOutputPath(job, new Path("/apriori/four_words_table/fourTF_1000"));

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(df.format(new Date()));
		job.waitForCompletion(true);
		System.out.println(df.format(new Date()));
		System.out.println("Done");
	}
}

/*
 * 82KB, 
 * 16.4*2**10 5B
 * 16.4*2**10 25B
 * 
 * */
//generate a table, two-terms as one line
	/* a file having over 600 MB, so drop the method.
	public static void generateTwoWordsTable() {
		twoWordsList = new ArrayList<String>();
		haveGoneWordSet = new HashSet<String>();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.145.131:9000");
		FileSystem fs = null;
		FSDataOutputStream fsout = null;
		try {
			fs = FileSystem.get(conf);
			fsout = fs.create(
					new Path("/apriori/two_words_table/combined2"));
			for(String word1: oneWordList) {
				for(String word2: oneWordList) {
					if(word1 == word2 || haveGoneWordSet.contains(word2))
						continue;
					fsout.writeBytes(word1+"\t"+word2+"\n");
				}
				haveGoneWordSet.add(word1);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				fsout.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	*/