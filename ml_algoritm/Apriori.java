package apriori;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class Apriori {
	private HashMap<String, Integer> tf;
	private HashMap<String, HashMap<String, Integer>> twoTF;
	private BufferedReader br;
	private double threshold;
	public void setThreshold(double confidenceThreshold) {
		this.threshold = confidenceThreshold;
	}
	public void initializeTF(int overNum) throws Exception {
		tf = new HashMap<String, Integer>();
		this.br = new BufferedReader(
				new FileReader("data/1TF_over"+overNum+".txt"));
		String[] ones = null;
		String line = br.readLine();
		while(line != null && !line.equals("")) {
			ones = line.split("\t");
			tf.put(ones[0], Integer.parseInt(ones[1]));
			
			line = br.readLine();
		}
		
		br.close();
		
	}
	
	public void initializeTwoTF(int overNum) throws Exception {
		twoTF = new HashMap<String, HashMap<String, Integer>>();
		this.br = new BufferedReader(
				new FileReader("data/2TF_over"+overNum+".txt"));
		String[] ones = null;
		String[] words = null;
		String line = br.readLine();
		Integer num = null;
		while(line != null && !line.equals("")) {
			ones = line.split("\t");
			words = ones[0].split(",");
			num = Integer.parseInt(ones[1]);
			
			if(!twoTF.containsKey(words[0])) 
				twoTF.put(words[0], new HashMap<String, Integer>());
			//if(!twoTF.containsKey(words[1]))
				//twoTF.put(words[1], new HashMap<String, Integer>());
			twoTF.get(words[0]).put(words[1], num);
			//twoTF.get(words[1]).put(words[0], num);
			
			line = br.readLine();
		}
		
		br.close();
		
	}
	
	public void run() throws IOException {
		FileWriter fw = new FileWriter("data/correlation.txt");
		for(String word1: twoTF.keySet()) {
			for(String word2: twoTF.get(word1).keySet()) {
				double alpha1 = (double)twoTF.get(word1).get(word2)/(double)tf.get(word1);
				double alpha2 = (double)twoTF.get(word1).get(word2)/(double)tf.get(word2);
				if(alpha1 > threshold) {
					fw.write(word1+"->"+word1+","+word2+"\n");
				}
				if(alpha2 > threshold) {
					fw.write(word2+"->"+word1+","+word2+"\n");
				}
			}
		}
		fw.close();
	}
	
	public Apriori() throws Exception {
		this.initializeTF(300);
		this.initializeTwoTF(500);
		this.setThreshold(0.9);
	}
	
	
	public static void main(String[] args) throws Exception {
		new Apriori().run();
	}
}
