package dbscan;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class DBSCAN {
	private HashMap<String, HashSet<String>> wordMap;
	private HashMap<String, Integer> pointIn; // word in which cluster
	private HashMap<Integer, HashSet<String>> clusterMap;
	private HashMap<String, Integer> tf;
	private int minPointNum;
	public void setMinPointNum(int n) {
		this.minPointNum = n;
	}
	private double radius;
	private BufferedReader br;
	public void initialize(int overNum) {
		this.wordMap = new HashMap<String, HashSet<String>>();
		this.tf = new HashMap<String, Integer>();
		this.clusterMap = new HashMap<Integer, HashSet<String>>();
		this.pointIn = new HashMap<String, Integer>();
		try {
			this.br = new BufferedReader(
					new FileReader("data/2TF_over"+overNum+".txt"));
			this.radius = 1/(double)100; // 关联次数越多，距离越小，半径如此理解，但不使用
			String line = this.br.readLine();
			String[] words = null;
			String[] wordStr;
			Integer num;
			while(line != null && !line.equals("")) {
				wordStr = line.split("\t");
				words = wordStr[0].split(",");
				if(!wordMap.containsKey(words[0])) 
					wordMap.put(words[0], new HashSet<String>());
				((HashSet)wordMap.get(words[0])).add(words[1]);	
				if(!wordMap.containsKey(words[1])) 
					wordMap.put(words[1], new HashSet<String>());
				((HashSet)wordMap.get(words[1])).add(words[0]);
				
				num = Integer.parseInt(wordStr[1]);
				tf.put(wordStr[0], num);
				
				line = this.br.readLine();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				this.br.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}

	}
	/*
	public void run_recursion() throws Exception {
		this.br = new BufferedReader(
				new FileReader("data/1TF_over300.txt"));
		String line = this.br.readLine();
		String word = null;
		int label = 0;
		Queue<String> q = new LinkedList<String>();
		while(line != null && !line.equals("")) { // 每次循环都是搞出一个cluster来
			word = line.split("\t")[0];
			if(pointIn.containsKey(word)) { 
				// 被考察过，已经加入了cluster
				line = this.br.readLine();
				continue;
			}
			if(!wordMap.containsKey(word)
					|| wordMap.get(word).size() < this.minPointNum) {
				// 要么必然noise, labeling 0
				// 要么以后成为border point或者有成为border point的可能
				pointIn.put(word, 0);  
				line = this.br.readLine();
				continue;
			}
				
			// 如果大于等于minPointNum值，那么开始一次聚类
			label ++;
			q.offer(word);
			String w = null;
			while(!q.isEmpty()) {
				w = q.poll();
				pointIn.put(w, label); // 不管怎样，本次聚类涉及到的都加入
				// 否则首先聚类的那个被偏好
				// 可为什么还是第一个聚类总是远大于别的聚类？我知道了，
				// 就是因为密度直连，它先把所有的中心点给吃了
				// 于是就呈放射状增大了，给它加个权重吧，让它范围越大越不好扩
				// 不行，不能递归，要随机地几个聚簇一起聚
				if(wordMap.get(w).size() >= this.minPointNum) {
					for(String k:wordMap.get(w)) {
						if(!pointIn.containsKey(k) 
							|| pointIn.get(k) != label)
							q.offer(k);
					}
				}
			}
			System.out.println("一个聚簇   "+label);
			line = this.br.readLine();
		}
		this.br.close();
	}
	*/
	
	public void run_parallel(int overNum) throws Exception {
		this.br = new BufferedReader(
				new FileReader("data/1TF_over"+overNum+".txt"));
		String line = this.br.readLine();
		String word = null;
		int label = 0;
		while(line != null && !line.equals("")) {
			word = line.split("\t")[0];
			if(!pointIn.containsKey(word)) { // 没有加入任何聚类
				if(wordMap.containsKey(word) 
					&& wordMap.get(word).size() >= this.minPointNum) {
					label++;   // 并且还是个中心点，那么造个类label，让直达的点加入其中
					pointIn.put(word, label);
					for(String k:wordMap.get(word)) {
						// 若这个被考察的中心点的这个直达点有了别的标记
						// 那么这个直达点要根据它周围所有中心的标记来决定加入哪个
						// 加入TF最小的那个，或者说IDF最大的那个
						if(pointIn.containsKey(k) && pointIn.get(k) != 0) {
							int max = 0;
							String joinWhich=null;
							for(String kk: wordMap.get(k)) {
								if(wordMap.get(kk).size() >= this.minPointNum
									&& pointIn.containsKey(kk)) {
									if(tf.containsKey(kk+","+k)) {
										if(tf.get(kk+","+k) > max) {
											max = tf.get(kk+","+k);
											joinWhich = kk;
										}
									}
									else if(tf.containsKey(k+","+kk)) {
										if(tf.get(k+","+kk) > max) {
											max = tf.get(k+","+kk);
											joinWhich = kk;
										}
									}
								}
							}
							pointIn.put(k, pointIn.get(joinWhich));
							continue;
						}
						pointIn.put(k, label);
					}
				}
				else { // 要么不是中心点，要么radius内直接就没有直达的点
					pointIn.put(word, 0); // 不管什么情况，先标为noise，否则没机会了
				}
			}
			else if(pointIn.get(word) != 0){ // 加入了某个聚类，还不是标为噪声那个类
				if(wordMap.get(word).size() >= this.minPointNum) {
					// 如果是中心点，让它密度直达的点也加入进来
					for(String k:wordMap.get(word)) {
						if(pointIn.containsKey(k) && pointIn.get(k) != 0) {
							// 若这个被考察的中心点的这个直达点有了别的标记
							// 那么这个直达点要根据它周围所有中心的标记来决定加入哪个
							// 加入TF最小的那个，或者说IDF最大的那个
							int max = 0;
							String joinWhich=null;
							for(String kk: wordMap.get(k)) {
								if(wordMap.get(kk).size() >= this.minPointNum
									&& pointIn.containsKey(kk)) {
									if(tf.containsKey(kk+","+k)) {
										if(tf.get(kk+","+k) > max) {
											max = tf.get(kk+","+k);
											joinWhich = kk;
										}
									}
									else if(tf.containsKey(k+","+kk)) {
										if(tf.get(k+","+kk) > max) {
											max = tf.get(k+","+kk);
											joinWhich = kk;
										}
									}
								}
							}
							pointIn.put(k, pointIn.get(joinWhich));
							continue;
						}
						pointIn.put(k, pointIn.get(word));
					}
				}
				else {
					// 不是中心点，已经被加入了
					// 这种情况，可以考虑是否决定一下有更近的聚类中心
					// 也可以do nothing
				}
			}
			else { // 被标为了噪声
				// 这种情况，既可以找个最近的中心点，又可以do nothing
			}
			
			line = this.br.readLine();
		}
		
		this.br.close();
	}
	
	public DBSCAN() throws Exception {
		this.setMinPointNum(3);
		this.initialize(3000); // parameter is 2TF's repeat times
		this.run_parallel(1000); // parameter is 1TF's repeat times
		// this.run_recursion(); no, it's not a good idea
		// because it prefers the firest cluster, always.
		for(String word: pointIn.keySet()) {
			Integer label = pointIn.get(word);
			if(!this.clusterMap.containsKey(label))
				this.clusterMap.put(label, new HashSet<String>());
			this.clusterMap.get(label).add(word);
		}
		System.out.println("Done");
	}
	
	public static void main(String[] args) throws Exception {
		new DBSCAN();
	}
	
}

	
