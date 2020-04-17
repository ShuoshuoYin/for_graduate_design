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
			this.radius = 1/(double)100; // ��������Խ�࣬����ԽС���뾶�����⣬����ʹ��
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
		while(line != null && !line.equals("")) { // ÿ��ѭ�����Ǹ��һ��cluster��
			word = line.split("\t")[0];
			if(pointIn.containsKey(word)) { 
				// ����������Ѿ�������cluster
				line = this.br.readLine();
				continue;
			}
			if(!wordMap.containsKey(word)
					|| wordMap.get(word).size() < this.minPointNum) {
				// Ҫô��Ȼnoise, labeling 0
				// Ҫô�Ժ��Ϊborder point�����г�Ϊborder point�Ŀ���
				pointIn.put(word, 0);  
				line = this.br.readLine();
				continue;
			}
				
			// ������ڵ���minPointNumֵ����ô��ʼһ�ξ���
			label ++;
			q.offer(word);
			String w = null;
			while(!q.isEmpty()) {
				w = q.poll();
				pointIn.put(w, label); // �������������ξ����漰���Ķ�����
				// �������Ⱦ�����Ǹ���ƫ��
				// ��Ϊʲô���ǵ�һ����������Զ���ڱ�ľ��ࣿ��֪���ˣ�
				// ������Ϊ�ܶ�ֱ�������Ȱ����е����ĵ������
				// ���Ǿͳʷ���״�����ˣ������Ӹ�Ȩ�ذɣ�������ΧԽ��Խ������
				// ���У����ܵݹ飬Ҫ����ؼ����۴�һ���
				if(wordMap.get(w).size() >= this.minPointNum) {
					for(String k:wordMap.get(w)) {
						if(!pointIn.containsKey(k) 
							|| pointIn.get(k) != label)
							q.offer(k);
					}
				}
			}
			System.out.println("һ���۴�   "+label);
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
			if(!pointIn.containsKey(word)) { // û�м����κξ���
				if(wordMap.containsKey(word) 
					&& wordMap.get(word).size() >= this.minPointNum) {
					label++;   // ���һ��Ǹ����ĵ㣬��ô�����label����ֱ��ĵ��������
					pointIn.put(word, label);
					for(String k:wordMap.get(word)) {
						// ���������������ĵ�����ֱ������˱�ı��
						// ��ô���ֱ���Ҫ��������Χ�������ĵı�������������ĸ�
						// ����TF��С���Ǹ�������˵IDF�����Ǹ�
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
				else { // Ҫô�������ĵ㣬Ҫôradius��ֱ�Ӿ�û��ֱ��ĵ�
					pointIn.put(word, 0); // ����ʲô������ȱ�Ϊnoise������û������
				}
			}
			else if(pointIn.get(word) != 0){ // ������ĳ�����࣬�����Ǳ�Ϊ�����Ǹ���
				if(wordMap.get(word).size() >= this.minPointNum) {
					// ��������ĵ㣬�����ܶ�ֱ��ĵ�Ҳ�������
					for(String k:wordMap.get(word)) {
						if(pointIn.containsKey(k) && pointIn.get(k) != 0) {
							// ���������������ĵ�����ֱ������˱�ı��
							// ��ô���ֱ���Ҫ��������Χ�������ĵı�������������ĸ�
							// ����TF��С���Ǹ�������˵IDF�����Ǹ�
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
					// �������ĵ㣬�Ѿ���������
					// ������������Կ����Ƿ����һ���и����ľ�������
					// Ҳ����do nothing
				}
			}
			else { // ����Ϊ������
				// ����������ȿ����Ҹ���������ĵ㣬�ֿ���do nothing
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

	
