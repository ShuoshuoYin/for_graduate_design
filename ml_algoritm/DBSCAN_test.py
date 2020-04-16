# coding: gbk
"""
��¼һ���м��ļ����������м����壬
{word: [True, id]}
���ʣ� [core point���, �����۴�id����0]
"""

f1 = open("frequent_set2_filtered200.txt", 'r')

# word_dict = {word: [num, set()]}
# num Ϊ�����˶��ٸ������㣬set�ǹ����Ĵʵļ���
word_dict = {}
line = f1.readline()
while line != "" and line != "\n":
    words = line.split("\t")[0]
    word1, word2 = words.split(",")
    if word1 not in word_dict:
        word_dict[word1] = [1, set()]
    else:
        word_dict[word1][0] += 1
    word_dict[word1][1].add(word2)
    if word2 not in word_dict:
        word_dict[word2] = [1, set()]
    else:
        word_dict[word2][0] += 1
    word_dict[word2][1].add(word1)

    line = f1.readline()
f1.close()

# ������ʼ����DBSCAN�㷨
# �����󵽣��ȼ����ĸ��۴ؾ��������ĸ��۴أ����ܱ���
f2 = open("tf_50filtered.txt", 'r')
point = {}
line = f2.readline()
i = 0
line_num = 0
while line != "" and line != "\n":
    '''
    debugging start
    '''
    line_num += 1
    if line_num%100 == 0:
        print("���"+str(line_num)+"��")
    '''
    debugging end
    '''

    word = line.split("\t")[0]
    if word not in word_dict: # ����û�и���������С����ֵ�ģ���������
        point[word] = [False, 0]
        line = f2.readline()
        continue
    if word_dict[word][0] < 1:
        if word not in point: # �����Ժ������ĳ�����ļ���ĳ���۴أ��������ȱ�noise, or �����Ժ��Ҳ���
            point[word] = [False, 0] # ����Ѿ�������ĳ���۴أ����ܶ���
        line = f2.readline()
        continue
                                # ���������е����������ڵ���3�ģ��ܹ���Ϊ�������� or ���ĵ�
    if word not in point:       # ��ôҪô��û����������Ǿ����µ�һ���۴�
        i += 1
        point[word] = [True, i]
    else:                       # Ҫô���Ѿ�����������Ѿ���ĳ��cluster�ı߽��border point
        point[word][0] = True
                                # ������������������֣�����Ҫ������������е�������
    for neighbor in word_dict[word][1]:   
        if neighbor not in point: # ������������С�ף�û������κ�cluster
            point[neighbor] = [False, point[word][1]]
        elif point[neighbor][1] == 0: # ��ǰ������������Ǳ�labeled��noise
            point[neighbor][1] = point[word][1]
        else:
            pass # ��ǰ�����������������ĳ���۴أ��ǾͲ��ܶ�
    line = f2.readline()
f2.close()

# �����еľ������Ľ����ٴο��죬�ϲ�����label��ͬ�����ܹ��ҵ���������ʹ�����ܶ�������density connected
# ��Ȼ�͹�����ˣ��ֳ�50���cluster��ÿ���ص��������������һ������

"""
for word in point:
    if point[word][0] == False:
        continue
    for another_word in point:
        if word == another_word or point[another_word][0] == False \
            or point[another_word][1] == point[word][1]:
            continue
        if another_word in word_dict[word][1]:
            for one in point:
                if point[one][1] == point[another_word][1]:
                    point[one][1] = point[word][1]
"""



# �����Ѿ�����ˣ��������Ϣȫ��point�ֵ�������������Ϣ��

f3 = open("clusters.txt", 'w')
label = 0
while point != {}:
    '''
    debugging start
    '''
    print(str(label)+"��������")
    '''
    debugging end
    '''
    f3.write("\nCluster "+str(label)+"\n") if label != 0 else \
        f3.write("Noise\n")
    for_deleting = []
    for word in point: 
        if point[word][1] == label:
            for_deleting.append(word)
            f3.write(word+"\n")
    for word in for_deleting:
        point.pop(word)
    label += 1

f3.close()


