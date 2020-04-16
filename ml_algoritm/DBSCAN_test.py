# coding: gbk
"""
记录一下中间文件，或者是中间载体，
{word: [True, id]}
单词： [core point与否, 所属聚簇id或者0]
"""

f1 = open("frequent_set2_filtered200.txt", 'r')

# word_dict = {word: [num, set()]}
# num 为关联了多少个其他点，set是关联的词的集合
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

# 真正开始运行DBSCAN算法
# 先来后到，先加入哪个聚簇就是属于哪个聚簇，不能被抢
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
        print("完成"+str(line_num)+"行")
    '''
    debugging end
    '''

    word = line.split("\t")[0]
    if word not in word_dict: # 根本没有跟它距离能小于阈值的，绝对噪声
        point[word] = [False, 0]
        line = f2.readline()
        continue
    if word_dict[word][0] < 1:
        if word not in point: # 可能以后会由于某个中心加入某个聚簇，但现在先标noise, or 可能以后找不到
            point[word] = [False, 0] # 如果已经加入了某个聚簇，不能动它
        line = f2.readline()
        continue
                                # 对于邻域有点且数量大于等于3的，能够称为聚类中心 or 中心点
    if word not in point:       # 那么要么它没被考察过，是绝对新的一个聚簇
        i += 1
        point[word] = [True, i]
    else:                       # 要么它已经被考察过，已经是某个cluster的边界点border point
        point[word][0] = True
                                # 而这两种情况不论哪种，都需要把它邻域的所有点加入进来
    for neighbor in word_dict[word][1]:   
        if neighbor not in point: # 邻域的这个点是小白，没加入过任何cluster
            point[neighbor] = [False, point[word][1]]
        elif point[neighbor][1] == 0: # 以前被考察过，但是被labeled了noise
            point[neighbor][1] = point[word][1]
        else:
            pass # 以前被考察过，还加入了某个聚簇，那就不能动
    line = f2.readline()
f2.close()

# 对所有的聚类中心进行再次考察，合并所有label不同但是能够找到聚类中心使它们密度相连的density connected
# 不然就过拟合了，分出50多个cluster，每个簇的样本点基本都是一两个词

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



# 聚类已经完成了，聚类的信息全在point字典里，下面是输出信息的

f3 = open("clusters.txt", 'w')
label = 0
while point != {}:
    '''
    debugging start
    '''
    print(str(label)+"个类别被输出")
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


