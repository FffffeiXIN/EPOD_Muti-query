import os
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import random

# top-down: first cluster based on node number, and mixture;
# Then for each mixed cluster, cluster based on client number, and mixture

# Configuration that need to be changed each time
n_nodes = 6
n_clients = 10
mix_rate_node = 0.05
mix_rate_client = 0.05
nW = 20
W = 5000
data = pd.read_table('/home/shimin/EPOD/Datasets/stock.txt', sep=',', header=None)
prefix = "/home/shimin/EPOD/Datasets/DeviceId_data/Node_" \
         + str(n_nodes) + "_Device_" + str(n_clients) + "_STK_" + str(mix_rate_node) + "/"
# data = pd.read_table('E:\\DBGroup\\Huan\\EPOD_Muti-query\\Datasets\\stock.txt', sep=',', header=None)
# prefix = "E:\\DBGroup\\Huan\\EPOD_Muti-query\\Datasets\\DeviceId_data\\Node_" \
#          + str(n_nodes) + "_Device_" + str(n_clients) + "_STK_" + str(mix_rate_node) + "\\"

# read datasets
df = pd.DataFrame(data)
X = np.array(df)

clusters2 = []
for i in range(n_nodes * n_clients):
    clusters2.append([])

while True:
    # step 1: cluster based on node number, and mixture;
    kmeans = KMeans(n_init='auto', n_clusters=2).fit(X)
    tmp0 = []
    for i in range(2):
        tmp0.append([])
    for i in range(kmeans.labels_.size):
        tmp0[kmeans.labels_[i]].append(X[i])
    for i in range(2):
        random.shuffle(tmp0[i])
    clusters_tmp = []
    tmp = tmp0[1]
    if len(tmp0[1]) < len(tmp0[0]):
        tmp = tmp0[0]
        clusters_tmp.append(tmp0[1])
    else:
        clusters_tmp.append(tmp0[0])
    kmeans = KMeans(n_init='auto', n_clusters=6).fit(tmp)
    for i in range(6):
        clusters_tmp.append([])
    for i in range(kmeans.labels_.size):
        clusters_tmp[kmeans.labels_[i] + 1].append(tmp[i])
    for i in range(len(clusters_tmp)):
        random.shuffle(clusters_tmp[i])
    flag = 0  # number of clusters that has less than 10 points
    for i in range(len(clusters_tmp)):
        if len(clusters_tmp[i]) < 10:
            flag = flag + 1
            if flag == 2:
                print("cannot generate enough clusters.")
                break
    if flag == 2:
        continue

    clusters = []
    for i in range(len(clusters_tmp)):
        if len(clusters_tmp[i]) >= 10:
            clusters.append(clusters_tmp[i])
    # mixture
    clusters = sorted(clusters, key=lambda y: len(y), reverse=True)
    for i in range(len(clusters)):
        print(len(clusters[i]))
    print("=====================================")
    group = []
    for i in range(n_nodes):
        count = int(len(clusters[i]) * mix_rate_node)
        for j in range(count):
            group.append(clusters[i][j])
    random.shuffle(group)

    # reassign
    clusters1 = []
    for i in range(n_nodes):
        clusters1.append([])

    left = 0
    right = int(len(group) / n_nodes)
    for i in range(n_nodes):
        # mixed part
        for j in range(left, right):
            clusters1[i].append(group[j])
        # original part
        count = int(len(clusters[i]) * mix_rate_node)
        for j in range(count + 1, len(clusters[i])):
            clusters1[i].append(clusters[i][j])
        left = right
        if i == n_nodes - 2:
            right = len(group)
        else:
            right = int(left + len(group) / n_nodes)

    for i in range(n_nodes):
        random.shuffle(clusters1[i])


    # step 2: for each node, cluster based on client number, and mixture
    for i in range(n_nodes):
        kmeans = KMeans(n_init='auto', n_clusters=n_clients).fit(clusters1[i])
        clusters = []
        for j in range(n_clients):
            clusters.append([])
        for j in range(kmeans.labels_.size):
            clusters[kmeans.labels_[j]].append(clusters1[i][j])
        for j in range(n_clients):
            random.shuffle(clusters[j])
        # mixture
        clusters = sorted(clusters, key=lambda y: len(y), reverse=True)
        for x in range(len(clusters)):
            print(len(clusters[x]))
        group = []
        for j in range(n_clients):
            count = int(len(clusters[j]) * mix_rate_client)
            for k in range(count):
                group.append(clusters[j][k])
        random.shuffle(group)

        # reassign

        left = 0
        right = int(len(group) / n_clients)
        for j in range(n_clients):
            # mixed part
            tmp = []
            for k in range(left, right):
                tmp.append(group[k])
            # original part
            count = int(len(clusters[j]) * mix_rate_client)
            for k in range(count + 1, len(clusters[j])):
                tmp.append(clusters[j][k])
            left = right
            if i == n_clients - 2:
                right = len(group)
            else:
                right = int(left + len(group) / n_clients)
            random.shuffle(tmp)
            for k in range(len(tmp)):
                clusters2[i * n_clients + j].append(tmp[k])

    # check: the smallest cluster has at least 10 window points - 10000 * 10
    print("=====================================")
    smallest = 100000000
    for i in range(len(clusters2)):
        if len(clusters2[i]) < smallest:
            smallest = len(clusters2[i])
#     if smallest >= W * nW:
    if smallest >= 20000:
        break
# print to file
try:
    os.mkdir(prefix, 0o0755)
except OSError:
    print()

files = []
for i in range(n_nodes * n_clients):
    file = open(prefix + str(i) + ".txt", "w")
    files.append(file)

for i in range(n_nodes * n_clients):
    for j in range(len(clusters2[i])):
        files[i].write(str(i) + ",")
        for k in range(len(clusters2[i][j])):
            if k == len(clusters2[i][j]) - 1:
                files[i].write(str(clusters2[i][j][k]))
            else:
                files[i].write(str(clusters2[i][j][k]) + ",")
        files[i].write("\n")

for i in range(n_nodes):
    files[i].close()
