from sklearn.cluster import DBSCAN
from gensim.models import Word2Vec
from pathlib import Path
import os
from collections import Counter


# Get the current directory
current_dir = os.getcwd()
# Get the parent directory
parent_dir = os.path.dirname(current_dir)
# Specify the file name or relative path from the parent directory
file_name = 'word2vec.model'
# Construct the file path
file_path = os.path.join(parent_dir, file_name)

word_vectors = Word2Vec.load(file_path).wv

DBSCAN_cluster = DBSCAN(eps=0.3, min_samples=10)
labels = DBSCAN_cluster.fit_predict(word_vectors.vectors.astype('double'))

# Print the cluster labels
print(labels)
print(DBSCAN_cluster.labels_)
print(set(DBSCAN_cluster.labels_))
print(Counter(DBSCAN_cluster.labels_))