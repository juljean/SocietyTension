from gensim.models import Word2Vec
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples

word_vectors = Word2Vec.load("word2vec.model").wv
k_means_model = KMeans(n_clusters=10, max_iter=300, random_state=0, n_init='auto').fit(
    X=word_vectors.vectors.astype('double'))
print(word_vectors.similar_by_vector(k_means_model.cluster_centers_[1], topn=20, restrict_vocab=None))


def clusterization():
    """
    Assign each word sentiment score — negative or positive value (-1 or 1) based on the cluster to which they
    belong. To weigh this score I multiplied it by how close they were to their cluster (to weigh how potentially
    positive/negative they are). As the score that K-means algorithm outputs is distance from both clusters,
    to properly weigh them I multiplied them by the inverse of closeness score (divided sentiment score by closeness
    score)
    :return:
    """
    positive_cluster_index = 1
    words_clusters = pd.DataFrame(word_vectors.index_to_key)
    words_clusters.columns = ['words']
    words_clusters['vectors'] = words_clusters.words.apply(lambda x: word_vectors[f'{x}'])
    words_clusters['cluster'] = words_clusters.vectors.apply(lambda x: k_means_model.predict([np.array(x)]))
    words_clusters.cluster = words_clusters.cluster.apply(lambda x: x[0])

    words_clusters['cluster_value'] = [1 if i == positive_cluster_index else -1 for i in words_clusters.cluster]
    words_clusters['closeness_score'] = words_clusters.apply(lambda x: 1 / (k_means_model.transform([x.vectors]).min()),
                                                             axis=1)
    words_clusters['sentiment_coeff'] = words_clusters.closeness_score * words_clusters.cluster_value

    words_clusters[['words', 'sentiment_coeff']].to_csv('sentiment_results/sentiment_dictionary.csv', index=False)


def estimation():
    """
    Function for silhouette estimation, which calculates the silhouette coefficient and scatter-plot the cluster points
    :return:
    """
    # Set up the plotting frame
    fig, (ax1, ax2) = plt.subplots(1, 2)
    fig.set_size_inches(10, 5)

    # Run the Kmeans algorithm
    labels = k_means_model.fit_predict(X=word_vectors.vectors.astype('double'))
    centroids = k_means_model.cluster_centers_

    # Get silhouette samples
    silhouette_vals = silhouette_samples(word_vectors.vectors.astype('double'), labels)

    # Silhouette plot
    y_lower, y_upper = 0, 0
    for i, cluster in enumerate(np.unique(labels)):
        cluster_silhouette_vals = silhouette_vals[labels == cluster]
        cluster_silhouette_vals.sort()
        y_upper += len(cluster_silhouette_vals)
        ax1.barh(range(y_lower, y_upper), cluster_silhouette_vals, edgecolor='none', height=1)
        ax1.text(-0.03, (y_lower + y_upper) / 2, str(i + 1))
        y_lower += len(cluster_silhouette_vals)

    # Get the average silhouette score and plot it
    avg_score = np.mean(silhouette_vals)
    ax1.axvline(avg_score, linestyle='--', linewidth=2, color='green')
    ax1.set_yticks([])
    ax1.set_xlim([-0.1, 1])
    ax1.set_xlabel('Silhouette coefficient values')
    ax1.set_ylabel('Cluster labels')
    ax1.set_title('Silhouette plot for the various clusters', y=1.02)

    # Scatter plot of data colored with labels
    ax2.scatter(word_vectors.vectors.astype('double')[:, 0], word_vectors.vectors.astype('double')[:, 1], c=labels)
    ax2.scatter(centroids[:, 0], centroids[:, 1], marker='*', c='r', s=250)
    ax2.set_xlim([-0.3, 0.3])
    ax2.set_xlim([-0.3, 0.3])
    ax2.set_xlabel('Eruption time in mins')
    ax2.set_ylabel('Waiting time to next eruption')
    ax2.set_title('Visualization of clustered data', y=1.02)
    ax2.set_aspect('equal')
    plt.tight_layout()
    plt.show()


clusterization()