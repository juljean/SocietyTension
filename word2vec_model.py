from time import time

import numpy as np
from gensim.models import Word2Vec
import multiprocessing
from sklearn.manifold import TSNE
from db_connection import db_communicator


w2v_model = Word2Vec(min_count=15,
                     window=4,
                     sample=1e-5,
                     alpha=0.1,
                     epochs=7,
                     workers=multiprocessing.cpu_count()-1)

start = time()

training_data = db_communicator.get_train_data()
# List of [comment_id, comment] format is a train data format
training_data_purified = [training_data[i][1].split() for i in range(len(training_data))]
w2v_model.build_vocab(training_data_purified)


def reduce_dimensions(model):
    num_components = 2  # number of dimensions to keep after compression

    # extract vocabulary from model and vectors in order to associate them in the graph
    vectors = np.asarray(model.wv.vectors)
    labels = np.asarray(model.wv.index_to_key)

    # apply TSNE
    tsne = TSNE(n_components=num_components, random_state=0)
    vectors = tsne.fit_transform(vectors)

    x_vals = [v[0] for v in vectors]
    y_vals = [v[1] for v in vectors]
    return x_vals, y_vals, labels


def plot_embeddings(x_vals, y_vals, labels):
    import plotly.graph_objs as go
    fig = go.Figure()
    trace = go.Scatter(x=x_vals, y=y_vals, mode='markers', text=labels)
    fig.add_trace(trace)
    fig.update_layout(title="Word2Vec - Visualizzazione embedding con TSNE")
    fig.show()
    return fig


print('Time to build vocab: {} mins'.format(round((time() - start) / 60, 2)))

start = time()

w2v_model.train(training_data_purified, total_examples=w2v_model.corpus_count, epochs=30, report_delay=1)

print('Time to train the model: {} mins'.format(round((time() - start) / 60, 2)))

w2v_model.init_sims(replace=True)
print(f"Топ 3 контекстуально-схожих слова на \"Путін\" {w2v_model.wv.most_similar(positive=['путін'], topn=3)}")
print(f"Топ 3 контекстуально-схожих слова на \"Зеленський\" {w2v_model.wv.most_similar(positive=['зеленський'], topn=3)}")

x_vals, y_vals, labels = reduce_dimensions(w2v_model)

plot = plot_embeddings(x_vals, y_vals, labels)

w2v_model.save("word2vec.model")
