from gensim.models import Word2Vec
import db_connection
from sklearn.manifold import TSNE
import numpy as np

training_data = db_connection.get_train_data()
# List of [comment_id, comment] format is a train data format
training_data_purified = [training_data[i][1].split() for i in range(len(training_data))]

model_cbow = Word2Vec(training_data_purified, min_count=10, window=5, sg=0)

model_skip_gram = Word2Vec(training_data_purified, min_count=10, window=5, sg=1)


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


x_vals, y_vals, labels = reduce_dimensions(model_cbow)

plot1 = plot_embeddings(x_vals, y_vals, labels)

x_vals, y_vals, labels = reduce_dimensions(model_skip_gram)
plot2 = plot_embeddings(x_vals, y_vals, labels)
