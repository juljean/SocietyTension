from time import time
from gensim.models import Word2Vec
import multiprocessing
import db_connection
import stemmer

w2v_model = Word2Vec(min_count=3,
                     window=4,
                     sample=1e-5,
                     negative=20,
                     workers=multiprocessing.cpu_count()-1)

start = time()

training_data = db_connection.connect("get_train_data")
# List of [comment_id, comment] format is a train data format
training_data_purified = [training_data[i][0].split() for i in range(len(training_data))]
w2v_model.build_vocab(training_data_purified)

print('Time to build vocab: {} mins'.format(round((time() - start) / 60, 2)))

start = time()

w2v_model.train(training_data_purified, total_examples=w2v_model.corpus_count, epochs=30, report_delay=1)

print('Time to train the model: {} mins'.format(round((time() - start) / 60, 2)))

w2v_model.init_sims(replace=True)

w2v_model.save("word2vec.model")
