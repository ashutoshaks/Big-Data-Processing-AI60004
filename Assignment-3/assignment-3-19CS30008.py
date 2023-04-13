'''
pyspark code using RDDs to compute top k words with highest pointwise mutual information (PMI) with a given word in a set of documents. 
Remove stopwords which are given in a different file.
In another file, each line represents one document.
'''

from pyspark import SparkConf, SparkContext
import sys
import math


def filter_stopwords_unique(doc, stopwords):
    '''
    Filter stopwords from a document and return a list of unique words
    '''
    words = doc.split()
    uniq_words = set([word.lower() for word in words if word not in stopwords])
    return ' '.join(uniq_words)


def get_pairs(doc, query_word):
    '''
    Return a list of (word, query_word) pairs for each word in the document
    '''
    words = doc.split()
    cnt = 1 if query_word in words else 0
    return [((query_word, word), cnt) for word in words if word != query_word]


def pmi(co_count, w1_count, w2_count, docs_count):
    '''
    Compute the pointwise mutual information (PMI) for two words
    '''
    if co_count == 0:
        return -1e6
    return math.log2((co_count * docs_count) / (w1_count * w2_count))


if __name__ == "__main__":
    # Create Spark context with Spark configuration
    conf = SparkConf().setMaster("local").setAppName(
        "PMI top k and lowest k for a given word")
    sc = SparkContext(conf=conf)

    docs_path = sys.argv[1]
    query_word = sys.argv[2]
    k = int(sys.argv[3])
    stopwords_path = sys.argv[4]

    # Read the documents file
    docs_rdd = sc.textFile(docs_path)
    stopwords_rdd = sc.textFile(stopwords_path)
    stopwords = set(stopwords_rdd.collect())

    docs_rdd = docs_rdd.map(
        lambda doc: filter_stopwords_unique(doc, stopwords))
    words_rdd = docs_rdd.flatMap(lambda doc: doc.split())

    # Compute the number of documents
    docs_count = docs_rdd.count()

    word_count_rdd = words_rdd.map(lambda word: (
        word, 1)).reduceByKey(lambda x, y: x + y)

    # Documents containing the query word
    # docs_with_query_word = docs_rdd.filter(
    #     lambda doc: query_word in doc.split())

    word_pairs_rdd = docs_rdd.flatMap(
        lambda doc: get_pairs(doc, query_word))
    print(word_pairs_rdd.collect())
    word_pairs_counts_rdd = word_pairs_rdd.reduceByKey(lambda x, y: x + y)

    # ((w1, w2), (co_count, w1_count))
    word1_counts_rdd = word_pairs_counts_rdd.map(lambda x: (x[0][0], (x[0][1], x[1]))).join(
        word_count_rdd).map(lambda x: ((x[0], x[1][0][0]), (x[1][0][1], x[1][1])))
    # ((w1, w2), w2_count)
    word2_counts_rdd = word_pairs_counts_rdd.map(lambda x: (x[0][1], (x[0][0], x[1]))).join(
        word_count_rdd).map(lambda x: ((x[1][0][0], x[0]), x[1][1]))
    
    # print(word1_counts_rdd.collect())
    # print(word2_counts_rdd.collect())

    # (co, w1, w2)
    co_w1_w2_rdd = word1_counts_rdd.join(word2_counts_rdd).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1])))

    print(co_w1_w2_rdd.collect())

    # Compute PMI for each word pair
    pmi_rdd = co_w1_w2_rdd.map(lambda x: (x[0], pmi(x[1][0], x[1][1], x[1][2], docs_count)))
    print(pmi_rdd.collect())

    # get top k and lowest k words
    top_k = pmi_rdd.takeOrdered(k, key=lambda x: -x[1])
    bottom_k = pmi_rdd.takeOrdered(k, key=lambda x: x[1])

    print("Top k words with highest PMI with the query word: ", top_k)
    print("Bottom k words with lowest PMI with the query word: ", bottom_k)