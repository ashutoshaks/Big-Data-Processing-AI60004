from pyspark import SparkConf, SparkContext
import sys
import math
import re


def filter_stopwords_unique(doc, stopwords):
    words = doc.split()
    uniq_words = set([word.lower() for word in words if word not in stopwords])
    return ' '.join(uniq_words)


def get_pairs(doc, query_word):
    words = doc.split()
    cnt = 1 if query_word in words else 0
    return [((query_word, word), cnt) for word in words if word != query_word]


def pmi(co_count, w1_count, w2_count, docs_count):
    if co_count == 0:
        return -math.inf
    return math.log2((co_count * docs_count) / (w1_count * w2_count))


def main():
    conf = SparkConf().setMaster("local").setAppName(
        "PMI top k and lowest k for a given word")
    sc = SparkContext(conf=conf)

    if len(sys.argv) != 5:
        print("Usage: assignment-3-19CS30008.py <docs_path> <query_word> <k> <stopwords_path>")
        sys.exit(1)

    docs_path = sys.argv[1]
    query_word = sys.argv[2]
    k = int(sys.argv[3])
    stopwords_path = sys.argv[4]

    # Read the documents file
    docs_rdd = sc.textFile(docs_path)
    stopwords_rdd = sc.textFile(stopwords_path)
    stopwords = stopwords_rdd.collect()
    stopwords = set([word.lower() for word in stopwords])

    docs_rdd = docs_rdd.map(
        lambda doc: filter_stopwords_unique(doc, stopwords))
    words_rdd = docs_rdd.flatMap(lambda doc: doc.split()).filter(
        lambda word: re.match(r'^[a-zA-Z]+$', word))

    # Compute the number of documents
    docs_count = docs_rdd.count()

    word_count_rdd = words_rdd.map(lambda word: (
        word, 1)).reduceByKey(lambda x, y: x + y)

    word_pairs_rdd = docs_rdd.flatMap(
        lambda doc: get_pairs(doc, query_word))
    word_pairs_counts_rdd = word_pairs_rdd.reduceByKey(lambda x, y: x + y)

    # ((w1, w2), (co_count, w1_count))
    word1_counts_rdd = word_pairs_counts_rdd.map(lambda x: (x[0][0], (x[0][1], x[1]))).join(
        word_count_rdd).map(lambda x: ((x[0], x[1][0][0]), (x[1][0][1], x[1][1])))
    # ((w1, w2), w2_count)
    word2_counts_rdd = word_pairs_counts_rdd.map(lambda x: (x[0][1], (x[0][0], x[1]))).join(
        word_count_rdd).map(lambda x: ((x[1][0][0], x[0]), x[1][1]))

    # ((w1, w2), (co, w1, w2))
    co_w1_w2_rdd = word1_counts_rdd.join(word2_counts_rdd).map(
        lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1])))

    # Compute PMI for each word pair
    pmi_rdd = co_w1_w2_rdd.map(lambda x: (
        x[0], pmi(x[1][0], x[1][1], x[1][2], docs_count)))

    # Remove the entries with -inf
    pmi_rdd = pmi_rdd.filter(lambda x: x[1] != -math.inf)

    # Get top k and lowest k words
    top_k = pmi_rdd.takeOrdered(k, key=lambda x: -x[1])
    bottom_k = pmi_rdd.takeOrdered(k, key=lambda x: x[1])

    print()
    print("Top k words with highest (positive) PMI with the query word: ")
    for e in top_k:
        if e[1] > 0:
            print(e[0][1], e[1])
    print()

    print("Bottom k words with lowest (negative) PMI with the query word: ")
    for e in bottom_k:
        if e[1] < 0:
            print(e[0][1], e[1])
    print()


if __name__ == "__main__":
    main()
