import re
import sys
import os
import threading
import heapq
import heapdict


if len(sys.argv) != 5:
    print("Usage: python assignment-1-19CS30008.py <path to data directory> <# threads> <value of n for n-grams> <value of k>")
    exit(1)
data_path = sys.argv[1]
num_threads = int(sys.argv[2])
n = int(sys.argv[3])
k = int(sys.argv[4])
# print(data_path, num_threads, n, k)

classes = []
num_docs = {}
files = []
for dir in os.listdir(data_path):
    if os.path.isdir(os.path.join(data_path, dir)):
        classes.append(dir)
        curr_files = os.listdir(os.path.join(data_path, dir))
        num_docs[dir] = len(curr_files)
        for file in curr_files:
            files.append(os.path.join(data_path, dir, file))

# print(classes)
# print(num_docs)
# print(len(classes))
# print(len(files))

ngram_dict = {}
lock = {}
for cls in classes:
    ngram_dict[cls] = {}
    lock[cls] = threading.Lock()


def get_ngrams(words, n):
    ngrams = []
    for i in range(len(words) - n + 1):
        ngrams.append(tuple(words[i: i + n]))
    return ngrams


def task(id, start, end):
    print(f"Thread {id} started")
    for i in range(start, end + 1):
        file = files[i]
        with open(file, 'r', encoding='utf-8', errors='replace') as f:
            file_str = f.read()
            words = re.findall(r'[a-zA-Z0-9]+', file_str)
            words = [word.lower() for word in words]
            ngrams = get_ngrams(words, n)
            # print(ngrams)
            cls = os.path.basename(os.path.dirname(
                os.path.join(data_path, file)))
            for ngram in ngrams:
                # if ngram[0] == 'ax' and ngram[1] == 'ax' and ngram[2] == 'ax' and ngram[3] == 'ax' and ngram[4] == 'ax':
                #     print(cls)
                #     print(file)
                lock[cls].acquire()
                if ngram in ngram_dict[cls]:
                    ngram_dict[cls][ngram] += 1
                else:
                    ngram_dict[cls][ngram] = 1
                lock[cls].release()
    print(f"Thread {id} finished")


threads = []
chunk_size = len(files) // num_threads
for i in range(num_threads):
    start = i * chunk_size
    end = (i + 1) * chunk_size - 1
    # print(start, end)
    t = threading.Thread(target=task, args=(i + 1, start, end))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

for key, value in ngram_dict.items():
    print(key, len(value))
    # print(value)

# score = count of ngram in a class / no. of documents in the class

# (26.988)
ngram_scores = {}
ngram_class = {}
for key, value in ngram_dict.items():
    for ngram, count in value.items():
        if ngram in ngram_scores:
            if ngram_scores[ngram] < count / num_docs[key]:
                ngram_class[ngram] = key
                ngram_scores[ngram] = count / num_docs[key]
            # ngram_scores[ngram] = max(ngram_scores[ngram], count / num_docs[key])
        else:
            ngram_class[ngram] = key
            ngram_scores[ngram] = count / num_docs[key]

# # get top k ngrams
# top_k_ngrams = sorted(ngram_scores.items(),
#                       key=lambda x: x[1], reverse=True)[:k]


# using heapq (16.987)
ngs = [(-score, ngram) for ngram, score in ngram_scores.items()]
top_k_ngrams = heapq.nsmallest(k, ngs)

top_k_cls = []
for score, ngram in top_k_ngrams:
    top_k_cls.append(ngram_class[ngram])
# for ngram, score in top_k_ngrams:
#     top_k_cls.append(ngram_class[ngram])
print(top_k_ngrams)
print(top_k_cls)
