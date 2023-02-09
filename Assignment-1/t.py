import re
import sys
import os
import threading
import heapq
import time

t1 = time.time()

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

ngram_parts = [None] * num_threads


def get_ngrams(words, n):
    # ngrams = []
    # for i in range(len(words) - n + 1):
    #     ngrams.append(tuple(words[i: i + n]))
    if len(words) < n:
        return []
    ngrams = []

    curr_ngram = ""
    for i in range(n):
        curr_ngram += words[i] + " "
    ngrams.append(curr_ngram[:-1])
    for i in range(n, len(words)):
        prev_len = len(words[i - n])
        curr_ngram = curr_ngram[prev_len + 1:]
        curr_ngram += words[i] + " "
        ngrams.append(curr_ngram[:-1])
        
    return ngrams


def task(id, start, end):
    print(f"Thread {id + 1} started")
    ngram_parts[id] = {}
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
                if (ngram, cls) in ngram_parts[id]:
                    ngram_parts[id][(ngram, cls)] += 1
                else:
                    ngram_parts[id][(ngram, cls)] = 1
    print(f"Thread {id + 1} finished")


threads = []
chunk_size = len(files) // num_threads
for i in range(num_threads):
    start = i * chunk_size
    end = (i + 1) * chunk_size - 1
    # print(start, end)
    t = threading.Thread(target=task, args=(i, start, end))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

t2 = time.time()

ngram_cnts = {}
for i in range(num_threads):
    for key, value in ngram_parts[i].items():
        if key[0] in ngram_cnts:
            if key[1] in ngram_cnts[key[0]]:
                ngram_cnts[key[0]][key[1]] += value
            else:
                ngram_cnts[key[0]][key[1]] = value
        else:
            ngram_cnts[key[0]] = {}
            ngram_cnts[key[0]][key[1]] = value

t3 = time.time()


# score = count of ngram in a class / no. of documents in the class

# (26.988)
ngram_scores = {}
ngram_class = {}
for ngram, cnts in ngram_cnts.items():
    for cls, cnt in cnts.items():
        score = cnt / num_docs[cls]
        if ngram in ngram_scores:
            if score > ngram_scores[ngram]:
                ngram_scores[ngram] = score
                ngram_class[ngram] = cls
        else:
            ngram_scores[ngram] = score
            ngram_class[ngram] = cls

# # get top k ngrams
# top_k_ngrams = sorted(ngram_scores.items(),
#                       key=lambda x: x[1], reverse=True)[:k]

t4 = time.time()

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

t5 = time.time()

print(f"Time taken for reading files: {t2 - t1}")
print(f"Time taken for counting ngrams: {t3 - t2}")
print(f"Time taken for scoring ngrams: {t4 - t3}")
print(f"Time taken for getting top k ngrams: {t5 - t4}")