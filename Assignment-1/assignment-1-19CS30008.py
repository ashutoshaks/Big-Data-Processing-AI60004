# Ashutosh Kumar Singh
# 19CS30008

import re
import sys
import os
import threading
import heapq
import math

if len(sys.argv) != 5:
    print('Usage: python assignment-1-19CS30008.py <path to data directory> <# threads> <value of n for n-grams> <value of k>')
    exit(1)
data_path = sys.argv[1]  # path to data directory
num_threads = int(sys.argv[2])  # number of threads
n = int(sys.argv[3])    # value of n for n-grams
k = int(sys.argv[4])    # value of k

if n < 1:
    print('Value of n should be greater than or equal to 1')
    exit(1)
if k < 1:
    print('Value of k should be greater than or equal to 1')
    exit(1)

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

ngram_dict = {}  # dictionary having an entry for each document class, each entry in itself is a dictionary storing n-grams and their corresponding counts
lock = {}
for cls in classes:
    ngram_dict[cls] = {}
    lock[cls] = threading.Lock()

# Returns n-grams for a given list of words
def get_ngrams(words, n):
    if len(words) < n:
        return []

    ngrams = []
    curr_ngram = ''
    for i in range(n):
        curr_ngram += words[i] + ' '
    ngrams.append(curr_ngram[:-1])
    for i in range(n, len(words)):
        prev_len = len(words[i - n])
        curr_ngram = curr_ngram[prev_len + 1:]
        curr_ngram += words[i] + ' '
        ngrams.append(curr_ngram[:-1])
    return ngrams

# Task to be performed by each thread
# For the files assigned to it, it reads the file, extracts words, gets n-grams and updates the n-gram dictionary
def task(id, start, end):
    for i in range(start, end + 1):
        file = files[i]
        with open(file, 'r', encoding='utf-8', errors='replace') as f:
            file_str = f.read()
            words = re.findall(r'[a-zA-Z0-9]+', file_str)
            words = [word.lower() for word in words]
            ngrams = get_ngrams(words, n)
            cls = os.path.basename(os.path.dirname(file))
            for ngram in ngrams:
                lock[cls].acquire()
                if ngram in ngram_dict[cls]:
                    ngram_dict[cls][ngram] += 1
                else:
                    ngram_dict[cls][ngram] = 1
                lock[cls].release()

threads = []
chunk_size = int(math.ceil(len(files) / num_threads))
if chunk_size == 0:
    chunk_size = 1

print('Threads started')

for i in range(num_threads):
    start = min(i * chunk_size, len(files))
    end = min((i + 1) * chunk_size - 1, len(files) - 1)
    t = threading.Thread(target=task, args=(i + 1, start, end))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print('Threads finished')

ngram_scores = {}   # dictionary storing n-grams and their corresponding scores
ngram_class = {}    # dictionary storing n-grams and their corresponding class with the highest score
for key, value in ngram_dict.items():
    for ngram, count in value.items():
        if ngram in ngram_scores:
            if ngram_scores[ngram] < count / num_docs[key]:
                ngram_class[ngram] = key
                ngram_scores[ngram] = count / num_docs[key]
        else:
            ngram_class[ngram] = key
            ngram_scores[ngram] = count / num_docs[key]

# Get the top k n-grams using a heap
ngs = [(-score, ngram) for ngram, score in ngram_scores.items()]
top_k_ngrams = heapq.nsmallest(k, ngs)

print(f'\nTop {k} n-grams (n = {n}):')
print('(n-gram, score, class)')
for i, (score, ngram) in enumerate(top_k_ngrams):
    print(f'{i + 1}. ({ngram}, {round(-score, 4)}, {ngram_class[ngram]})')
