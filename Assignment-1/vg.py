import sys
import os
import math
import threading
import re
import time

# Find all the directories in the given directory
def find_all_directories(directory_path):
    directories = []
    for root, dirs, files in os.walk(directory_path):
        for dir in dirs:
            directories.append(os.path.join(root, dir))
    return directories


# Find all n-grams in a given file
# n: n-gram size
#  words-delimiter: any non alphanumeric character
def find_n_grams(file_path, n):
    n_grams = []
    # find all words in the file delimited by any non alphanumeric character, ignore any non-unicode characters
    with open(file_path, 'rb') as file:
        words = re.split(r'\W+', file.read().decode('utf-8', 'ignore'))
    # find all n-grams
    for i in range(len(words) - n + 1):
        n_grams.append(' '.join(words[i: i + n]))
    # for i in range(len(words) - n + 1):
    #     n_gram = ''
    #     for j in range(n):
    #         n_gram += words[i+j] + ' '
    #     n_grams.append(n_gram.strip())
    return n_grams
    

# function that a thread runs to caluclate all n-grams in a document, takes doc name and directory as input
def thread_function(start):
    global docs
    global directory_names
    global dictionary_n_grams
    global directory_locks
    global n
    global k
    global docs_per_thread
    for i in range(start, start + docs_per_thread):
        if i >= len(docs):
            break
        file_name = docs[i][0]
        directory = docs[i][1]
        directory_name = docs[i][1].split('/')[-1]
        file_path = directory + '/' + file_name
        n_grams = find_n_grams(file_path, n)
        for n_gram in n_grams:
            directory_locks[directory_name].acquire()
            if n_gram in dictionary_n_grams[directory_name]:
                dictionary_n_grams[directory_name][n_gram] += 1
            else:
                dictionary_n_grams[directory_name][n_gram] = 1
            directory_locks[directory_name].release()

start_time = time.time()

# storing the command line arguments
# 1. data directory path
directory_path = sys.argv[1]

# 2. no. of threads
no_of_threads = int(sys.argv[2])

# 3. n value
n = int(sys.argv[3])

# 4. k value
k = int(sys.argv[4])

# Finding all the directories in the given directory
directories = find_all_directories(directory_path)

# Extracting the directory names from the path (for class labels)
directory_names = []
for directory in directories:
    directory_names.append(directory.split('/')[-1])

# No. of directories
no_of_directories = len(directories)

# Create a flat list of all the file {names, directory} in each directory    
docs = []
total_doc_count = 0

# dictionary of no. of documents in each directory
no_of_docs_in_directory = {}

for directory in directories:
    doc_count = 0
    for file in os.listdir(directory):
        total_doc_count += 1
        doc_count += 1
        docs.append([file, directory])
    # no of documents in each directory with key = directory name
    no_of_docs_in_directory[directory.split('/')[-1]] = doc_count

# print(files)
# print(len(files))

# No. of documents per thread rounded up
docs_per_thread = math.ceil(total_doc_count/no_of_threads)

# create a dictonary of no_of_directories dictionaries
# each dictionary will store the n-grams and their total frequency in all the files of that particular directory
dictionary_n_grams = {}
for directory in directory_names:
    dictionary_n_grams[directory] = {}   

# create no_of_directories lock objects, one for each directory
directory_locks = {}
for directory in directory_names:
    directory_locks[directory] = threading.Lock()

start = 0

# create no_of_threads threads
threads = []
for i in range(no_of_threads):
    threads.append(threading.Thread(target=thread_function, args=({start}   )))
    start += docs_per_thread
    # start the thread
    threads[i].start()

# wait for all the threads to finish
for i in range(no_of_threads):
    threads[i].join()

# create a dictionary of unique n-grams in all the files of each directory, the value will be {score, directory_name}
dictionary_unique_n_grams = {}
for directory in directory_names:
    for n_gram in dictionary_n_grams[directory]:
        if n_gram in dictionary_unique_n_grams:
            if dictionary_n_grams[directory][n_gram] / no_of_docs_in_directory[directory] > dictionary_unique_n_grams[n_gram][0]:
                dictionary_unique_n_grams[n_gram][0] = dictionary_n_grams[directory][n_gram] / no_of_docs_in_directory[directory]
                dictionary_unique_n_grams[n_gram][1] = [directory]
        else:
            dictionary_unique_n_grams[n_gram] = [dictionary_n_grams[directory][n_gram] / no_of_docs_in_directory[directory], [directory]]

#  sorted list of size k with values {score, n_gram, directory_name}
top_k_unique_n_grams = sorted(dictionary_unique_n_grams.items(), key=lambda x: x[1][0], reverse=True)[:k]

# print the top k unique n-grams
print('Top ', k, ' unique n-grams are:')
for n_gram in top_k_unique_n_grams:
    print(n_gram[0], ' ', n_gram[1][0], ' ', n_gram[1][1])

end_time = time.time()
print('Time taken: ', end_time - start_time)