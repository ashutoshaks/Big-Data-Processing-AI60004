# Big Data Processing (AI60004)
# Assignment - 2
# Ashutosh Kumar Singh - 19CS30008

import sys
import random
import time


# Union-find data structure
class UnionFind:
    # When data[x] < 0, x is a root and -data[x] is its tree size. When data[x] >= 0, data[x] is x's parent.
    # This data structure is 1-indexed
    def __init__(self, n):
        self.data = [-1] * (n + 1)
        self.components = n

    def find(self, x):
        if self.data[x] < 0:
            return x
        self.data[x] = self.find(self.data[x])
        return self.data[x]

    def unite(self, x, y):
        x = self.find(x)
        y = self.find(y)
        if x == y:
            return False
        if -self.data[x] < -self.data[y]:
            x, y = y, x
        self.data[x] += self.data[y]
        self.data[y] = x
        self.components -= 1
        return True

    def get_size(self, x):
        return -self.data[self.find(x)]


class Graph:
    # Input_file has list of edges (space-separated pairs on each line)
    def __init__(self, input_file):
        self.edges = []
        self.n = 0      # number of nodes
        self.min_id = 1     # 0-indexed or 1-indexed

        # Read edges, input_file has list of edges (space-separated pairs on each line)
        with open(input_file, "r") as f:
            for line in f:
                e = line.split()
                self.edges.append((int(e[0]), int(e[1])))
                self.n = max(self.n, int(e[0]), int(e[1]))
                self.min_id = min(self.min_id, int(e[0]), int(e[1]))
        if self.min_id == 0:
            # make everything 1-indexed
            self.edges = [(e[0] + 1, e[1] + 1) for e in self.edges]
            self.n += 1
        self.groups = UnionFind(self.n)    # create union-find data structure

    # Karger's min-cut algo: Contract the graph until there are 2 components
    def contract(self):
        while self.groups.components > 2:
            e = random.choice(self.edges)
            if self.groups.find(e[0]) == self.groups.find(e[1]):
                continue
            self.groups.unite(e[0], e[1])

    # Return the cut size and the 2 subsets of nodes
    def get_min_cut(self):
        self.contract()
        cut_size = 0
        for e in self.edges:
            if self.groups.find(e[0]) != self.groups.find(e[1]):
                cut_size += 1
        pars = [i for i in range(1, self.n + 1) if self.groups.data[i] < 0]
        g1 = [i for i in range(1, self.n + 1)
              if self.groups.find(i) == pars[0]]
        g2 = [i for i in range(1, self.n + 1)
              if self.groups.find(i) == pars[1]]
        return cut_size, g1, g2


if __name__ == "__main__":
    random.seed(time.time())
    if len(sys.argv) != 2:
        print("Usage: python3 assignment-2-19CS30008.py <input_file>")
        sys.exit(1)
    input_file = sys.argv[1]
    graph = Graph(input_file)
    cut_size, g1, g2 = graph.get_min_cut()
    print("Min-cut size:", cut_size)
    print("Communities:")
    g1.sort()
    g2.sort()
    for i in g1:
        print(i - (1 - graph.min_id), 1)
    for i in g2:
        print(i - (1 - graph.min_id), 2)
