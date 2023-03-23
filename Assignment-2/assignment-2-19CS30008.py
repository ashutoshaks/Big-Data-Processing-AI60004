import sys
import random
import time

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
    # input_file has list of edges (space-separated pairs on each line)
    def __init__(self, input_file):
        self.edges = []
        self.n = 0
        with open(input_file, "r") as f:
            for line in f:
                # self.edges.append((int(line.split()[0]), int(line.split()[1])))
                e = line.split()
                # self.edges.append((int(e[0]) + 1, int(e[1]) + 1))
                self.edges.append((int(e[0]), int(e[1])))
                self.n = max(self.n, self.edges[-1][0], self.edges[-1][1])
        self.groups = UnionFind(self.n)

    def is_connected(self):
        # check using edgelist
        uf = UnionFind(self.n)
        for e in self.edges:
            uf.unite(e[0], e[1])
        return uf.components == 1

    def contract(self):
        while self.groups.components > 2:
            e = random.choice(self.edges)
            # print(e)
            if self.groups.find(e[0]) == self.groups.find(e[1]):
                continue
            # print(e)
            # assert self.groups.find(e[0]) != self.groups.find(e[1])
            self.groups.unite(e[0], e[1])
            # self.edges = list(filter(lambda e: self.groups.find(e[0]) != self.groups.find(e[1]), self.edges))
            # print(self.edges)
            # print(self.groups.components)
            # print(self.groups.data)

    def get_min_cut(self):
        self.contract()
        # return the cut size and the 2 subsets of nodes
        cut_size = 0
        cut_size = 0
        for e in self.edges:
            if self.groups.find(e[0]) != self.groups.find(e[1]):
                cut_size += 1
        pars = [i for i in range(1, self.n + 1) if self.groups.data[i] < 0]
        g1 = [i for i in range(1, self.n + 1) if self.groups.find(i) == pars[0]]
        g2 = [i for i in range(1, self.n + 1) if self.groups.find(i) == pars[1]]
        return cut_size, g1, g2

if __name__ == "__main__":
    # seed random
    random.seed(time.time())
    if len(sys.argv) != 2:
        print("Usage: python3 assignment-2-19CS30008.py <input_file>")
        sys.exit(1)
    input_file = sys.argv[1]
    graph = Graph(input_file)
    if not graph.is_connected():
        print("Graph is not connected")
        sys.exit(1)
    cut_size, g1, g2 = graph.get_min_cut()
    print("Cut size:", cut_size)
    print("Group 1:", g1)
    print("Group 2:", g2)