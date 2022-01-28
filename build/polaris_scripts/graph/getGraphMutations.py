import random
import sys

from pyspark.sql import SparkSession


def get_mutations(line, workers):
    windows = [0, 10, 15, 20, 25, 36]
    num_windows = 5

    x = line.strip().split(" ")
    vid = x[0]
    v_start = x[1]
    v_end = x[2]

    mutations = [""]*num_windows
    start_window = -1
    end_window = -1

    for i in range(1, num_windows+1):
        w_start = windows[i-1]
        w_end = windows[i]

        if w_start <= int(v_start) < w_end:
            start_window = i
            if start_window != 1:
                mutations[i-1] = "ADD "
            mutations[i-1] += (vid + " " + v_start)

        if w_start < int(v_end) <= w_end:
            end_window = i
            if end_window == start_window:
                mutations[i-1] += ("/" + v_end)
            else:
                mutations[i-1] = ("TRUNCATE " + vid + " " + v_end)

    if end_window != num_windows:
        mutations[end_window] = "REMOVE " + vid

    for e_index in range(3, len(x), 3):
        eid = x[e_index]
        e_start = int(x[e_index + 1])
        e_end = int(x[e_index + 2])

        for w_index in range(start_window, end_window+1):
            w_start = windows[w_index-1]
            w_end = windows[w_index]

            if w_start >= e_end:
                break

            if e_start < w_end and e_end > w_start:
                s = max(w_start, e_start)
                e = min(w_end, e_end)

                if w_index != start_window and w_index != end_window:
                    if mutations[w_index-1] == "":
                        mutations[w_index-1] = "EDGE " + vid
                elif w_index == end_window:
                    if "EDGE" not in mutations[w_index-1]:
                        mutations[w_index-1] += ("\nEDGE " + vid)
                mutations[w_index - 1] += (" " + eid + " " + str(s) + " " + str(e))

    result = []
    w = random.randint(0, workers - 1)
    for index in range(len(mutations)):
        if mutations[index] != "":
            key = index*workers + w
            result.append((key, mutations[index]))
    return result


### SPARK
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GraphMutations")\
        .getOrCreate()

    sc = spark.sparkContext
    workers = 4
    num_windows = 5

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    mutations = lines.flatMap(lambda r: get_mutations(r, workers))
    dummy = sc.parallelize([(0, "-1 0")])
    mutations = mutations.union(dummy).partitionBy(workers * num_windows, lambda k: k).map(lambda r : r[1])

    # mutations.collect()
    mutations.saveAsTextFile("output")
