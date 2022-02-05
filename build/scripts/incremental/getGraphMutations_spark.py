import sys

from pyspark.sql import SparkSession


def get_vertex_mutations(line, b_windows, partitions, workers):
    windows = b_windows.value
    num_windows = len(windows) - 1

    x = line.strip().split(" ")
    vid = x[0]
    v_start = int(x[1])
    v_end = int(x[2])

    mutations = []
    start_window = -1
    end_window = -1

    for i in range(1, num_windows + 1):
        w_start = windows[i - 1]
        w_end = windows[i]

        if w_start <= v_start < w_end:
            start_window = i

        if w_start < v_end <= w_end:
            end_window = i

    w = (int(vid) % partitions) % workers
    s_key = (start_window - 1) * workers + w
    e_key = (end_window - 1) * workers + w
    mutations.append(((s_key, int(vid)), (v_start, -1)))
    mutations.append(((e_key, int(vid)), (v_end, 0)))

    # if end_window != num_windows:
    #     r_key = end_window*workers + w
    #     mutations.append(((r_key, int(vid)), (v_end, 1)))
    if start_window != 1:  # for reverse (LD)
        r_key = (start_window - 2) * workers + w
        mutations.append(((r_key, int(vid)), (v_start, 1)))
    return mutations


def get_edges(line):
    x = line.strip().split(" ")
    vid = int(x[0])

    edges = []
    for e_index in range(3, len(x), 3):
        did = int(x[e_index])
        e_start = int(x[e_index + 1])
        e_end = int(x[e_index + 2])
        edges.append((vid, (did, (e_start, e_end))))
    return edges


def get_edge_mutations(edge, b_windows, partitions, workers):
    windows = b_windows.value
    num_windows = len(windows) - 1

    vid = edge[0]
    did = edge[1][0]
    e_start = edge[1][1][0]
    e_end = edge[1][1][1]

    w = (vid % partitions) % workers
    mutations = []

    for i in range(1, num_windows + 1):
        w_start = windows[i - 1]
        w_end = windows[i]

        if w_start >= e_end:
            break

        if e_start < w_end and e_end > w_start:
            s = max(w_start, e_start)
            e = min(w_end, e_end)
            e_key = (i - 1) * workers + w
            mutations.append(((e_key, vid), (did, (s, e))))
    return mutations


def to_string(mutation_record, workers, num_windows):
    key = mutation_record[0][0]
    vid = mutation_record[0][1]

    local_vertex_mutations = []
    edge_mutation_present = mutation_record[1][1] is not None
    if mutation_record[1][0] is not None:
        local_vertex_mutations = list(mutation_record[1][0])

    if len(local_vertex_mutations) > 0 and local_vertex_mutations[0][1] == 1:
        return "REMOVE " + str(vid)

    record_string = ""
    if len(local_vertex_mutations) > 0:
        if len(local_vertex_mutations) == 1:
            vertex_add = local_vertex_mutations[0][0] if (local_vertex_mutations[0][1] == -1) else -1
            vertex_remove = local_vertex_mutations[0][0] if (local_vertex_mutations[0][1] == 0) else -1
        else:
            vertex_add = local_vertex_mutations[0][0] if (local_vertex_mutations[0][1] == -1) \
                else local_vertex_mutations[1][0]
            vertex_remove = local_vertex_mutations[0][0] if (local_vertex_mutations[0][1] == 0) \
                else local_vertex_mutations[1][0]

        # for reverse (LD)
        tmp = vertex_add
        vertex_add = vertex_remove
        vertex_remove = tmp

        if vertex_add >= 0:
            # if key >= workers:
            if key < (num_windows - 1) * workers:  # for reverse (LD)
                record_string = "ADD "
            # record_string += (str(vid) + " " + str(vertex_add)) # comment for reverse (LD)

            if vertex_remove > 0:
                record_string += (str(vid) + " " + str(vertex_remove) + "/" + str(vertex_add))
                # record_string += ("/" + str(vertex_remove)) # comment for reverse (LD)
            else:  # for reverse (LD)
                record_string += (str(vid) + " " + str(vertex_add))
        else:
            record_string = "TRUNCATE " + str(vid) + " " + str(vertex_remove)
    else:
        record_string = "EDGE " + str(vid)

    if edge_mutation_present:
        edge_string = ""
        for element in mutation_record[1][1]:
            edge_string += (" " + str(element[0]) + " " + str(element[1][0]) + " " + str(element[1][1]))
        record_string += edge_string
    return record_string


### SPARK
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("GraphMutations") \
        .getOrCreate()

    graph_windows = [0, 20, 30, 40]
    sc = spark.sparkContext
    window_broadcast = sc.broadcast(graph_windows)

    graph_workers = 1
    graph_partitions = 3
    graph_num_windows = len(graph_windows) - 1

    # dummy = sc.parallelize([((0, -1), (0, -1))])
    dummy = sc.parallelize([(((graph_num_windows-1)*graph_workers, -1), (36, 0))])  # for reverse (LD)

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    vertex_mutations = lines.flatMap(lambda r: get_vertex_mutations(r, window_broadcast, graph_partitions, graph_workers))
    vertex_mutations = vertex_mutations.union(dummy)
    vertex_mutations = vertex_mutations.groupByKey()

    graph_edges = lines.flatMap(lambda r: get_edges(r))
    edge_mutations = graph_edges.flatMap(lambda r: get_edge_mutations(r, window_broadcast, graph_partitions, graph_workers))
    edge_mutations = edge_mutations.groupByKey()

    graph_mutations = vertex_mutations.fullOuterJoin(edge_mutations)
    graph_mutations = graph_mutations.partitionBy(graph_workers * graph_num_windows, lambda k: k[0])
    graph_mutations_string = graph_mutations.map(lambda r: to_string(r, graph_workers, graph_num_windows))
    graph_mutations_string.saveAsTextFile(sys.argv[2])
