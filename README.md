# Incremental Temporal Algorithms using Windowed ICM

Windowed ICM can also be used for incremental computations of temporal algorithms.
No changes in the WICM computation pipeline are required. In terms of code, `DebugWICMMutationsIntervalComputation` extends `DebugDeferredWindowIntervalComputation`. Incremental temporal algorithms build on `DebugWICMMutationsIntervalComputation`. Reading mutation files is coordinated with the help of `WICMMutationsWorkerContext`. All the mutation related code is present in `src/graph/mutations/` and `src/io/mutations`.

---
## 1. Installing Graphite ICM

Pre-requisites:
 * A Linux Ubuntu-based system (VM or bare-metal) with >= 8GB RAM
 * Java JDK 8
 * Maven >= 3.6.0

 1. First setup Hadoop 3.1.1 on the system with HDFS and YARN. Instructions for this are in [`HadoopSetup.md`](https://github.com/dream-lab/IncrementalWICM/blob/main/HadoopSetup.md).
 2. Next, we install Graphite ICM jars, which is an extension of Apache Giraph. A single jar with all the dependencies is present under [`jars/`](https://github.com/dream-lab/IncrementalWICM/tree/main/jars). To install ICM using maven:
```
cd jars
bash ./install.sh
```

Hadoop services should start on successful Hadoop/HDFS/YARN setup. Please see [`HadoopSetup.md`](https://github.com/dream-lab/IncrementalWICM/blob/main/HadoopSetup.md) for details.
Successful installation of ICM will result in creation of `org/apache/giraph` under `~/.m2/repository`.


---
## 2. Building WICM from Source

Our WICM source code is present under [`src/`](https://github.com/dream-lab/IncrementalWICM/tree/main/src/in/dreamlab/wicm). To build the project, run the `make.sh` script in the [`build/`](https://github.com/dream-lab/IncrementalWICM/tree/main/build) folder.

```
cd build
bash ./make.sh
```

`WICM-1.0-SNAPSHOT-jar-with-dependencies.jar` will be created at the end of the build script under `build/`.

---
## 3. Running a Graphite ICM job

This evaluates the basline ICM platform that is used for comparison in our paper.

With Graphite ICM and Hadoop deployed, you can run your first ICM temporal graph processing job. We will use the **Earliest Arrival Time (EAT)** algorithm from the EuroSys paper for this example. The job reads an input file of an interval graph in one of the supported formats and computes the earliest arrival path from a provided source node. We will use `IntIntNullTextInputFormat` input format, which indicates that the vertex ID is of type `Int`, the time dimension is of type `Int`, with no (`Null`) edge properties, and `Text` implies that the input graph file is in text format. 

A sample graph [`sampleGraph.txt`](https://github.com/dream-lab/wicm/blob/IncrementalWICM/build/graphs/sampleGraph.txt) has been provided in `build/graphs` with ~30,000 nodes ~1,000,000 edges. The spatial topology of the graph was generated using [`PaRMAT`](https://github.com/farkhor/PaRMAT). The start-time and end-time of interval edges are uniformly sampled. The lifespan of the vertex is set accordingly to maintain referential integrity in the graph.

Each line is an adjacency list of one source and one or more sink vertices of the format `source_id source_startTime source_endTime dest1_id dest1_startTime dest1_endTime dest2_id dest2_startTime dest2_endTime ...`. To run the `EAT` algorithm, the Giraph job script `runEAT.sh` has been provided in [`build/scripts/giraph/icm`](https://github.com/dream-lab/wicm/tree/IncrementalWICM/build/scripts/giraph/icm). The job script takes 4 arguments:

 1. source : The source vertex ID from which the traversal algorithm will start (e.g., `0`)
 2. perfFlag : Set to `true` to dump performance related log information, `false` otherwise (e.g., `false`)
 3. inputGraph : HDFS path to the input graph (e.g., `sampleGraph.txt`)
 4. outputDir : HDFS path to the output folder (e.g., `output`)

```
runEAT.sh <source> <perfFlag> <inputGraph> <outputDir>
```

To run the script, first copy the sample graph file to HDFS:
```
hdfs dfs -copyFromLocal build/graphs/sampleGraph.txt
```
And check if the input graph has been copied to HDFS:
```
hdfs dfs -ls sampleGraph.txt
```

Running ICM mode job with sourceID as `0`:
```
cd build
bash ./scripts/giraph/icm/runEAT.sh 0 false sampleGraph.txt output
```
`output` should be present under `build/` after successful finishing of the job.

---
## 4. Running WICM Incremental Job

This evaluates the Windowed ICM in incremental version, coupled with the Local Unrolling (LU) and Deferred Scatter (DS) optimizations.

The related scripts are provided in [`build/scripts/giraph/wicmi`](https://github.com/dream-lab/wicm/tree/IncrementalWICM/build/scripts/giraph/wicmi). The scripts have additional arguments compared to the ICM script:

5. lowerE : Start time of the graph lifespan (e.g., `0`)
6. upperE : End time of the graph lifespan (e.g., `40`)
7. windows : Temporal partitioning of the graph's lifespan, specified as timepoint boundaries separated by semicolon (e.g., `0;20;30;40`)
8. mPath : Path from where the mutations will be read (e.g., `sampleGraphMutation/window`)
9. last : last timepoint in the overall temporal graph (e.g., `40`)
```
runEAT.sh <source> <inputGraph> <outputDir> <lowerE> <upperE> <windows> <mPath> <last>
```

Mutations for the sample graph have been provided in `sampleGraphMutations` in `build/graphs`. PySpark code `getGraphMutations_spark.py` has been provided in `build/scripts/incremental` to generate mutations for some partitioning strategy. Line `143` can be modified to generate mutations for different partitioning strategies. The same pyspark code can be used to generate reverse mutations for temporal algorithms that move backwards in time (like LD). The provided mutations are for the partitioning `[0,20)`,`[20,30)` and `[30,40)`.

To run the WICM Incremental job using this configuration and with the same source ID `0` on the mutations:

```
cd build
hdfs dfs -copyFromLocal graphs/sampleGraphMutations 
bash ./scripts/wicmi/runEAT.sh 0 "sampleGraphMutations/window-1" output 0 40 "0;20;30;40" "sampleGraphMutations/window" 40
```
`output` should be present under `build/` after successful finishing of the job.

---
## 5. Running Other Graph Algorithms

Our paper evaluates six graph traversal algorithms: Earliest Arrival TIme (EAT), Single Source Shortest Path (SSSP), Temporal Reachability (TR), Latest Departure time (LD), Temporal Minimum Spanning Tree (TMST) and Fastest travel Time (FAST).
We have provided a job scripts for all platform variants to run each of these 6 traversal algorithms: `runEAT.sh, runSSSP.sh, runTR.sh, runLD.sh, runTMST.sh, runFAST.sh` under respective folders [ICM](https://github.com/dream-lab/wicm/tree/IncrementalWICM/build/scripts/giraph/icm) and [WICM(I)](https://github.com/dream-lab/wicm/tree/IncrementalWICM/build/scripts/giraph/wicmi). 

The scripts can be edited to specify the number of workers using the argument `-w <num_workers>` and the number of threads per worker using the argument `giraph.numComputeThreads <num_threads>`. By default, we run on `1` worker and `1` thread per worker. Maximum application heap space can also be changed by modifying `yarnheap` argument.

---
## 6. Minimal experimental pipeline to run all algorithms

A minimal experiment script has been provided [`build/scripts/giraph/runExperiments.sh`](https://github.com/dream-lab/wicm/blob/IncrementalWICM/build/scripts/giraph/runExperiments.sh) to run all algorithms for two different source vertices (`22499` and `19862`) in the sample graph, for ICM and WICM(I). The temporal partitioning of the graph is `"0;20;30;40"`. 
For each source vertex and algorithm, the `build/scripts/giraph/compare.sh` script automatically verifies that the job output returned by ICM is identical to the ones returned by WICM(I). This is a sanity check to ensure the correctness of incremental WICM relative to the ICM baseline.

To run the experiment pipeline:

```
cd build
bash ./scripts/giraph/runExperiments.sh > experiment.out 2> experiment.err
```

---
## 7. Analytical Model for Memory Usage

The Python code for analytical modelling is present in `memoryEstimation.py` under `build/scripts/incremental`. 

---
## Contact

For more information, please contact: **Animesh Baranawal <animeshb@iisc.ac.in>**, DREAM:Lab, Department of Computational and Data Sciences, Indian Institute of Science, Bangalore, India
