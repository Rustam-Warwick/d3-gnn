# D3-GNN
## Requirements
- *Java 8 or 11* (Some higher version sometimes work as well)
- Gradle packet manager

## Datasets
1. **StackOverflow** dataset can be downloaded from https://snap.stanford.edu/data/sx-stackoverflow.html
2. **Reddit-Hyperlinks-body** dataset can be downloaded from https://snap.stanford.edu/data/soc-RedditHyperlinks.html
3. **SX-SuperUser** dataset can be downloaded from https://snap.stanford.edu/data/sx-superuser.html

We pre-process those datasets to make them easier to parse using the corresponding **SxSuperUser** **RedditHyperlinks** and **Stackoverflow** parsers.
We include this pre-processing script in /scripts/jupyter/Pre Process Datasets.ipynb file

To test the aforementioned datasets organize them in a folder structure as such:
- Datasets
  - RedditHyperlinks
    - soc-redditHyperlinks-body.tsv
  - StackOverflow
    - sx-stackoverflow.tsv
  - SX-SuperUser
    - sx-superuser.tsv

Then create an environments variable _DATASET_DIR_ pointing to the /Datasets folder.

## How to Run
1. Run **libShadowJar** task to generate library jar file. Then copy it to flink/lib directory 
2. Extend the **Dataset** class and implement _Splitter_ and _Parser_ logic for the particular dataset of choice or alternatively pass -d=[reddit-hyperlink | stackoverflow] to test with the implemented datasets. The latter will only work if the steps in Datasets are correctly executed.
3. Code the desired Flink pipeline in helpers/Main.java (default pipeline as described in the paper is already implemented)
4. Run shadowJar task to generate job jar file. 
5. Deploy the jar file using the Flink Dashboard
   1. For slurm cluster deployment there are helper scripts in _slurm-config/flink_. Read the corresponding README for instructions.
   

## Key Components
#### **src/main/java** is the main Java development folder
#### **src/main/lib** is the Folder that goes to Flink lib folder hence is statically class-loaded on cluster startup time
### Elements
Elements store main classes involved within Flink operators
- **GraphElement**: Base Class for all graph elements(@Vertex, @Feature, @Edge, @Plugin)
- **ReplicableGraphElement**: Base Class for all graph elements that can be replicated(@Vertex, @Plugin, @Feature)
- **Vertex**: Graph Vertex
- **Edge**: Graph Edge which has a source and destination @Vertices
- **Feature**: Anything that stores values. Can be attached to another @GraphElement thus be an attribute or can be independent replicable element
- **GraphOp**: Operation on Graph that comprises a @GraphElement, @Operation, part where this GraphOp should be directed to
- **Plugin**: Special GraphElements that are attached to @BaseStorage to extend the streaming logic of storage. Plugins should be subclassed for usage.
### Aggregators
Aggregators are Vertex @Features that aggregate messages from the previous layer neighborhood.
- **BaseAggregator**: Interface all aggregators should support
- **InPlaceMeanAggregator**: 1-hop neighborhood mean aggregator

### Features
Features can be instantiated using `new Feature()`. But this module contains Features that needed subclassing to have some custom logic inside.
- **Tensor**: Used to store node features

### Iterations
Stores all helpers and types that deal with Iterative Stream Processing
- **Rpc**: **Asynchronous Remote Method Call** object. Rpc is also a type of GraphElement. Rpc object contains information about which GraphElement to execute the procedure on, which method should be called and with which arguments to execute procedure.
- **RemoteFunction**: A special decorator for GraphElement's methods that can be using for Rpc calls. Failing to decorate as such will result in RuntimeErrors.
### Plugins
All the Plugins are contianed here
- **StreamingGNNEmbeddingLayer**: Streaming Inference plugin
- **WindowedGNNEmbeddingLayer**: Windowed Inference plugin
### Partitioner
- **BasePartitioner**: Base class for all partitioners
### Storage
Storage is central process function of gnn layers. Storage is responsible for storing GraphElements
- **BaseStorage**: Defines the main interfaces and common logic for storage.
- **ListGraphStorage**: Implementation of the storage using **TMSharedStorageBackend**
### Helpers
Miscellaneous classes
- **GraphStream**: Helper class to initialize iterative Flink operators for each Storage Layer
- **Main**: Main Entrypoint of the application that defines the Flink dataflow pipeline