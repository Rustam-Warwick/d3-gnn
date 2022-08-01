# Flink Graph Neural Networks
## Requirements
- *Java 8 or 11* (Some higher version sometimes work as well)
- *build.gradle* file for Java dependencies
- *environment.yml* file for Python dependencies
- Dataset to be ingested, **Cora, MovieLens, Reddit, Stackoverflow** tested so far

## How to Run
1. Run **libShadowJar** task to generate library jar file. Then copy it to flink/lib directory 
2. Extend the **Dataset** class and provide _Splitter_ and _Parser_ for the particular dataset of choice
3. Code the desired Flink pipeline in helpers/Main.java
4. Run shadowJar task to generate job jar file. 
5. Deploy the jar file using the Flink Dashboard
   1. For slurm cluster deployment there is a helper _slurm-config/slurm.script_
   2. Local exeuction works as expected
   3. For K8 deployment there are helper files in kude-config
   

## Key Components
#### **src/main/java** is the main Java development folder
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
- **MeanAggregator**: 1-hop neighborhood mean aggregator

### Features
Features can be instantiated using `new Feature()`. But this module contains Features that needed subclassing to have some custom logic inside.
- **Tensor**: Used to store node features
- **Set**: Used to store replicated part arrays
- **MeanGradientCollector**: Used to collect gradients in backward passs

### Iterations
Stores all helpers and types that deal with Iterative Stream Processing
- **IterationState**: How the flowing GraphOp should be delivered, FORWARD(next-operator), ITERATE(same-operator), BACKWARD(previous-operator)
- **Rpc**: **Asynchronous Remote Method Call** object. Rpc is also a type of GraphElement. Rpc object contains information about which GraphElement to execute the procedure on, which method should be called and with which arguments to execute procedure.
- **RemoteFunction**: A special decorator for GraphElement's methods that can be using for Rpc calls. Failing to decorate as such will result in RuntimeErrors.
### Plugins
All the Plugins are contianed here
- **StreamingGNNEmbeddingLayer**: Streaming Inference plugin
- **WindowedGNNEmbeddingLayer**: Windowed Inference plugin
- **EdgeClassificationTrainingPlugin**: Output plugin for starting the edge classification training
- **VerteClassifiationTrainingPlugin**: Output plugin for starting the vertex classification training
- **GNNEmbeddingLayerTrainingPlugin**: Trainer plugin for intermediate gnn embedding layers
### Partitioner
- **BasePartitioner**: Base class for all partitioners
### Serializers
Some Java Types are not serializable(Converted into byte stream). All Custom Serializers are implemented here. They follow Flink's own tutorial on how to extend Flink Types.
### Storage
Storage is central process function of gnn layers. Storage is responsible for storing GraphElements
- **BaseStorage**: Defines the main interfaces and common logic for storage.
- **FlatInMemoryClassStorage**: Storage Implemented as tables of HashMaps that also use Flink State
### Helpers
Miscellaneous classes
- **GraphStream**: Helper class to initialize iterative Flink operators for each Storage Layer
- **Main**: Main Entrypoint of the application that defines the Flink dataflow pipeline