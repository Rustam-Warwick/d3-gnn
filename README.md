# Flink Graph Neural Networks
## Requirements
- *Java 8 or 11* (Some higher version sometimes work as well)
- *build.gradle* file for Java dependencies
- *environment.yml* file for Python dependencies
- Dataset to be ingested, **movielens, cora** tested so far
- Use Java -Xmx to increase heap size in case of memory errors

## Key Components
#### **src/main/java** is the main Java development folder
### Elements
Elements store main classes involved within Flink operators
- **GraphElement**: Base Class for all graph elements(@Vertex, @Feature, @Edge, @Plugin)
- **ReplicableGraphElement**: Base Class for all graph elements that can be replicated(@Vertex, @Plugin, @Feature)
- **Vertex**: Graph Vertex
- **Edge**: Graph Edge which has a source and destination @Vertices
- **Feature**: Anything that stores values. Can be attached to another @GraphElement thus be an attribute or can be independent replicable element
- **GraphOp**: Operation on Graph that comprises a @GraphElement, @Operation, part where this GraphOp should be directed to and @IterationState
- **Plugin**: Special GraphElements that are attached to @BaseStorage to extend the streaming logic of storage. Plugins should be subclassed for usage.
### Aggregators
Aggregators are Vertex @Features that aggregate messages from the previous layer neighborhood. 
- **BaseAggregator**: Interface all aggregators should support
- **MeanAggregator**: 1-hop neighborhood mean aggregator
- **SumAggregator**: 1-hop neighborhood sum aggregator

### Features
Features can be instantiated using `new Feature()`. But this module contains Features that needed subclassing to have some custom logic inside.
- **Set**: Feature that stores a List. This one is used to store replica parts of @ReplicableGraphElements
- **VTensor**: VersionedTensor. Important to store embeddings in gnn layers that are also aware about which model version did deliver them. This is a utility for that.

### Iterations
Stores all helpers and types that deal with Iterative Stream Processing
- **Filters**: Straightforward from their name
- **IterationState**: How the flowing GraphOp should be delivered, FORWARD(next-operator), ITERATE(same-operator), BACKWARD(previous-operator)
- **RemoteDestination**: How to determine the part of next operator. Used to make code cleaner.
- **Rpc**: **Asynchronous Remote Method Call** object. Rpc is also a type of GraphElement. Rpc object contains information about which GraphElement to execute the procedure on, which method should be called and with which arguments to execute procedure. 
- **RemoteFunction**: A special decorator for GraphElement's methods that can be using for Rpc calls. Failing to decorate as such will result in RuntimeErrors. 
### Plugins
All the Plugins are contianed here
- **GNNLayerInference**: Streaming Inference Logic contained here. This one is abstract **message** and **update** functions need to be implemented. 
- **GnnOutputInference**: Streaming Inference logic for the last layer. Logic is different from LayerInference since we only need the **output** function
- Those 2 above also have associated **trainer** plugins. Trainer are **automatically added once you add inference plugins**.
### Partitioner
All Streaming Partitioners are implemented here. One thing to note, currently all partitioners are not parallelizable.
- **BasePartitioner**: Base class for all partitioners
### Serializers
Some Java Types are not serializable(Converted into byte stream). All Custom Serializers are implemented here. They follow Flink's own tutorial on how to extend Flink Types. 
### Storage
Storage is central process function of gnn layers. Storage is responsible for storing GraphElements
- **BaseStorage**: Defines the main interfaces and common logic for storage. 
- **HashMapStorage**: Storage Implemented as tables of HashMaps that also use Flink State 
### Helpers
Miscellaneous classes
- **Selectors**: ElementIdSelector, PartKeySelector are used for selecting keys for keying the streaming
- **MyKeyedProcessOperator**: Special Process Operator that should be used with @BaseStorage. Populates some variables in @BaseStorage that allows us send data, register timers on open() call. Normally we should wait for first element to arrive.
- **TaskNDManager**: A helper NDManager for managing out of JVM memory that is used in Deep Java Library & Pytorch.
- **MyParameterStore**: A ParameterStore that also accumulates gradients and can be serialized. This one is used within the GNNInference plugins.
- **KeepLastElement**: Evictor to only keep and process the last element in window function. Used for computing loss based on joined prediction and label streams.
- **JavaTensor**: A Special Proxy Tensor that is automatically freed from memory once dereferenced. 
- **GraphStream**: A helper class to construct a full GNN Pipeline. 
- **Main**: Main method that constructs the GNN pipeline and invokes it. 
## How to Run
1. Prepare the file to be streamed in. I use movielens ratings for now.
2. Stream that file through helper python script `python /python/stream_file.py "path/to/file"`
3. Write your GNN Pipeline on `helpers/Main.class` 
4. Execute locally for testing

- Note that we can use `readFile` primitives to read the file directly in Flink, but such way we have no control over the timing of the stream. In Python script throughput can be modified using `Thread.sleep` time 
- Once tested we can use `gradle::jar` task to prepare a fat-jar file in `build/libs`. This jar then can be submitted to Flink cluster. 