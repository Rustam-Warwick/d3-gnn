/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package plugins;

import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.nn.Parameter;
import ai.djl.nn.ParameterList;
import ai.djl.training.ParameterStore;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.Tracker;
import ai.djl.util.Pair;
import elements.GraphEvent;
import elements.GraphOp;
import elements.Plugin;
import elements.Rmi;
import elements.annotations.RemoteFunction;
import elements.enums.Op;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.GraphEventPool;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.jetbrains.annotations.Nullable;

/**
 * Plugin that stores a single model withing the GNN Pipeline
 * <p>
 *      Handles model synchronization through {@code allReduce}
 * </p>
 */
public class ModelServer<T extends Block> extends Plugin {

    public Model model;

    protected transient T block;

    protected transient Optimizer optimizer; // Optimizer

    protected transient Shape[] inputShapes; // Input Shape of the model

    protected transient Shape[] outputShapes; // Output Shape of the model

    protected transient ParameterStore parameterStore;

    protected transient Tuple2<ParameterList, Short> syncParameters;

    protected transient GraphRuntimeContext masterRuntimeContext; // First runtime context registered will be master

    public ModelServer(Model m) {
        super(String.format("%s-server", m.getName()));
        this.model = m;
    }

    public synchronized void open(Configuration params) throws Exception {
        super.open(params);
        if(block == null) {
            masterRuntimeContext = getRuntimeContext();
            inputShapes = model.getBlock().getInputShapes();
            outputShapes = model.getBlock().getOutputShapes(inputShapes);
            parameterStore = new ParameterStore();
            block = (T) model.getBlock();
            optimizer = Optimizer.sgd().setLearningRateTracker(Tracker.fixed(0.01f)).optClipGrad(1).build();
        }
    }

    public Model getModel() {
        return model;
    }

    public T getBlock() {
        return block;
    }

    public Shape[] getInputShapes() {
        return inputShapes;
    }

    public Shape[] getOutputShapes() {
        return outputShapes;
    }

    public ParameterStore getParameterStore() {
        return parameterStore;
    }

    /**
     * Train and broadcast new parameters
     */
    public void syncFirstPhase(){
        if(getRuntimeContext() == masterRuntimeContext) {
            for (Pair<String, Parameter> parameter : block.getParameters()) {
                if (parameter.getValue().getArray().hasGradient()) {
                    optimizer.update(parameter.getValue().getId(), parameter.getValue().getArray(), parameter.getValue().getArray().getGradient());
                    parameter.getValue().getArray().setRequiresGradient(false);
                }
            }
            getRuntimeContext().broadcast(
                    new GraphOp(Op.RMI,
                            new Rmi(getId(),
                                    "sync",
                                    getType(),
                                    new Object[]{block.getParameters()})
                    ),
                    OutputTags.ITERATE_OUTPUT_TAG
            );
        }
        getRuntimeContext().broadcast(new GraphOp(new ParametersSynced()), OutputTags.ITERATE_OUTPUT_TAG);
    }

    public void syncSecondPhase(){
        if(getRuntimeContext() == masterRuntimeContext){
            for (int i = 0; i < block.getParameters().size(); i++) {
                block.getParameters().get(i).getValue().getArray().subi(block.getParameters().get(i).getValue().getArray()).addi(syncParameters.f0.get(i).getValue().getArray());
            }
            syncParameters = null;
        }
    }

    @RemoteFunction(triggerUpdate = false)
    public void sync(ParameterList parameterList){
        if(getRuntimeContext() == masterRuntimeContext){
            if(syncParameters == null) syncParameters = Tuple2.of(parameterList, (short) 1);
            else{
                syncParameters.f1++;
                for (int i = 0; i < syncParameters.f0.size(); i++) {
                    NDArray current = syncParameters.f0.get(i).getValue().getArray();
                    NDArray update = parameterList.get(i).getValue().getArray();
                    current.addi(update.sub(current).divi(syncParameters.f1));
                }
            }
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if(evt instanceof TrainingSubCoordinator.ForwardPhaser){
            if (((TrainingSubCoordinator.ForwardPhaser) evt).iteration == 0) {
                syncFirstPhase();
            }
        }else if(evt instanceof ParametersSynced){
            syncSecondPhase();
        }
    }

    /**
     * <p>
     *     Event evicting only after all the parameters have been synced
     * </p>
     */
    public static class ParametersSynced extends GraphEvent{
        transient short received;
        @Override
        public void merge(GraphEventPool pool, @Nullable GraphEvent incoming) {
            super.merge(pool, incoming);
            if(++received == pool.graphRuntimeContext.getNumberOfParallelSubtasks()){
                pool.evict(this);
            }
        }
    }

}
