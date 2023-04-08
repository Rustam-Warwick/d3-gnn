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
import elements.GraphOp;
import elements.Plugin;
import elements.Rmi;
import elements.annotations.RemoteFunction;
import elements.enums.Op;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;
import org.apache.flink.runtime.state.tmshared.states.TMSharedValueState;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;

/**
 * Plugin that stores a single model withing the GNN Pipeline
 * <p>
 * Steps for synchronization:
 * 1. Do the optimizer.update in the master
 * 2. Broadcast the model parameters
 * 3. Calculate the streaming average of the model parameters
 * 4. Replace the local model with the average ones
 * </p>
 */
public class ModelServer<T extends Block> extends Plugin {

    /**
     * The {@link Model} object that is passed on job creation time
     */
    public Model model;

    /**
     * The Task local {@link ModelWrapper} object
     */
    public transient ModelWrapper<T> modelWrapper;

    /**
     * If this task is the one which's model was used it is considered as the master
     */
    public transient boolean isMaster;

    /**
     * Parameters that are gathered by this model. Actually created on {@code sync()}
     */
    protected transient Tuple2<ParameterList, Short> syncParameters;

    public ModelServer(Model m) {
        super(String.format("%s-server", m.getName()));
        this.model = m;
        this.listening = false; // Never listens to graph callbacks
    }

    public void open(Configuration params) throws Exception {
        super.open(params);
        modelWrapper = (ModelWrapper<T>) getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>(getId(), ModelWrapper.class, () -> new TMSharedValueState(new ModelWrapper<>(model)))).getValue();
        if (modelWrapper.model == model) isMaster = true;
        model = null;
    }

    public Model getModel() {
        return modelWrapper.model;
    }

    public T getBlock() {
        return modelWrapper.block;
    }

    public Shape[] getInputShapes() {
        return modelWrapper.inputShapes;
    }

    public Shape[] getOutputShapes() {
        return modelWrapper.outputShapes;
    }

    public ParameterStore getParameterStore() {
        return modelWrapper.parameterStore;
    }

    /**
     * Train, stop gradients and broadcast new parameters
     */
    public void syncFirstPhase() {
        if (isMaster) {
            for (Pair<String, Parameter> parameter : modelWrapper.block.getParameters()) {
                if (parameter.getValue().getArray().hasGradient()) {
                    modelWrapper.optimizer.update(parameter.getValue().getId(), parameter.getValue().getArray(), parameter.getValue().getArray().getGradient());
                    parameter.getValue().getArray().setRequiresGradient(false);
                }
            }
            getRuntimeContext().broadcast(
                    new GraphOp(Op.RMI,
                            new Rmi(getId(),
                                    "sync",
                                    getType(),
                                    new Object[]{modelWrapper.block.getParameters()})
                    ),
                    OutputTags.ITERATE_OUTPUT_TAG
            );
        }
    }

    /**
     * Update the current model with the average of other model parameters
     */
    public void syncSecondPhase() {
        if (isMaster) {
            for (int i = 0; i < modelWrapper.block.getParameters().size(); i++) {
                modelWrapper.block.getParameters().get(i).getValue().getArray().setRequiresGradient(false);
                modelWrapper.block.getParameters().get(i).getValue().getArray().subi(modelWrapper.block.getParameters().get(i).getValue().getArray()).addi(syncParameters.f0.get(i).getValue().getArray());
                syncParameters.f0.get(i).getValue().getArray().close();
            }
            syncParameters = null;
        }
    }

    /**
     * Calculate the running average of the model parameters
     */
    @RemoteFunction(triggerUpdate = false)
    public void sync(ParameterList parameterList) {
        if (isMaster) {
            if (syncParameters == null) syncParameters = Tuple2.of(parameterList, (short) 1);
            else {
                syncParameters.f1++;
                for (int i = 0; i < syncParameters.f0.size(); i++) {
                    NDArray current = syncParameters.f0.get(i).getValue().getArray();
                    NDArray update = parameterList.get(i).getValue().getArray();
                    current.addi(update.subi(current).divi(syncParameters.f1));
                    update.close(); // Close prematurely
                }
            }
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if (evt instanceof TrainingSubCoordinator.ForwardPhaser) {
            if (((TrainingSubCoordinator.ForwardPhaser) evt).iteration == 0) {
                syncFirstPhase();
            } else if (((TrainingSubCoordinator.ForwardPhaser) evt).iteration == 1) {
                syncSecondPhase();
            }
        }
    }

    /**
     * Simple Wrapper around the model object
     * <p>
     * This object is going to be added to TaskLocalState
     * </p>
     */
    protected static class ModelWrapper<T extends Block> {

        T block;

        Model model;

        Shape[] inputShapes;

        Shape[] outputShapes;

        ParameterStore parameterStore;

        Optimizer optimizer;

        public ModelWrapper(Model model) {
            this.model = model;
            this.block = (T) model.getBlock();
            this.inputShapes = block.getInputShapes();
            this.outputShapes = block.getOutputShapes(inputShapes);
            this.optimizer = Optimizer.adam().optLearningRateTracker(Tracker.fixed(0.01f)).build();
            this.parameterStore = new ParameterStore();
        }
    }

}
