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
import ai.djl.ndarray.NDArrayCollector;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.training.ParameterStore;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.Tracker;
import ai.djl.util.PairList;
import elements.Plugin;
import elements.annotations.RemoteFunction;

/**
 * Plugin that stores a single model withing the GNN Pipieline
 * <p>
 * Handles model synchronization through MASTER - REPLICA (2hop process)
 * </p>
 */
public class ModelServer<T extends Block> extends Plugin {

    public Model model;

    public transient T block;

    public int NUMBER_OF_COLLECTED_PARAMETERS; // How many gradients have been collected so far

    protected transient Optimizer optimizer; // Optimizer

    private transient PairList<String, Shape> inputShape; // Input Shape of the model

    private transient PairList<String, Shape> outputShape; // Output Shape of the model

    private transient ParameterStore parameterStore;

    private transient NDArrayCollector<String> collectedParameters;

    public ModelServer(Model m) {
        super(String.format("%s-server", m.getName()));
        this.model = m;
    }

    public void open() throws Exception {
        super.open();
        inputShape = model.describeInput();
        outputShape = model.describeOutput();
        optimizer = Optimizer.sgd().setLearningRateTracker(Tracker.fixed(0.01f)).optClipGrad(1).build();
        parameterStore = new ParameterStore();
        collectedParameters = new NDArrayCollector<>(true);
        block = (T) model.getBlock();
    }

    public Model getModel() {
        return model;
    }

    public T getBlock() {
        return block;
    }

    public PairList<String, Shape> getInputShape() {
        return inputShape;
    }

    public PairList<String, Shape> getOutputShape() {
        return outputShape;
    }

    public ParameterStore getParameterStore() {
        return parameterStore;
    }

    /**
     * Collect gradients from replica on the master (part-0) node.
     * <p>
     * Upon receiving all gradients updates the models and syncs with the replicas
     * </p>
     */
    @RemoteFunction(triggerUpdate = false)
    public void collectParameters(NDArrayCollector<String> newParameters) {
        collectedParameters.putAll(newParameters);
        if (++NUMBER_OF_COLLECTED_PARAMETERS == getStorage().layerFunction.getRuntimeContext().getNumberOfParallelSubtasks()) {
            parameterStore.updateAllParameters();
            NUMBER_OF_COLLECTED_PARAMETERS = 0;
            collectedParameters.clear();
        }
    }


}
