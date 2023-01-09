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
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.training.ParameterStore;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.Tracker;
import elements.Plugin;
import org.apache.flink.configuration.Configuration;

/**
 * Plugin that stores a single model withing the GNN Pipeline
 * <p>
 *      Handles model synchronization through {@code allReduce}
 * </p>
 */
public class ModelServer<T extends Block> extends Plugin {

    public Model model;

    public transient T block;

    protected transient Optimizer optimizer; // Optimizer

    private transient Shape[] inputShapes; // Input Shape of the model

    private transient Shape[] outputShapes; // Output Shape of the model

    private transient ParameterStore parameterStore;


    public ModelServer(Model m) {
        super(String.format("%s-server", m.getName()));
        this.model = m;
    }

    public synchronized void open(Configuration params) throws Exception {
        super.open(params);
        if(block == null) {
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

}
