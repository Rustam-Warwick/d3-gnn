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

import ai.djl.Device;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Parameter;
import ai.djl.training.ParameterStore;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.Tracker;
import ai.djl.util.Pair;
import ai.djl.util.PairList;
import elements.GraphOp;
import elements.Op;
import elements.Plugin;
import elements.iterations.*;
import helpers.GradientCollector;

import java.util.HashMap;
import java.util.Objects;

/**
 * Plugin that stores a single model withing the GNN Pipieline
 * <p>
 * Handles model synchronization through MASTER - REPLICA (2hop process)
 * </p>
 */
public class ModelServer extends Plugin {

    public Model model; // Model attached

    public int NUMBER_OF_COLLECTED_GRADIENTS; // How many gradients have been collected so far

    protected transient Optimizer optimizer; // Optimizer

    private transient PairList<String, Shape> inputShape; // Input Shape of the model

    private transient ParameterStore parameterStore;

    private transient GradientCollector<String> collectedGradients;

    public ModelServer(Model m) {
        super(String.format("%s-server", m.getName()));
        this.model = m;
    }

    public void open() throws Exception {
        super.open();
        inputShape = model.describeInput();
        optimizer = Optimizer.sgd().setLearningRateTracker(Tracker.fixed(0.01f)).optClipGrad(1).build();
        parameterStore = new ParameterStoreWrapper();
        if (getPartId() == 0) collectedGradients = new GradientCollector<>(5);
    }

    public Model getModel() {
        return model;
    }

    public PairList<String, Shape> getInputShape() {
        return inputShape;
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
    @RemoteFunction
    public void collect(GradientCollector<String> newGradients) {
        assert getPartId() == 0;
        collectedGradients.merge(newGradients);
        if (++NUMBER_OF_COLLECTED_GRADIENTS == storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks()) {
            parameterStore.updateAllParameters();
            NUMBER_OF_COLLECTED_GRADIENTS = 0;
        }
    }

    /**
     * Update the model of replicas to the sent master parameters
     */
    @RemoteFunction
    public void updateAllParameters(HashMap<String, NDArray> masterParameters) {
        System.out.println("MODEL UPDATED");
        model.getBlock().getParameters().forEach(parameter -> {
            if (masterParameters.containsKey(parameter.getValue().getId())) {
                parameter.getValue().close();
                parameter.getValue().setShape(null);
                parameter.getValue().setArray(masterParameters.get(parameter.getValue().getId()));
                parameter.getValue().getArray().detach();
            }
        });
    }

    public class ParameterStoreWrapper extends ParameterStore {
        /**
         * Method to be called from master part of modelServer to sync all parameters of out model
         */
        @Override
        public void updateAllParameters() {
            HashMap<String, NDArray> parameters = new GradientCollector<>();
            for (Pair<String, Parameter> parameter : model.getBlock().getParameters()) {
                if (collectedGradients.containsKey(parameter.getValue().getId())) {
                    optimizer.update(parameter.getValue().getId(), parameter.getValue().getArray(), collectedGradients.get(parameter.getValue().getId()));
                    parameters.put(parameter.getValue().getId(), parameter.getValue().getArray());
                }
            }
            Rmi rmi = new Rmi(getId(), "updateAllParameters", new Object[]{parameters}, elementType(), false, null);
            storage.layerFunction.broadcastMessage(new GraphOp(Op.RMI, null, rmi, null, MessageCommunication.BROADCAST), MessageDirection.ITERATE);
            collectedGradients.clearPrepone();
        }

        @Override
        public NDArray getValue(Parameter parameter, Device device, boolean training) {
            if (Objects.nonNull(parameter)) {
                parameter.getArray().setRequiresGradient(training);
                return parameter.getArray();
            } else {
                return null;
            }
        }

        /**
         * Send local gradients to master part for synchrnization
         */
        @Override
        public void sync() {
            GradientCollector<String> thisGradients = new GradientCollector<>();
            model.getBlock().getParameters().forEach((parameter) -> {
                if (parameter.getValue().getArray().hasGradient() && parameter.getValue().getArray().isValid()) {
                    thisGradients.put(parameter.getValue().getId(), parameter.getValue().getArray().getGradient());
                }
            });
            new RemoteInvoke()
                    .withArgs(thisGradients)
                    .where(MessageDirection.ITERATE)
                    .toElement(getId(), elementType())
                    .noUpdate()
                    .addDestination((short) 0)
                    .method("collect")
                    .buildAndRun(storage);
        }
    }
}
