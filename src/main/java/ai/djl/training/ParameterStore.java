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

package ai.djl.training;

import ai.djl.Device;
import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Parameter;
import ai.djl.training.optimizer.Adam;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.util.PairList;
import elements.Plugin;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.RemoteInvoke;
import operators.coordinators.events.StopTraining;

import java.util.HashMap;
import java.util.Objects;

/**
 * Re-define ParameterStore as an interaface, because it is going to be abstracted into a plugin
 * Each ParameterStore stores the parameters of a single Model only.
 * If you want to have multiple Model in the same operator you need to define several ParameterStores for each of them
 */
public class ParameterStore extends Plugin {
    protected Model model;

    protected int NUMBER_OF_COLLECTED_GRADIENTS; // How many gradients have been collected

    protected HashMap<String, NDArray> collectedGradients; // HashMap of the collected gradients so far

    protected Optimizer optimizer; // Optimizer

    private transient PairList<String, Shape> inputShape; // Input Shape of the model

    public ParameterStore(Model m){
        super(String.format("%s-server",m.getName()));
        this.model = m;
    }

    public void open() {
        super.open();
        inputShape = model.describeInput();
        optimizer =  Adam.builder().build();
    }

    // INITIALIZATION DONE!!!

    public Model getModel() {
        return model;
    }

    public PairList<String, Shape> getInputShape(){
        return inputShape;
    };

    public void setParameterServer(ParameterServer parameterServer, Device[] devices) {
        // NONE. Just need to have this because DJL might need it at some point
    }

    public void updateAllParameters() {
        HashMap<String, NDArray> parameters = new HashMap<>();
        model.getBlock().getParameters().forEach(parameter->{
            optimizer.update(parameter.getValue().getId(), parameter.getValue().getArray(), collectedGradients.get(parameter.getValue().getId()));
            parameters.put(parameter.getValue().getId(), parameter.getValue().getArray());
        });
        collectedGradients = null;
        new RemoteInvoke()
                .method("updateReplicaParameters")
                .noUpdate()
                .where(MessageDirection.ITERATE)
                .toElement(getId(), elementType())
                .withArgs(parameters)
                .addDestinations(othersMasterParts())
                .buildAndRun(storage);
        storage.layerFunction.operatorEventMessage(new StopTraining());
    }

    @RemoteFunction
    public void updateReplicaParameters(HashMap<String, NDArray> masterParameters){
        model.getBlock().getParameters().forEach(parameter->{
            parameter.getValue().close();
            parameter.getValue().setShape(null);
            parameter.getValue().setArray(masterParameters.get(parameter.getValue().getId()));
        });
        storage.layerFunction.operatorEventMessage(new StopTraining());
    }

    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        if(Objects.nonNull(parameter)){
            parameter.getArray().setRequiresGradient(training);
            return parameter.getArray();
        }else{
            return null;
        }
    }

    public NDManager getManager() {
        return BaseNDManager.threadNDManager.get();
    }

    /**
     * Collect the partial Gradients from replicas,
     * @implNote Note that this is only called in the master(0 part) of this operator
     * @param gradients Partial Gradients from various operators
     */
    @RemoteFunction
    public void collect(HashMap<String, NDArray> gradients){
        if(Objects.isNull(collectedGradients)){
            collectedGradients = gradients;
        }else{
            gradients.forEach((key, gradient)->{
                collectedGradients.compute(key,(a, value)->value.addi(gradient));
            });
        }
        if(++NUMBER_OF_COLLECTED_GRADIENTS == storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks()){
            updateAllParameters();
            NUMBER_OF_COLLECTED_GRADIENTS = 0;
            collectedGradients = null;
        }
    }

    /**
     * Sync gradients from all the parallel operators.
     * @implNote Calling Sync for all of the parallel subtasks will result in training and syncing of the underlying model
     */
    public void sync() {
        HashMap<String, NDArray> thisGradients = new HashMap<>();
        model.getBlock().getParameters().forEach((parameter)->{
            if(parameter.getValue().getArray().hasGradient()){
                thisGradients.put(parameter.getValue().getId(), parameter.getValue().getArray().getGradient());
            }else{
                thisGradients.put(parameter.getValue().getId(), parameter.getValue().getArray().zerosLike());
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
