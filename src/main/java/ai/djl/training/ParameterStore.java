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
import ai.djl.nn.Parameter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@code ParameterStore} contains a map from a parameter to the mirrors of it on other devices.
 */
public class ParameterStore {
    public transient Map<String, NDArray> parameterArrays;
    /** Constructs a new {@code ParameterStore} instance. */
    public ParameterStore(Model storedModel) {
        parameterArrays = new ConcurrentHashMap<>();
        storedModel.getBlock().getParameters().forEach(item -> {
            this.parameterArrays.putIfAbsent(item.getValue().getId(), item.getValue().getArray());
        });
    }

    /**
     * Sets the parameterServer used to apply updates to the parameters.
     *
     * @param parameterServer the parameterServer
     * @param devices the devices to create mirrored parameters on
     */
    public void setParameterServer(ParameterServer parameterServer, Device[] devices) {
        throw new RuntimeException("Parameter Server abstraction no longer used");
    }

    /** Updates all the mirrored parameters. */
    public void updateAllParameters() {
        // Maybe
    }

    /**
     * Returns the value of a mirrored parameter on a device.
     *
     * @param parameter the parameter to get the value for
     * @param device the device to get the mirror from
     * @param training true for a training forward pass
     * @return the value of the mirrored parameter on the device
     */
    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        if (parameter != null && parameterArrays.containsKey(parameter.getId())) {
            NDArray valueParam = this.parameterArrays.get(parameter.getId());
            if (valueParam.hasGradient() && !training) {
                System.out.println("Training switched off");
//                NDArray grad = valueParam.getGradient();
//                meanAccumulateGrads(new Tuple2<>(grad, 1), parameter.getId());
//                valueParam.setRequiresGradient(false);
            } else if (!valueParam.hasGradient() && training) {
                valueParam.setRequiresGradient(true);
            }

            return valueParam;
        } else {
            return null;
        }
    }

    /**
     * Get the {@link NDManager} associated with {@code ParameterStore}.
     *
     * @return the {@link NDManager}
     */
    public NDManager getManager() {
        return BaseNDManager.threadNDManager.get();
    }

    /** Synchronizes the values on all mirrors with the main parameter. */
    public void sync() {

    }

}
