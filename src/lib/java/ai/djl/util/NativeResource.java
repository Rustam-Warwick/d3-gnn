/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package ai.djl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code NativeResource} is an internal class for {@link AutoCloseable} blocks of memory created in
 * the different engines.
 *
 * @author rustambaku13
 * <p>
 * Removed unnecessary fields
 * Removed atomic access
 * Remove marshalling
 * </p>
 */
public abstract class NativeResource<T> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(NativeResource.class);

    protected long handle;

    protected NativeResource(long handle) {
        this.handle = handle;
    }

    /**
     * Gets the boolean that indicates whether this resource has been released.
     *
     * @return whether this resource has been released
     */
    public boolean isReleased() {
        return handle == Long.MAX_VALUE;
    }

    /**
     * Mark this as released
     */
    public void markReleased() {
        handle = Long.MAX_VALUE;
    }

    /**
     * Get handle
     */
    public T getHandle() {
        return (T) Long.valueOf(handle);
    }

    /**
     * Gets the unique ID of this resource.
     *
     * @return the unique ID of this resource
     */
    public final String getUid() {
        logger.warn("Accessing UID of resource is costly, try to not do it");
        return String.valueOf(handle);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        throw new UnsupportedOperationException("Not implemented.");
    }
}
