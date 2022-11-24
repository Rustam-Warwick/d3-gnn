/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package typeinfo.listinfo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.jetbrains.annotations.Nullable;
import typeinfo.DummySerializer;
import typeinfo.recursivepojoinfo.RecursivePojoTypeInfo;

import java.util.List;

/**
 * A {@link TypeInformation} for the list types of the Java API where the inner element is recursive.
 * That is inner element contains List fields of the same type that refers to the same elementTypeInfo
 * Modified createSerializer, equals, hashCode logic to support nesting
 * <strong>Made to be jointly used with {@link RecursivePojoTypeInfo}<strong/>
 * <p>
 * - .createSerializer is creating a dummy serializer that cannot serialize anything
 * - actual elementSerializer should be separately set through reflection
 * </p>
 *
 * @param <T> The type of the elements in the list.
 */
@PublicEvolving
public final class RecursiveListTypeInfo<T> extends TypeInformation<List<T>> {

    private static final long serialVersionUID = 1L;

    public TypeInformation<T> elementTypeInfo;

    public RecursiveListTypeInfo(@Nullable TypeInformation<T> elementTypeInfo) {
        this.elementTypeInfo = elementTypeInfo;
    }

    // ------------------------------------------------------------------------
    //  ListTypeInfo specific properties
    // ------------------------------------------------------------------------

    /**
     * Gets the type information for the elements contained in the list
     */
    public TypeInformation<T> getElementTypeInfo() {
        return elementTypeInfo;
    }

    // ------------------------------------------------------------------------
    //  TypeInformation implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        // similar as arrays, the lists are "opaque" to the direct field addressing logic
        // since the list's elements are not addressable, we do not expose them
        return 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<List<T>> getTypeClass() {
        return (Class<List<T>>) (Class<?>) List.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<List<T>> createSerializer(ExecutionConfig config) {
        return new ListSerializer<>(new DummySerializer<>()); // This function is called first by Flink pojo Type info later populated by my TypeInfo factory
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "List<" + "Recursive" + '>';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof RecursiveListTypeInfo) {
            final RecursiveListTypeInfo<?> other = (RecursiveListTypeInfo<?>) obj;
            return other.canEqual(this);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }
}
