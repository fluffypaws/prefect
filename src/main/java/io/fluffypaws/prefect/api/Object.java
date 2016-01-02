/*
 *  Copyright 2016 FluffyPaws Inc. (http://www.fluffypaws.io)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.fluffypaws.prefect.api;

public interface Object extends ImmutableObject {

    /**
     * Returns the object as it existed at the given stamp, or null otherwise.
     * TODO decide on rules: for now, supplying a stamp > this object's getTimestamp returns null.
     * @param stamp NonNull
     * @return the object as it existed at the given stamp, or null otherwise
     * @throws StoreException
     */
    ImmutableObject get(Stamp stamp) throws StoreException;

    /**
     * Sets the value for this object.
     * @param value can be null
     * @throws StoreException
     */
    void setValue(Value value) throws StoreException;

    /**
     * Creates a new {@link Object} with the given value and adds it using the given name. If the given name is already
     * used, the original child is overwritten.
     * @param name NonNull
     * @param value can be null
     * @return
     * @throws StoreException
     */
    Object addChild(String name, Value value) throws StoreException;

    /**
     * Removes child with the given name. Succeeds silently if the name was not among the children to begin with.
     * @param name NonNull
     * @throws StoreException
     */
    void deleteChild(String name) throws StoreException;

    /**
     * Returns the child with the given name or null if it does not exist.
     * @param name NonNull
     * @return the child with the given name or null if it does not exist
     */
    @Override
    Object getChild(String name) throws StoreException;

}