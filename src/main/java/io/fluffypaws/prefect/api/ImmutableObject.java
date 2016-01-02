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

import java.lang.*;
import java.util.Iterator;

/**
 * See {@link Object}
 */
public interface ImmutableObject {

    /**
     * Returns the key of this object.
     * @return the key of this object
     */
    Key getKey();

    /**
     * Returns the stamp when this object was created.
     * @return the stamp when this object was created
     */
    Stamp getCreation();

    /**
     * Returns the stamp of the last recorded change of this object. Note that if this object was obtained through
     * a call to {@link Object#get(Stamp)} this method will likely return a stamp lower than what was used for
     * the call to {@link Object#get(Stamp)}.
     * @return the stamp of the last recorded change of this object
     */
    Stamp getStamp() throws StoreException;

    /**
     * Returns the value associated with this object.
     * @return the value associated with this object
     * @throws StoreException
     */
    Value getValue() throws StoreException;

    /**
     * Returns an iterator for the names of the children of this object.
     * @return an iterator for the names of the children of this object
     */
    Iterator<String> getChildNames() throws StoreException;

    /**
     * Returns the child with the given name or null if it does not exist.
     * @param name
     * @return the child with the given name or null if it does not exist
     */
    ImmutableObject getChild(String name) throws StoreException;

}