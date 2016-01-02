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

import java.time.Instant;

/**
 * General note: return values are _always_ fetched from the underlying physical store and are _never_ cached.
 */
public interface KeyValueStore {

    /**
     * Gets the key of the first object that was created in the store. This object has an initial empty value and cannot be deleted.
     * @return
     */
    Key getFirstKey();

    /**
     * Adds a value to the store
     * @param value NonNull
     * @return the key for the newly added value
     */
    Key add(Value value) throws StoreException;

    /**
     * Retrieves the Value corresponding with the key from the store
     * @param key NonNull
     * @return the value or null if not found
     */
    Value read(Key key) throws StoreException;

    /**
     * Writes a value pair to the store
     * @param key NonNull
     * @param value NonNull
     */
    void write(Key key, Value value) throws StoreException;

    /**
     * Deletes a key
     * @param key NonNull
     * @throws IllegalArgumentException when called with the key that is returned by #getFirstKey();
     */
    void delete(Key key) throws StoreException, IllegalArgumentException;

    /**
     * Creates a new Stamp
     * @return a new Stamp
     */
    Stamp generateStamp();

    /**
     * Creates a Stamp that represents a moment in the database's history as close as possible to the provided instant.
     * @param instant NonNull
     * @return a new Stamp
     */
    Stamp calculateSnapshotStamp(Instant instant);

    /**
     * Returns current statistics of this store
     * @return
     */
    KeyValueStoreStatistics getStatistics();

}