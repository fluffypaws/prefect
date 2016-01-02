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

package io.fluffypaws.prefect.impl;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fluffypaws.prefect.api.Key;
import io.fluffypaws.prefect.api.KeyValueStore;
import io.fluffypaws.prefect.api.Object;
import io.fluffypaws.prefect.api.ObjectStore;
import io.fluffypaws.prefect.api.StoreException;
import io.fluffypaws.prefect.api.Value;

public class ObjectStoreImpl implements ObjectStore {

    private static Logger log = LoggerFactory.getLogger(ObjectStoreImpl.class);

    public static String STORE_LIST_COMPACTING_THRESHOLD_KEY = "store.compacting";

    private KeyValueStore keyValueStore;

    public ObjectStoreImpl() {
    }

    public boolean initialize(Properties properties, KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;

        try {
            Value value = keyValueStore.read(keyValueStore.getFirstKey());
            if (value.isEmpty()) {
                ObjectImpl.InitialObjectStore.create(keyValueStore, keyValueStore.getFirstKey(), null);
            }
            return true;
        } catch (StoreException e) {
            log.error("Cannot initialize ObjectStoreImpl, cannot write initial object", e);
            return false;
        }
    }

    public Object getRoot() throws StoreException {
        return get(keyValueStore.getFirstKey());
    }

    public Object get(final Key key) throws StoreException {
        return ObjectImpl.read(keyValueStore, key);
    }

    public KeyValueStore getKeyValueStore() {
        return keyValueStore;
    }

}