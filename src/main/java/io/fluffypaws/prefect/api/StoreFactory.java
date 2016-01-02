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

import java.util.Properties;

import io.fluffypaws.prefect.impl.CassandraKeyValueStore;
import io.fluffypaws.prefect.impl.FileSystemKeyValueStore;
import io.fluffypaws.prefect.impl.ObjectStoreImpl;

public class StoreFactory {

    public static final String STORE_DIRECTORY_KEY = "store.directory";
    public static final String STORE_IMPLEMENTATION = "store.implementation";

    public static KeyValueStore createKeyValueStore(Properties properties) {
        final String implementation = properties.getProperty(STORE_IMPLEMENTATION, "filesystem");

        switch (implementation) {
            case "filesystem":
                FileSystemKeyValueStore fileSystemKeyValueStore = new FileSystemKeyValueStore();
                if (!fileSystemKeyValueStore.initialize(properties)) {
                    return null;
                }
                return fileSystemKeyValueStore;
            case "cassandra":
                CassandraKeyValueStore cassandraKeyValueStore = new CassandraKeyValueStore();
                if (!cassandraKeyValueStore.initialize(properties)) {
                    return null;
                }
                return cassandraKeyValueStore;
        }

        return null;
    }

    public static ObjectStore createObjectStore(Properties properties) {
        KeyValueStore keyValueStore = createKeyValueStore(properties);
        if (keyValueStore == null) {
            return null;
        }

        ObjectStoreImpl versionedListStore = new ObjectStoreImpl();
        if (!versionedListStore.initialize(properties, keyValueStore)) {
            return null;
        }
        return versionedListStore;
    }

}