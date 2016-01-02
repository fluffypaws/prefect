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

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.fluffypaws.prefect.api.Key;
import io.fluffypaws.prefect.api.StoreException;
import io.fluffypaws.prefect.api.Value;
import io.fluffypaws.prefect.api.KeyValueStore;
import io.fluffypaws.prefect.api.StoreFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class KeyValueStoreTest {

    private KeyValueStore store;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    final public void initializeStore() {
        Properties properties = new Properties();
        properties.setProperty(StoreFactory.STORE_DIRECTORY_KEY, temporaryFolder.getRoot().getAbsolutePath());
        store = StoreFactory.createKeyValueStore(properties);
        assertThat(store).isNotNull();
    }

    @Test
    public void testStore() throws StoreException {
        Value value1 = new Value("1".getBytes());
        Value value2 = new Value("2".getBytes());
        Key key1 = store.add(value1);
        Key key2 = store.add(value2);

        Value value3 = store.read(key1);
        Value value4 = store.read(key2);

        assertThat(value3.getData()).isEqualTo(value1.getData());
        assertThat(value4.getData()).isEqualTo(value2.getData());

        store.delete(key1);
        assertThat(store.read(key1)).isNull();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotDeleteFirstKey() {
        try {
            store.delete(new Key(String.valueOf(0).getBytes(StandardCharsets.UTF_8)));
        } catch (StoreException e) {
            fail("Should have thrown IllegalArgumentException");
        }
    }

}