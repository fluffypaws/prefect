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
import java.time.Instant;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import io.fluffypaws.prefect.api.Object;
import io.fluffypaws.prefect.api.ImmutableObject;
import io.fluffypaws.prefect.api.ObjectStore;
import io.fluffypaws.prefect.api.Stamp;
import io.fluffypaws.prefect.api.StoreException;
import io.fluffypaws.prefect.api.StoreFactory;
import io.fluffypaws.prefect.api.Value;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectStoreTest {

    private ObjectStore objectStore;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    final public void initializeStore() {
        Properties properties = new Properties();
        properties.setProperty(StoreFactory.STORE_DIRECTORY_KEY, temporaryFolder.getRoot().getAbsolutePath());
        properties.setProperty(ObjectStoreImpl.STORE_LIST_COMPACTING_THRESHOLD_KEY, String.valueOf(3));
        objectStore = StoreFactory.createObjectStore(properties);
        assertThat(objectStore).isNotNull();
    }

    @Test
    public void testRootNodeSimpleValueOperations() throws StoreException {
        io.fluffypaws.prefect.api.Object root = objectStore.getRoot();
        Stamp rootStamp0 = root.getStamp();
        Instant testStart = Instant.now();
        Value value0 = root.getValue();

        assertThat(value0).isNull();

        byte[] helloWorld = "hello world".getBytes(StandardCharsets.UTF_8);
        root.setValue(new Value(helloWorld));
        Stamp rootStamp1 = root.getStamp();
        assertThat(rootStamp1).isGreaterThan(rootStamp0);

        Object updatedRoot = objectStore.getRoot();
        assertThat(rootStamp1).isEqualTo(updatedRoot.getStamp());
        assertThat(helloWorld).isEqualTo(updatedRoot.getValue().getData());

        ImmutableObject originalRoot = root.get(rootStamp0);
        assertThat(originalRoot).isNotNull();
        assertThat(originalRoot.getStamp()).isEqualTo(rootStamp0);
        assertThat(originalRoot.getValue()).isNull();

        ImmutableObject prehistoricRoot = root.get(objectStore.getKeyValueStore().calculateSnapshotStamp(testStart.minusSeconds(1)));
        assertThat(prehistoricRoot).isNull();
    }

    @Test
    public void testBackToTheFuture() throws StoreException, InterruptedException {
        Object object = objectStore.getRoot();
        Thread.sleep(1);
        ImmutableObject ancestor = object.get(objectStore.getKeyValueStore().calculateSnapshotStamp(Instant.now()));
        assertThat(ancestor).isNull();
    }

    // TODO: add test to add - delete - cycle back in time & check the added object still exists and all its children/values

    @Test
    public void testIterator() throws StoreException, InterruptedException {
        Object object = objectStore.getRoot();
        Iterator<String> iterator = object.getChildNames();
        assertThat(iterator.hasNext()).isFalse();

        object.addChild("c1", null);
        object.addChild("c2", null);
        assertThat(object.getChildNames()).containsOnlyOnce("c1", "c2");

        object.deleteChild("c1");
        assertThat(object.getChildNames()).containsOnlyOnce("c2");

        object.deleteChild("c2");
        assertThat(object.getChildNames().hasNext()).isFalse();
    }

    @Test
    public void testAddChildReturnsFunctioningObject() throws StoreException {
        Object root = objectStore.getRoot();
        Object c1 = root.addChild("c1", null);
        c1.addChild("c2", null);
    }

    @Ignore
    @Test
    public void cannotAddTwoChildrenWithSameName() throws StoreException {
        thrown.expect(IllegalArgumentException.class);
        Object root = objectStore.getRoot();
        root.addChild("c1", null);
        root.addChild("c1", null);
    }

    // next to test: do a run that results in compacting, that will/should break now

}