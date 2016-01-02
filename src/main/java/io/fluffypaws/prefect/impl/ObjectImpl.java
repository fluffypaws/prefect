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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fluffypaws.prefect.api.ImmutableObject;
import io.fluffypaws.prefect.api.Key;
import io.fluffypaws.prefect.api.KeyValueStore;
import io.fluffypaws.prefect.api.Object;
import io.fluffypaws.prefect.api.Stamp;
import io.fluffypaws.prefect.api.StoreException;
import io.fluffypaws.prefect.api.Value;

class ObjectImpl implements io.fluffypaws.prefect.api.Object {

    private static Logger log = LoggerFactory.getLogger(ObjectImpl.class);

    enum PatchType {
        ADD_CHILD, DELETE_CHILD, SET_VALUE
    }

    static class Patch implements Serializable {
        public Stamp stamp;
        public PatchType type;
        public String childName;
        public Key key;
        public Patch(Stamp stamp, PatchType type, String childName, Key key) {
            this.stamp = stamp;
            this.type = type;
            this.childName = childName;
            this.key = key;
        }
    }

    /* Types:
     * - initial object: stores value + stamp
     * - object + patches: stores value + stamp + key for patches
     * - object + slices: stores value + stamp + key for slices
     * - branched object: array of (ancestor OHS key + stamp) + key for slices
     */
    enum ObjectHistoryStoreType {
        InitialObject, ObjectPlusPatches
    }

    static class History implements Serializable {
        Stamp stamp;
        Value value;
        Key next;
    }

    interface ObjectHistoryStore extends ImmutableObject {
        ObjectHistoryStore addPatch(Patch patch) throws StoreException;
        ImmutableObject get(Stamp stamp);
        Object getChild(final String name) throws StoreException;
    }

    static class InitialObjectStore implements ObjectHistoryStore {
        private KeyValueStore store;
        private Key key;
        private History history;
        private InitialObjectStore(final KeyValueStore store, final Key key, final History history) {
            this.store = store;
            this.key = key;
            this.history = history;
        }
        public static ObjectHistoryStore read(final KeyValueStore store, final Key key, final ObjectInput objectInput) throws StoreException {
            try {
                History history = (History)objectInput.readObject();
                return new InitialObjectStore(store, key, history);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Error deserializing " + new String(key.getData(), StandardCharsets.UTF_8), e);
                throw new StoreException(e);
            }
        }
        public static ObjectHistoryStore create(final KeyValueStore store, Key key, final Value value) throws StoreException {
            History history = new History();
            history.stamp = store.generateStamp();
            history.value = value;
            history.next = null;

            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutput out = new ObjectOutputStream(bos)) {
                ObjectHistoryStoreType type = ObjectHistoryStoreType.InitialObject;
                out.writeObject(type);
                out.writeObject(history);
                if (key != null) {
                    store.write(key, new Value(bos.toByteArray()));
                } else {
                    key = store.add(new Value(bos.toByteArray()));
                }
                return new InitialObjectStore(store, key, history);
            } catch (IOException e) {
                log.error("Error serializing " + new String(key.getData(), StandardCharsets.UTF_8), e);
                throw new StoreException(e);
            }
        }
        public ObjectHistoryStore addPatch(final Patch patch) throws StoreException {
            return ObjectPlusPatchesStore.create(store, key, history, patch);
        }
        public ImmutableObject get(final Stamp stamp) {
            if (history.stamp.equals(stamp)) {
                return this;
            } else {
                return null;
            }
        }
        public Object getChild(final String name) throws StoreException {
            return null;
        }
        public Key getKey() {
            return key;
        }
        public Stamp getCreation() {
            return history.stamp;
        }
        public Stamp getStamp() throws StoreException {
            return history.stamp;
        }
        public Value getValue() throws StoreException {
            return history.value;
        }
        public Iterator<String> getChildNames() throws StoreException {
            return Collections.emptyIterator();
        }
    }

    static class ObjectPlusPatchesStore implements ObjectHistoryStore {
        private KeyValueStore store;
        private Key key;
        private History history;
        private ArrayList<Patch> patches;
        private Stamp snapshotStamp;
        private ObjectPlusPatchesStore(final KeyValueStore store, final Key key, final History history, final ArrayList<Patch> patches, Stamp snapshotStamp) {
            this.store = store;
            this.key = key;
            this.history = history;
            this.patches = patches;
            this.snapshotStamp = snapshotStamp;
        }
        public static ObjectPlusPatchesStore read(final KeyValueStore store, final Key key, final ObjectInput objectInput) throws StoreException {
            try {
                History history = (History)objectInput.readObject();
                Value serializedPatches = store.read(history.next);
                try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedPatches.getData());
                     ObjectInput patchesObjectInput = new ObjectInputStream(bis) {
                     }) {
                    ArrayList<Patch> patches = (ArrayList<Patch>)patchesObjectInput.readObject();
                    return new ObjectPlusPatchesStore(store, key, history, patches, null);
                } catch (IOException | ClassNotFoundException e) {
                    log.error("Error deserializing patches from " + new String(history.next.getData(), StandardCharsets.UTF_8), e);
                    throw new StoreException(e);
                }
            } catch (IOException | ClassNotFoundException e) {
                log.error("Error deserializing " + new String(key.getData(), StandardCharsets.UTF_8), e);
                throw new StoreException(e);
            }
        }
        public static ObjectHistoryStore create(final KeyValueStore store, Key key, History history, final Patch patch) throws StoreException {
            ArrayList<Patch> patches = new ArrayList<>();
            patches.add(patch);

            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(patches);
                history.next = store.add(new Value(bos.toByteArray()));
            } catch (IOException e) {
                log.error("Error serializing initial patch for " + new String(key.getData(), StandardCharsets.UTF_8), e);
                throw new StoreException(e);
            }

            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutput out = new ObjectOutputStream(bos)) {
                ObjectHistoryStoreType type = ObjectHistoryStoreType.ObjectPlusPatches;
                out.writeObject(type);
                out.writeObject(history);
                store.write(key, new Value(bos.toByteArray()));
                return new ObjectPlusPatchesStore(store, key, history, patches, null);
            } catch (IOException e) {
                log.error("Error serializing history for " + new String(key.getData(), StandardCharsets.UTF_8), e);
                throw new StoreException(e);
            }
        }
        private Iterable<Patch> getAvailablePatches() {
            if (snapshotStamp == null) {
                return patches;
            } else {
                return new Iterable<Patch>() {
                    @Override
                    public Iterator<Patch> iterator() {
                        return new Iterator<Patch>() {
                            Iterator<Patch> iterator = patches.iterator();
                            Patch next = null;

                            boolean findNext() {
                                if (next != null) {
                                    return true;
                                }
                                while (iterator.hasNext()) {
                                    next = iterator.next();
                                    if (next.stamp.compareTo(snapshotStamp) <= 0) {
                                        return true;
                                    }
                                }
                                return false;
                            }

                            @Override
                            public boolean hasNext() {
                                return findNext();
                            }

                            @Override
                            public Patch next() {
                                if (findNext()) {
                                    Patch value = next;
                                    next = null;
                                    return value;
                                } else {
                                    throw new NoSuchElementException();
                                }
                            }
                        };
                    }
                };
            }
        }
        public ObjectHistoryStore addPatch(final Patch patch) throws StoreException {
            patches.add(patch);

            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(patches);
                store.write(history.next, new Value(bos.toByteArray()));
                return this;
            } catch (IOException e) {
                log.error("Error serializing patches for " + new String(key.getData(), StandardCharsets.UTF_8), e);
                throw new StoreException(e);
            }
        }
        public ImmutableObject get(final Stamp stamp) {
            if (history.stamp.isAfter(stamp)) {
                return null;
            }
            return new ObjectPlusPatchesStore(store, key, history, patches, stamp);
        }
        public Object getChild(final String name) throws StoreException {
            Key key = null;
            for (Patch p : getAvailablePatches()) {
                if (p.type == PatchType.ADD_CHILD && p.childName.equals(name)) {
                    key = p.key;
                } else if (p.type == PatchType.DELETE_CHILD && p.childName.equals(name)) {
                    key = null;
                }
            }
            if (key == null) {
                return null;
            } else {
                return ObjectImpl.read(store, key);
            }
        }
        public Key getKey() {
            return key;
        }
        public Stamp getCreation() {
            return history.stamp;
        }
        public Stamp getStamp() throws StoreException {
            Stamp latest = history.stamp;
            for (Patch p : getAvailablePatches()) {
                latest = p.stamp;
            }
            return latest;
        }
        public Value getValue() throws StoreException {
            Key latest = null;
            for (Patch p : getAvailablePatches()) {
                if (p.type == PatchType.SET_VALUE)
                latest = p.key;
            }
            if (latest == null) {
                return history.value;
            } else {
                return store.read(latest);
            }
        }
        public Iterator<String> getChildNames() throws StoreException {
            /* Not likely the best performing mechanism, but for now will do:
             * - simply create a list of all existing children, then delete
             * - iterate twice over the patches to remove any that are inserted and also deleted
             * - children are never reused, so after a delete in a patch element, another add will never follow
             */
            HashSet<String> set = new HashSet<>();
            for (Patch p : getAvailablePatches()) {
                if (p.type == PatchType.ADD_CHILD) {
                    set.add(p.childName);
                }
            }
            for (Patch p : getAvailablePatches()) {
                if (p.type == PatchType.DELETE_CHILD) {
                    set.remove(p.childName);
                }
            }
            return set.iterator();
        }
    }

    private KeyValueStore store;
    private Key key;
    private ObjectHistoryStore ohs;

    private ObjectImpl(KeyValueStore store, Key key, ObjectHistoryStore ohs) {
        this.store = store;
        this.key = key;
        this.ohs = ohs;
    }

    public static ObjectImpl read(KeyValueStore store, Key key) throws StoreException {
        ObjectHistoryStore ohs;
        Value value = store.read(key);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(value.getData());
             ObjectInput objectInput = new ObjectInputStream(bis)) {
            ObjectHistoryStoreType type = (ObjectHistoryStoreType)objectInput.readObject();
            switch (type) {
                case InitialObject:
                    ohs = InitialObjectStore.read(store, key, objectInput);
                    break;
                case ObjectPlusPatches:
                    ohs = ObjectPlusPatchesStore.read(store, key, objectInput);
                    break;
                default:
                    String message = "Error deserializing, unknown type " + type + " in key " + new String(key.getData(), StandardCharsets.UTF_8);
                    log.error(message);
                    throw new StoreException(message);
            }
            return new ObjectImpl(store, key, ohs);
        } catch (IOException | ClassNotFoundException e) {
            log.error("Error deserializing " + new String(key.getData(), StandardCharsets.UTF_8), e);
            throw new StoreException(e);
        }
    }

    public void setValue(final Value value) throws StoreException {
        Key key = store.add(value);
        ohs = ohs.addPatch(new Patch(store.generateStamp(), PatchType.SET_VALUE, null, key));
    }

    public Object addChild(final String name, final Value value) throws StoreException {
        ObjectHistoryStore child = InitialObjectStore.create(store, null, value);
        ohs = ohs.addPatch(new Patch(store.generateStamp(), PatchType.ADD_CHILD, name, child.getKey()));
        return new ObjectImpl(store, child.getKey(), child);
    }

    public void deleteChild(final String name) throws StoreException {
        ohs = ohs.addPatch(new Patch(store.generateStamp(), PatchType.DELETE_CHILD, name, null));
    }

    public Key getKey() {
        return key;
    }

    public Stamp getCreation() {
        return ohs.getCreation();
    }

    public Stamp getStamp() throws StoreException {
        return ohs.getStamp();
    }

    public ImmutableObject get(final Stamp stamp) throws StoreException {
        return ohs.get(stamp);
    }

    public Value getValue() throws StoreException {
        return ohs.getValue();
    }

    public Iterator<String> getChildNames() throws StoreException {
        return ohs.getChildNames();
    }

    public Object getChild(final String name) throws StoreException {
        return ohs.getChild(name);
    }

}