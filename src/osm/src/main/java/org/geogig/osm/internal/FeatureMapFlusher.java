/* Copyright (c) 2013-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Victor Olaya (Boundless) - initial implementation
 */
package org.geogig.osm.internal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.impl.RevFeatureBuilder;
import org.locationtech.geogig.model.impl.RevFeatureTypeBuilder;
import org.locationtech.geogig.repository.DefaultProgressListener;
import org.locationtech.geogig.repository.FeatureInfo;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.impl.FeatureToDelete;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.FeatureType;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;

/**
 * A buffer that wraps a multimap to store features, that flushes when a certain limit is reached.
 * Flushing means in this case inserting the features in the working tree.
 * 
 * The main purpose of this is to reduce the number of insert operations, while still supporting a
 * large amount of feature without causing an OOM error.
 * 
 */
public class FeatureMapFlusher {

    private static final int LIMIT = 100000;

    private HashMultimap<String, SimpleFeature> map;

    private Repository repository;

    private int count;

    public FeatureMapFlusher(Repository repository) {
        this.repository = repository;
        map = HashMultimap.create();
        count = 0;
    }

    public void put(String path, SimpleFeature feature) {
        map.put(path, feature);
        count++;
        if (count > LIMIT) {
            flushAll();
        }

    }

    private void flush(String path) {
        Set<SimpleFeature> features = map.get(path);
        if (!features.isEmpty()) {
        	Map<FeatureType, RevFeatureType> types = new HashMap<>();
            Iterator<FeatureInfo> finfos = Iterators.transform(features.iterator(), (f) -> {
                FeatureType ft = f.getType();
                RevFeatureType rft = types.get(ft);
                if (rft == null) {
                    rft = RevFeatureTypeBuilder.build(ft);
                    types.put(ft, rft);
                    repository.objectDatabase().put(rft);
                }
                String featurePath = NodeRef.appendChild(path, f.getIdentifier().getID());
                if (f instanceof FeatureToDelete) {
                	return FeatureInfo.delete(featurePath);
                }
                return FeatureInfo.insert(RevFeatureBuilder.build(f), rft.getId(), featurePath);
            });

            repository.workingTree().insert(finfos, DefaultProgressListener.NULL);
        }
    }

    /**
     * Inserts all features currently stored in this object into the working tree.
     */
    public void flushAll() {
        for (String key : map.keySet()) {
            flush(key);
        }
        count = 0;
        map.clear();
    }

}
