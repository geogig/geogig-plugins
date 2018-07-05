/* Copyright (c) 2014-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.geogig.osm.internal;

import java.io.Serializable;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import com.google.common.base.Preconditions;

/**
 * A {@link CoordinateSequenceFactory} that produces packed sequences of coordinates as
 * {@code int[]} as per {@link OSMCoordinateSequence}
 */
public class OSMCoordinateSequenceFactory implements CoordinateSequenceFactory, Serializable {

    private static final long serialVersionUID = -1245500277641319804L;

    private static final OSMCoordinateSequenceFactory INSTANCE = new OSMCoordinateSequenceFactory();

    @Override
    public OSMCoordinateSequence create(Coordinate[] coordinates) {
        return new OSMCoordinateSequence(coordinates);
    }

    @Override
    public CoordinateSequence create(CoordinateSequence coordSeq) {
        return new CoordinateArraySequence(coordSeq);
    }

    @Override
    public OSMCoordinateSequence create(int size, int dimension) {
        Preconditions.checkArgument(dimension == 2);
        return create(size);
    }

    public OSMCoordinateSequence create(int size) {
        return new OSMCoordinateSequence(size);
    }

    public static OSMCoordinateSequenceFactory instance() {
        return INSTANCE;
    }

}
