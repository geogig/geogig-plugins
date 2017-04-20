package org.geogig.osm.internal.history;

import java.util.List;

interface ChangesetDownloader {

    List<Changeset> fetchChangesets(List<Long> batchIds);

    void dispose();

}