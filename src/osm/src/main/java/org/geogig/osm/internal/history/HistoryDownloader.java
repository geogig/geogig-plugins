/* Copyright (c) 2013-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Victor Olaya (Boundless) - initial implementation
 */
package org.geogig.osm.internal.history;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.locationtech.geogig.repository.ProgressListener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;

/**
 *
 */
public class HistoryDownloader {

    private final long initialChangeset;

    private final long finalChangeset;

    private final ChangesetDownloader downloader;

    private Predicate<Changeset> filter = Predicates.alwaysTrue();

    @VisibleForTesting
    public static boolean alwaysResolveRemoteDownloader;

    /**
     * @param osmAPIUrl api url, e.g. {@code http://api.openstreetmap.org/api/0.6},
     *        {@code file:/path/to/downloaded/changesets}
     * @param initialChangeset initial changeset id
     * @param finalChangeset final changeset id
     * @param preserveFiles
     */
    public HistoryDownloader(final String osmAPIUrl, final File downloadFolder,
            long initialChangeset, long finalChangeset, ExecutorService executor) {

        checkArgument(initialChangeset > 0 && initialChangeset <= finalChangeset);

        this.initialChangeset = initialChangeset;
        this.finalChangeset = finalChangeset;
        this.downloader = resolveDownloader(osmAPIUrl, downloadFolder, executor);
    }

    private ChangesetDownloader resolveDownloader(String osmAPIUrl, File downloadFolder,
            ExecutorService executor) {
        URI uri;
        try {
            uri = new URI(osmAPIUrl);
        } catch (Exception e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }
        String scheme = uri.getScheme();
        if (alwaysResolveRemoteDownloader || "http".equals(scheme) || "https".equals(scheme)) {
            return new RemoteChangesetDownloader(osmAPIUrl, downloadFolder, executor);
        }
        if ("file".equals(scheme) || "".equals(scheme) || null == scheme) {
            Path path;
            if (null == scheme) {
                path = Paths.get(osmAPIUrl);
            } else {
                path = Paths.get(uri);
            }
            checkArgument(Files.exists(path), "%s does not exist", osmAPIUrl);
            return new LocalChangesetDownloader(path);
        }
        throw new IllegalArgumentException("Can't resolve URI to a path:" + uri);
    }

    public void setChangesetFilter(Predicate<Changeset> filter) {
        this.filter = filter;
    }

    public void downloadAll(ProgressListener progressListener) {
        RemoteChangesetDownloader downloader = (RemoteChangesetDownloader) this.downloader;
        Range<Long> range = Range.closed(initialChangeset, finalChangeset);
        ContiguousSet<Long> changesetIds = ContiguousSet.create(range, DiscreteDomain.longs());

        progressListener.setDescription(
                "Downloading changesets " + initialChangeset + " to " + finalChangeset + "...");

        final int readTimeoutMinutes = 20;

        final AtomicBoolean abortFlag = new AtomicBoolean();

        List<Future<Long>> futures = new LinkedList<>();
        for (Long changesetId : changesetIds) {
            try {

                Future<Long> future = downloader.download(changesetId, readTimeoutMinutes,
                        abortFlag);
                futures.add(future);
            } catch (IOException e) {
                e.printStackTrace();
                throw Throwables.propagate(e);
            }
        }
        for (Future<Long> f : futures) {
            try {
                Long id = f.get();
                if (-1L == id.longValue()) {
                    continue;
                }
                progressListener.setDescription("Downloaded changeset " + id + ".");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw Throwables.propagate(e);
            }
        }
        progressListener.setDescription("Done!");
    }

    /**
     * @return the next available changeset, or absent if reached the last one
     * @throws IOException
     * @throws InterruptedException
     */
    public Iterator<Changeset> fetchChangesets() {

        Range<Long> range = Range.closed(initialChangeset, finalChangeset);
        ContiguousSet<Long> changesetIds = ContiguousSet.create(range, DiscreteDomain.longs());
        final int fetchSize = 100;
        Iterable<List<Long>> partitions = Iterables.partition(changesetIds, fetchSize);

        final Function<List<Long>, Iterable<Changeset>> asChangesets = (batchIds) -> {
            Iterable<Changeset> changesets = downloader.fetchChangesets(batchIds);
            return changesets;
        };

        Iterable<Iterable<Changeset>> changesets = Iterables.transform(partitions, asChangesets);
        Iterable<Changeset> concat = Iterables.concat(changesets);
        return concat.iterator();
    }

    public void dispose() {
        downloader.dispose();
    }

}
