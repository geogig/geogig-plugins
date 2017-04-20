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

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

/**
 * 
 * @see ChangesetScanner
 * @see ChangesetContentsScanner
 */
class LocalChangesetDownloader implements ChangesetDownloader {

    private final Path baseDirectory;

    private @Nullable FileSystem zipFilesystem;

    public LocalChangesetDownloader(Path contents) {
        if (Files.isRegularFile(contents)) {
            // it must be a zipfile

            Map<String, String> env = new HashMap<>();
            env.put("create", "false");
            // locate file system by using the syntax
            // defined in java.net.JarURLConnection
            String str = "jar:file:" + contents.toAbsolutePath().toString();
            System.err.println("creating URI: " + str);
            URI uri = URI.create(str);
            System.err.println("Parsing from " + uri);
            try {
                zipFilesystem = FileSystems.newFileSystem(uri, env);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            baseDirectory = zipFilesystem.getRootDirectories().iterator().next();
        } else {
            System.err.println("importing from directory " + contents.toString());
            Preconditions.checkArgument(Files.isDirectory(contents));
            baseDirectory = contents;
        }
    }

    @Override
    public void dispose() {
        if (zipFilesystem != null) {
            try {
                zipFilesystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public List<Changeset> fetchChangesets(List<Long> batchIds) {

        List<Changeset> changesets = new ArrayList<Changeset>(batchIds.size());

        for (Long changesetId : batchIds) {
            Path changesetPath = changesetPath(changesetId);
            Path changesPath = changesPath(changesetId);
            checkState(java.nio.file.Files.exists(changesetPath));
            checkState(java.nio.file.Files.exists(changesPath));

            Changeset changeset;

            try (InputStream in = java.nio.file.Files.newInputStream(changesetPath)) {
                ChangesetScanner scanner = new ChangesetScanner(in);
                changeset = scanner.parseNext();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }

            changeset.setChanges(new ChangesSupplier(changesPath));

            changesets.add(changeset);
        }

        return changesets;
    }

    private static class ChangesSupplier implements Supplier<Optional<Iterator<Change>>> {

        private Path changesPath;

        public ChangesSupplier(Path changesPath) {
            this.changesPath = changesPath;
        }

        @Override
        public Optional<Iterator<Change>> get() {

            List<Change> changes = new LinkedList<>();
            ChangesetContentsScanner changesScanner = new ChangesetContentsScanner();
            try (InputStream stream = Files.newInputStream(changesPath)) {
                Iterator<Change> it = changesScanner.parse(stream);
                it.forEachRemaining((c) -> changes.add(c));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            return Optional.of(changes.iterator());
        }

    }

    private Path changesetPath(long changesetId) {
        return baseDirectory.resolve(changesetId + ".xml");
    }

    private Path changesPath(long changesetId) {
        return baseDirectory.resolve(String.valueOf(changesetId)).resolve("download.xml");
    }

}
