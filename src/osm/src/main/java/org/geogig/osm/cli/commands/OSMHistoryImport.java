/* Copyright (c) 2013-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Victor Olaya (Boundless) - initial implementation
 */
package org.geogig.osm.cli.commands;

import static org.geogig.osm.internal.OSMUtils.NODE_TYPE_NAME;
import static org.geogig.osm.internal.OSMUtils.WAY_TYPE_NAME;
import static org.geogig.osm.internal.OSMUtils.nodeType;
import static org.geogig.osm.internal.OSMUtils.wayType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.relation.Relation;

import org.eclipse.jdt.annotation.Nullable;
import org.fusesource.jansi.Ansi.Color;
import org.geogig.osm.internal.OSMUtils;
import org.geogig.osm.internal.history.Change;
import org.geogig.osm.internal.history.Changeset;
import org.geogig.osm.internal.history.HistoryDownloader;
import org.geogig.osm.internal.history.Node;
import org.geogig.osm.internal.history.Primitive;
import org.geogig.osm.internal.history.Way;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geogig.cli.AbstractCommand;
import org.locationtech.geogig.cli.CLICommand;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.Console;
import org.locationtech.geogig.cli.GeogigCLI;
import org.locationtech.geogig.cli.InvalidParameterException;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.model.SymRef;
import org.locationtech.geogig.model.impl.RevFeatureBuilder;
import org.locationtech.geogig.model.impl.RevFeatureTypeBuilder;
import org.locationtech.geogig.plumbing.FindTreeChild;
import org.locationtech.geogig.plumbing.RefParse;
import org.locationtech.geogig.plumbing.ResolveTreeish;
import org.locationtech.geogig.porcelain.AddOp;
import org.locationtech.geogig.porcelain.CommitOp;
import org.locationtech.geogig.porcelain.index.CreateQuadTree;
import org.locationtech.geogig.porcelain.index.Index;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.DefaultProgressListener;
import org.locationtech.geogig.repository.FeatureInfo;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.repository.ProgressListener;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.WorkingTree;
import org.locationtech.geogig.repository.impl.GeoGIG;
import org.locationtech.geogig.storage.BlobStore;
import org.locationtech.geogig.storage.IndexDatabase;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.impl.Blobs;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 *
 */
@Parameters(commandNames = "import-history", commandDescription = "Import OpenStreetmap history")
public class OSMHistoryImport extends AbstractCommand implements CLICommand {

    private static final GeometryFactory GEOMF = new GeometryFactory();

    final RevFeatureType noderft = RevFeatureTypeBuilder.build(OSMUtils.nodeType());

    final RevFeatureType wayrft = RevFeatureTypeBuilder.build(OSMUtils.wayType());

    private static class SilentProgressListener extends DefaultProgressListener {
        private ProgressListener subject;

        public SilentProgressListener(ProgressListener subject) {
            this.subject = subject;
        }

        @Override
        public void cancel() {
            subject.cancel();
        }

        @Override
        public boolean isCanceled() {
            return subject.isCanceled();
        }
    }

    @ParametersDelegate
    public HistoryImportArgs args = new HistoryImportArgs();

    private SilentProgressListener silentListener;

    @Override
    protected void runInternal(GeogigCLI cli) throws IOException {
        checkParameter(args.numThreads > 0 && args.numThreads < 7,
                "numthreads must be between 1 and 6");
        silentListener = new SilentProgressListener(cli.getProgressListener());

        Console console = cli.getConsole();

        final String osmAPIUrl = resolveAPIURL();
        final File targetDir = resolveTargetDir(cli.getPlatform());

        final long startIndex;
        final long endIndex = args.endIndex;
        if (args.resume) {
            GeoGIG geogig = cli.getGeogig();
            if (args.downloadOnly) {
                startIndex = args.startIndex;
            } else {
                long lastChangeset = getCurrentBranchChangeset(geogig);
                startIndex = 1 + lastChangeset;
            }
        } else {
            startIndex = args.startIndex;
        }
        console.println(String.format("Obtaining OSM changesets %,d to %,d from %s", startIndex,
                args.endIndex, osmAPIUrl));

        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("osm-history-fetch-thread-%d").build();
        final ExecutorService executor = Executors.newFixedThreadPool(args.numThreads,
                threadFactory);
        console.flush();

        HistoryDownloader downloader;
        downloader = new HistoryDownloader(osmAPIUrl, targetDir, startIndex, endIndex, executor);

        Envelope env = parseBbox();
        Predicate<Changeset> filter = parseFilter(env);
        downloader.setChangesetFilter(filter);
        try {
            if (args.downloadOnly) {
                downloader.downloadAll(silentListener);
            } else {
                importOsmHistory(cli, console, downloader, env);
            }
        } finally {
            downloader.dispose();
            executor.shutdownNow();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new CommandFailedException(e);
            }
        }
    }

    private Predicate<Changeset> parseFilter(Envelope env) {
        if (env == null) {
            return Predicates.alwaysTrue();
        }
        BBoxFiler filter = new BBoxFiler(env);
        return filter;
    }

    private Envelope parseBbox() {
        final String bbox = args.bbox;
        if (bbox != null) {
            String[] split = bbox.split(",");
            checkParameter(split.length == 4,
                    String.format("Invalid bbox format: '%s'. Expected minx,miny,maxx,maxy", bbox));
            try {
                double x1 = Double.parseDouble(split[0]);
                double y1 = Double.parseDouble(split[1]);
                double x2 = Double.parseDouble(split[2]);
                double y2 = Double.parseDouble(split[3]);
                Envelope envelope = new Envelope(x1, x2, y1, y2);
                checkParameter(!envelope.isNull(), "Provided envelope is nil");
                return envelope;
            } catch (NumberFormatException e) {
                String message = String.format(
                        "One or more bbox coordinate can't be parsed to double: '%s'", bbox);
                throw new InvalidParameterException(message, e);
            }
        }
        return null;
    }

    private static class BBoxFiler implements Predicate<Changeset> {

        private Envelope envelope;

        public BBoxFiler(Envelope envelope) {
            this.envelope = envelope;
        }

        @Override
        public boolean apply(Changeset input) {
            Optional<Envelope> wgs84Bounds = input.getWgs84Bounds();
            return wgs84Bounds.isPresent() && envelope.intersects(wgs84Bounds.get());
        }

    }

    private File resolveTargetDir(Platform platform) throws IOException {
        final File targetDir;
        if (args.saveFolder == null) {
            try {
                final File tempDir = platform.getTempDir();
                Preconditions.checkState(tempDir.isDirectory() && tempDir.canWrite());
                File tmp = null;
                for (int i = 0; i < 1000; i++) {
                    tmp = new File(tempDir, "osmchangesets_" + i);
                    if (tmp.mkdir()) {
                        break;
                    }
                    i++;
                }
                targetDir = tmp;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            if (!args.saveFolder.exists() && !args.saveFolder.mkdirs()) {
                throw new IllegalArgumentException(
                        "Unable to create directory " + args.saveFolder.getAbsolutePath());
            }
            targetDir = args.saveFolder;
        }
        return targetDir;
    }

    private String resolveAPIURL() {
        String osmAPIUrl;
        if (args.useTestApiEndpoint) {
            osmAPIUrl = HistoryImportArgs.DEVELOPMENT_API_ENDPOINT;
        } else if (args.apiUrl.isEmpty()) {
            osmAPIUrl = HistoryImportArgs.DEFAULT_API_ENDPOINT;
        } else {
            osmAPIUrl = args.apiUrl.get(0);
        }
        return osmAPIUrl;
    }

    private void importOsmHistory(GeogigCLI cli, Console console, HistoryDownloader downloader,
            @Nullable Envelope featureFilter) throws IOException {

        ensureTypesExist(cli);

        Iterator<Changeset> changesets = downloader.fetchChangesets();

        GeoGIG geogig = cli.getGeogig();

        boolean initialized = false;
        Stopwatch sw = Stopwatch.createUnstarted();

        while (changesets.hasNext() && !silentListener.isCanceled()) {
            sw.reset().start();
            Changeset changeset = changesets.next();
            if (changeset.isOpen()) {
                throw new CommandFailedException("Can't import past changeset " + changeset.getId()
                        + " as it is still open.");
            }
            String desc = String.format("obtaining osm changeset %,d...", changeset.getId());
            console.print(desc);
            console.flush();

            Optional<Iterator<Change>> opchanges = changeset.getChanges().get();
            if (!opchanges.isPresent()) {
                updateBranchChangeset(geogig, changeset.getId());
                console.println(" does not apply.");
                console.flush();
                sw.stop();
                continue;
            }
            Iterator<Change> changes = opchanges.get();
            console.print(" inserting...");
            console.flush();

            long changeCount = insertChanges(cli, changes, featureFilter);
            if (!silentListener.isCanceled()) {
                console.print(String.format(" Applied %,d changes, staging...", changeCount));
                console.flush();
                geogig.command(AddOp.class).setProgressListener(silentListener).call();
                commit(cli, changeset);

                if (args.autoIndex && !initialized) {
                    initializeIndex(cli);
                    initialized = true;
                }
            }
            console.println(String.format(" (%s)", sw.stop()));
            console.flush();
        }
    }

    private void ensureTypesExist(GeogigCLI cli) throws IOException {
        Repository repo = cli.getGeogig().getRepository();
        WorkingTree workingTree = repo.workingTree();
        ImmutableMap<String, NodeRef> featureTypeTrees = Maps
                .uniqueIndex(workingTree.getFeatureTypeTrees(), (nr) -> nr.path());

        if (!featureTypeTrees.containsKey(WAY_TYPE_NAME)) {
            workingTree.createTypeTree(WAY_TYPE_NAME, wayType());
        }
        if (!featureTypeTrees.containsKey(NODE_TYPE_NAME)) {
            workingTree.createTypeTree(NODE_TYPE_NAME, nodeType());
        }
        repo.command(AddOp.class).call();
    }

    private void initializeIndex(GeogigCLI cli) throws IOException {
        Repository repo = cli.getGeogig().getRepository();

        IndexDatabase indexdb = repo.indexDatabase();
        if (indexdb.getIndexInfos(WAY_TYPE_NAME).isEmpty()) {
            createIndex(cli, WAY_TYPE_NAME);
        }
        if (indexdb.getIndexInfos(NODE_TYPE_NAME).isEmpty()) {
            createIndex(cli, NODE_TYPE_NAME);
        }
    }

    private void createIndex(GeogigCLI cli, String typeName) throws IOException {
        cli.getConsole().println("Creating initial index for " + typeName + "...");
        Repository repo = cli.getGeogig().getRepository();
        Index index = repo.command(CreateQuadTree.class).setIndexHistory(true)
                .setTreeRefSpec(typeName).call();
        cli.getConsole().println("Initial index created: " + index);
    }

    /**
     * @param cli
     * @param changeset
     * @throws IOException
     */
    private void commit(GeogigCLI cli, Changeset changeset) throws IOException {
        Preconditions.checkArgument(!changeset.isOpen());
        Console console = cli.getConsole();
        console.print(" Committing...");
        console.flush();

        GeoGIG geogig = cli.getGeogig();
        CommitOp command = geogig.command(CommitOp.class);
        command.setAllowEmpty(true);
        command.setProgressListener(silentListener);
        String message = "";
        if (changeset.getComment().isPresent()) {
            message = changeset.getComment().get() + "\nchangeset " + changeset.getId();
        } else {
            message = "changeset " + changeset.getId();
        }
        command.setMessage(message);
        final @Nullable String userName = changeset.getUserName();
        command.setAuthor(userName, null);
        command.setAuthorTimestamp(changeset.getCreated());
        command.setAuthorTimeZoneOffset(0);// osm timestamps are in GMT

        if (userName == null) {
            command.setCommitter("anonymous", null);
        } else {
            command.setCommitter(userName, null);
        }

        command.setCommitterTimestamp(changeset.getClosed().get());
        command.setCommitterTimeZoneOffset(0);// osm timestamps are in GMT

        if (silentListener.isCanceled()) {
            return;
        }
        try {
            RevCommit commit = command.call();
            Ref head = geogig.command(RefParse.class).setName(Ref.HEAD).call().get();
            Preconditions.checkState(commit.getId().equals(head.getObjectId()));
            updateBranchChangeset(geogig, changeset.getId());
            String commitStr = newAnsi(console).fg(Color.YELLOW)
                    .a(commit.getId().toString().substring(0, 8)).reset().toString();
            console.print(" Commit ");
            console.print(commitStr);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * @param geogig
     * @param id
     * @throws IOException
     */
    private void updateBranchChangeset(GeoGIG geogig, long id) throws IOException {
        String path = getBranchChangesetPath(geogig);
        BlobStore blobStore = geogig.getContext().blobStore();
        Blobs.putBlob(blobStore, path, String.valueOf(id));
    }

    private long getCurrentBranchChangeset(GeoGIG geogig) throws IOException {
        String path = getBranchChangesetPath(geogig);

        BlobStore blobStore = geogig.getContext().blobStore();

        Optional<String> blob = Blobs.getBlobAsString(blobStore, path);

        return blob.isPresent() ? Long.parseLong(blob.get()) : 0L;
    }

    private String getBranchChangesetPath(GeoGIG geogig) {
        final String branch = getHead(geogig).getTarget();
        String path = "osm/" + branch;
        return path;
    }

    private SymRef getHead(GeoGIG geogig) {
        final Ref currentHead = geogig.command(RefParse.class).setName(Ref.HEAD).call().get();
        if (!(currentHead instanceof SymRef)) {
            throw new CommandFailedException("Cannot run on a dettached HEAD");
        }
        return (SymRef) currentHead;
    }

    /**
     * @param cli
     * @param changes
     * @param featureFilter
     * @throws IOException
     */
    private long insertChanges(GeogigCLI cli, final Iterator<Change> changes,
            @Nullable Envelope featureFilter) throws IOException {

        final GeoGIG geogig = cli.getGeogig();
        final Context context = geogig.getContext().snapshot();
        final WorkingTree workTree = context.workingTree();

        Map<Long, Coordinate> thisChangePointCache = new LinkedHashMap<Long, Coordinate>() {
            /** serialVersionUID */
            private static final long serialVersionUID = 1277795218777240552L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Coordinate> eldest) {
                return size() == 10000;
            }
        };

        long cnt = 0;

        List<FeatureInfo> features = new ArrayList<>();

        while (changes.hasNext() && !silentListener.isCanceled()) {
            Change change = changes.next();
            final String featurePath = featurePath(change);
            if (featurePath == null) {
                continue;// ignores relations
            }
            final String parentPath = NodeRef.parentPath(featurePath);
            if (Change.Type.delete.equals(change.getType())) {
                cnt++;
                features.add(FeatureInfo.delete(featurePath));
            } else {
                final Primitive primitive = change.getNode().isPresent() ? change.getNode().get()
                        : change.getWay().get();
                final Geometry geom = parseGeometry(context, primitive, thisChangePointCache);
                if (geom instanceof Point) {
                    thisChangePointCache.put(Long.valueOf(primitive.getId()),
                            ((Point) geom).getCoordinate());
                }

                SimpleFeature feature = toFeature(primitive, geom);

                if (featureFilter == null
                        || featureFilter.intersects((Envelope) feature.getBounds())) {

                    RevFeatureType rft = NODE_TYPE_NAME.equals(parentPath) ? noderft : wayrft;
                    String path = NodeRef.appendChild(parentPath, feature.getID());
                    FeatureInfo fi = FeatureInfo.insert(RevFeatureBuilder.build(feature),
                            rft.getId(), path);
                    features.add(fi);
                    cnt++;
                }
            }
        }

        if (silentListener.isCanceled()) {
            return -1;
        }

//        System.err.printf("\nInserting %,d changes\n", features.size());
//        Stopwatch sw = Stopwatch.createStarted();
        workTree.insert(features.iterator(), DefaultProgressListener.NULL);
//        System.err.printf("workTree.insert: %s\n", sw.stop());

        return cnt;
    }

    /**
     * @param primitive
     * @param thisChangePointCache
     * @return
     */
    private Geometry parseGeometry(Context context, Primitive primitive,
            Map<Long, Coordinate> thisChangePointCache) {

        if (primitive instanceof Relation) {
            return null;
        }

        if (primitive instanceof Node) {
            Optional<Point> location = ((Node) primitive).getLocation();
            return location.orNull();
        }

        final Way way = (Way) primitive;
        final ImmutableList<Long> nodes = way.getNodes();

        List<Coordinate> coordinates = Lists.newArrayList(nodes.size());
        FindTreeChild findTreeChild = context.command(FindTreeChild.class);
        Optional<ObjectId> nodesTreeId = context.command(ResolveTreeish.class)
                .setTreeish(Ref.STAGE_HEAD + ":" + NODE_TYPE_NAME).call();
        if (nodesTreeId.isPresent()) {
            RevTree headTree = context.objectDatabase().getTree(nodesTreeId.get());
            findTreeChild.setParent(headTree);
        }
        int findTreeChildCalls = 0;
        Stopwatch findTreeChildSW = Stopwatch.createUnstarted();
        ObjectStore objectDatabase = context.objectDatabase();
        for (Long nodeId : nodes) {
            Coordinate coord = thisChangePointCache.get(nodeId);
            if (coord == null) {
                findTreeChildCalls++;
                String fid = String.valueOf(nodeId);
                findTreeChildSW.start();
                Optional<NodeRef> nodeRef = findTreeChild.setChildPath(fid).call();
                findTreeChildSW.stop();
                Optional<org.locationtech.geogig.model.Node> ref = Optional.absent();
                if (nodeRef.isPresent()) {
                    ref = Optional.of(nodeRef.get().getNode());
                }

                if (ref.isPresent()) {
                    final int locationAttIndex = 6;
                    ObjectId objectId = ref.get().getObjectId();
                    RevFeature revFeature = objectDatabase.getFeature(objectId);
                    Point p = (Point) revFeature.get(locationAttIndex, GEOMF).orNull();
                    if (p != null) {
                        coord = p.getCoordinate();
                        thisChangePointCache.put(Long.valueOf(nodeId), coord);
                    }
                }
            }
            if (coord != null) {
                coordinates.add(coord);
            }
        }
        if (findTreeChildCalls > 0) {
//            System.err.printf("%,d findTreeChild calls (%s)\n", findTreeChildCalls,
//                    findTreeChildSW);
        }
        if (coordinates.size() < 2) {
            return null;
        }
        return GEOMF.createLineString(coordinates.toArray(new Coordinate[coordinates.size()]));
    }

    /**
     * @param change
     * @return
     */
    private String featurePath(Change change) {
        if (change.getRelation().isPresent()) {
            return null;// ignore relations for the time being
        }
        if (change.getNode().isPresent()) {
            String fid = String.valueOf(change.getNode().get().getId());
            return NodeRef.appendChild(NODE_TYPE_NAME, fid);
        }
        String fid = String.valueOf(change.getWay().get().getId());
        return NodeRef.appendChild(WAY_TYPE_NAME, fid);
    }

    private static SimpleFeature toFeature(Primitive feature, Geometry geom) {

        SimpleFeatureType ft = feature instanceof Node ? nodeType() : wayType();
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(ft);

        // "visible:Boolean,version:Int,timestamp:long,[location:Point |
        // way:LineString];
        builder.set("visible", Boolean.valueOf(feature.isVisible()));
        builder.set("version", Integer.valueOf(feature.getVersion()));
        builder.set("timestamp", Long.valueOf(feature.getTimestamp()));
        builder.set("changeset", Long.valueOf(feature.getChangesetId()));

        Map<String, String> tags = feature.getTags();
        builder.set("tags", tags);

        String user = feature.getUserName() + ":" + feature.getUserId();
        builder.set("user", user);

        if (feature instanceof Node) {
            builder.set("location", geom);
        } else if (feature instanceof Way) {
            builder.set("way", geom);
            long[] nodes = buildNodesArray(((Way) feature).getNodes());
            builder.set("nodes", nodes);
        } else {
            throw new IllegalArgumentException();
        }

        String fid = String.valueOf(feature.getId());
        SimpleFeature simpleFeature = builder.buildFeature(fid);
        return simpleFeature;
    }

    private static long[] buildNodesArray(List<Long> nodeIds) {
        long[] nodes = new long[nodeIds.size()];
        for (int i = 0; i < nodeIds.size(); i++) {
            nodes[i] = nodeIds.get(i).longValue();
        }
        return nodes;
    }
}
