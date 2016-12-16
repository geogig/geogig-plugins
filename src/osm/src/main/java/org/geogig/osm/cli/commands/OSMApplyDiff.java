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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.geogig.osm.internal.OSMApplyDiffOp;
import org.geogig.osm.internal.OSMReport;
import org.locationtech.geogig.cli.AbstractCommand;
import org.locationtech.geogig.cli.CLICommand;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.GeogigCLI;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * Imports data from an OSM file
 */
@Parameters(commandNames = "apply-diff", commandDescription = "Apply a OSM diff file to OSM data in the current repo")
public class OSMApplyDiff extends AbstractCommand implements CLICommand {

    @Parameter(arity = 1, description = "OSM diff file path", required = true)
    public List<String> diffFilepath = Lists.newArrayList();

    @Override
    protected void runInternal(GeogigCLI cli) throws IOException {
        checkParameter(diffFilepath != null && diffFilepath.size() == 1,
                "One file must be specified");
        File diffFile = new File(diffFilepath.get(0));
        checkParameter(diffFile.exists(), "The specified OSM diff file does not exist");

        try {
            Optional<OSMReport> report = cli.getGeogig().command(OSMApplyDiffOp.class)
                    .setDiffFile(diffFile).setProgressListener(cli.getProgressListener()).call();
            if (report.isPresent()) {
                OSMReport rep = report.get();
                String msg;
                if (rep.getUnpprocessedCount() > 0) {
                    msg = String
                            .format("\nSome diffs from the specified file were not applied.\n"
                                    + "Total Items in Diff: %,d.\nUnprocessed items: %,d.\nTotal Nodes in diff: %,d.\nTotal Ways in diff: %,d.\n",
                                    rep.getCount(), rep.getUnpprocessedCount(), rep.getNodeCount(),
                                    rep.getWayCount());
                } else {
                    msg = String.format("\nAll items processed.  Total items: %,d.\n Nodes: %,d.\n Ways: %,d\n",
                            rep.getCount(), rep.getNodeCount(), rep.getWayCount());
                }
                cli.getConsole().println(msg);
            }

        } catch (RuntimeException e) {
            throw new CommandFailedException("Error importing OSM data: " + e.getMessage(), e);
        }

    }
}
