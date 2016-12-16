/* Copyright (c) 2012-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.geogig.osm.cli;

import org.geogig.osm.cli.commands.CreateOSMChangeset;
import org.geogig.osm.cli.commands.OSMApplyDiff;
import org.geogig.osm.cli.commands.OSMDownload;
import org.geogig.osm.cli.commands.OSMExport;
import org.geogig.osm.cli.commands.OSMExportPG;
import org.geogig.osm.cli.commands.OSMExportShp;
import org.geogig.osm.cli.commands.OSMHistoryImport;
import org.geogig.osm.cli.commands.OSMImport;
import org.geogig.osm.cli.commands.OSMMap;
import org.geogig.osm.cli.commands.OSMUnmap;
import org.locationtech.geogig.cli.CLICommandExtension;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

/**
 * {@link CLICommandExtension} that provides a {@link JCommander} for osm specific commands.
 * 
 * @see OSMHistoryImport
 */
@Parameters(commandNames = "osm", commandDescription = "GeoGig/OpenStreetMap integration utilities")
public class OSMCommandProxy implements CLICommandExtension {

    @Override
    public JCommander getCommandParser() {
        JCommander commander = new JCommander();
        commander.setProgramName("geogig osm");
        commander.addCommand("import-history", new OSMHistoryImport());
        commander.addCommand("import", new OSMImport());
        commander.addCommand("export", new OSMExport());
        commander.addCommand("download", new OSMDownload());
        commander.addCommand("create-changeset", new CreateOSMChangeset());
        commander.addCommand("map", new OSMMap());
        commander.addCommand("unmap", new OSMUnmap());
        commander.addCommand("export-shp", new OSMExportShp());
        commander.addCommand("export-pg", new OSMExportPG());
        commander.addCommand("apply-diff", new OSMApplyDiff());
        return commander;
    }
}
