// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.journal.JournalSystem;
import com.staros.manager.StarManager;
import com.staros.manager.StarManagerServer;
import com.staros.util.GlobalIdGenerator;
import com.starrocks.common.Config;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StarMgrServer {
    private static final int REPLAY_INTERVAL_MS = 1;
    private static final Logger LOG = LogManager.getLogger(StarMgrServer.class);

    private static StarMgrServer CHECKPOINT = null;
    private static long checkpointThreadId = -1;
    private static class SingletonHolder {
        private static final StarMgrServer INSTANCE = new StarMgrServer();
    }

    public static boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public static StarMgrServer getCurrentState() {
        if (isCheckpointThread()) {
            // only checkpoint thread it self will goes here.
            // so no need to care about thread safe.
            if (CHECKPOINT == null) {
                CHECKPOINT = new StarMgrServer(); // TODO: do we need a new one every time?
            }
            return CHECKPOINT;
        } else {
            return SingletonHolder.INSTANCE;
        }
    }

    private StarManagerServer starMgrServer;

    public StarMgrServer() {
        starMgrServer = new StarManagerServer();
    }

    // FOR TEST
    public StarMgrServer(StarManagerServer server) {
        starMgrServer = server;
    }

    public StarManager getStarMgr() {
        return starMgrServer.getStarManager();
    }

    public void initialize(BDBEnvironment environment) throws IOException {
        BDBJEJournald journald = new BDBJEJournald(environment);

        String[] starMgrAddr = Config.starmgr_address.split(":");
        if (starMgrAddr.length != 2) {
            LOG.fatal("Config.starmgr_address {} bad format.", Config.starmgr_address);
            System.exit(-1);
        }
        int port = Integer.parseInt(starMgrAddr[1]);

        if (Config.starmgr_s3_bucket.isEmpty()) {
            LOG.fatal("Config.starmgr_s3_bucket is not set.");
            System.exit(-1);
        }

        // necessary starMgr config setting
        com.staros.util.Config.STARMGR_IP = FrontendOptions.getLocalHostAddress();
        com.staros.util.Config.STARMGR_RPC_PORT = port;
        com.staros.util.Config.S3_BUCKET = Config.starmgr_s3_bucket;
        com.staros.util.Config.S3_REGION = Config.starmgr_s3_region;
        com.staros.util.Config.S3_ENDPOINT = Config.starmgr_s3_endpoint;
        com.staros.util.Config.S3_AK = Config.starmgr_s3_ak;
        com.staros.util.Config.S3_SK = Config.starmgr_s3_sk;

        JournalSystem.overrideJournald(journald);

        StarMgrIdGenerator generator =
                new StarMgrIdGenerator(GlobalStateMgr.getCurrentState().getIdGenerator());
        GlobalIdGenerator.overrideIdGenerator(generator);

        starMgrServer.start(com.staros.util.Config.STARMGR_RPC_PORT);
    }

    public void startBackgroundThreads() {
        getStarMgr().start();
    }

    public void stopBackgroundThreads() {
        getStarMgr().stop();
    }

    public void dumpMeta(DataOutputStream out) throws IOException {
        getStarMgr().dumpMeta(out);
    }

    public void loadMeta(DataInputStream in) throws IOException {
        getStarMgr().loadMeta(in);
    }
}
