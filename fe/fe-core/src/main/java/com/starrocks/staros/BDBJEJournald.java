// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.journal.Journal;
import com.staros.journal.Journald;
import com.starrocks.common.Config;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.Util;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriter;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.persist.EditLog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

// wrapper for star manager to use bdbje
public class BDBJEJournald implements Journald {
    private static final String JOURNAL_PREFIX = "starmgr_"; // do not change this string!
    private static final int REPLAY_INTERVAL_MS = 1;
    private static final Logger LOG = LogManager.getLogger(BDBJEJournald.class);

    private BDBJEJournal bdbjeJournal;
    private JournalWriter journalWriter;
    private EditLog editLog;
    private AtomicLong replayedJournalId;
    private Daemon replayer; // TODO: maybe it's better to move this to StarMgr

    public BDBJEJournald(BDBEnvironment environment) {
        BlockingQueue<JournalTask> journalQueue = new ArrayBlockingQueue<JournalTask>(Config.metadata_journal_queue_size);
        bdbjeJournal = new BDBJEJournal(environment);
        bdbjeJournal.setPrefix(JOURNAL_PREFIX);

        journalWriter = new JournalWriter(bdbjeJournal, journalQueue);

        editLog = new EditLog(journalQueue);

        replayedJournalId = new AtomicLong(0L);

        replayer = null;
    }

    public void becomeLeader() {
        // stop replayer
        if (replayer != null) {
            replayer.exit();
            try {
                replayer.join();
            } catch (InterruptedException e) {
                LOG.warn("got exception when stopping the star mgr replayer thread, {}.", e);
            }
            replayer = null;
        }

        // replay all journal and start journal writer
        try {
            bdbjeJournal.open();

            long replayStartTime = System.currentTimeMillis();
            replayTo(-1);
            long replayEndTime = System.currentTimeMillis();
            LOG.info("finish star manager replay in " + (replayEndTime - replayStartTime) + " msec.");

            journalWriter.init(bdbjeJournal.getMaxJournalId());

            journalWriter.startDaemon();
        } catch (Exception e) {
            LOG.warn("star mgr prepare journal failed before becoming leader, {}.", e);
            Util.stdoutWithTime(e.getMessage());
            System.exit(-1);
        }
    }

    public void becomeFollower() {
        // start replayer
        assert replayer == null;
        replayer = new Daemon("star_mgr_replayer", REPLAY_INTERVAL_MS) {
            @Override
            protected void runOneCycle() {
                try {
                    replayTo(-1);
                } catch (Throwable e) {
                    LOG.error("star mgr replayer thread catch an exception when replay journal, {}.", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        LOG.error("star mgr replayer sleep got exception, {}.", e1);
                    }
                }
            }
        };

        replayer.start();
    }

    public void write(Journal journal) throws StarException {
        try {
            editLog.logStarMgrOperation(new StarMgrJournal(journal));
        } catch (Exception e) {
            throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
        }
    }

    public void replayTo(long journalId) throws StarException {
        try {
            replayJournal(journalId);
        } catch (Exception e) {
            throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
        }
    }

    private synchronized boolean replayJournal(long toJournalId) throws JournalException {
        long newToJournalId = toJournalId;
        if (newToJournalId == -1) {
            newToJournalId = bdbjeJournal.getMaxJournalId();
        }
        if (newToJournalId <= replayedJournalId.get()) {
            return false;
        }

        LOG.info("star mgr replayed journal id is {}, replay to journal id is {}.", replayedJournalId.get(), newToJournalId);

        JournalCursor cursor = null;
        try {
            cursor = bdbjeJournal.read(replayedJournalId.get() + 1, newToJournalId);
        } catch (JournalException e) {
            LOG.warn("star mgr replayer failed to get cursor from {} to {}, {}.", replayedJournalId.get() + 1, newToJournalId, e);
            return false;
        }

        long startTime = System.currentTimeMillis();
        boolean hasLog = false;
        while (true) {
            JournalEntity entity = null;
            try {
                entity = cursor.next();
            } catch (InterruptedException | JournalInconsistentException e) {
                LOG.warn("star mgr replayer got interrupt exception or inconsistent exception when get next, {}, will exit.", e);
                // TODO exit gracefully
                Util.stdoutWithTime(e.getMessage());
                System.exit(-1);
            }

            // EOF or aggressive retry
            if (entity == null) {
                break;
            }

            hasLog = true;
            EditLog.loadJournal(null /* GlobalStateMgr */, entity);
            replayedJournalId.incrementAndGet();

            LOG.debug("star mgr journal {} replayed.", replayedJournalId);
        }
        long cost = System.currentTimeMillis() - startTime;
        if (cost >= 1000) {
            LOG.warn("star mgr replay journal cost too much time: {} replayedJournalId: {}.", cost, replayedJournalId);
        }

        return hasLog;
    }
}

