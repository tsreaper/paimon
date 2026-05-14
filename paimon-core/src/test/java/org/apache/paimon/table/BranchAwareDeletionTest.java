/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for branch-aware tag and snapshot deletion.
 *
 * <p>Branches share physical data and index files but maintain independent snapshot lineages.
 * Without cross-branch protection, deleting a tag (or expiring a snapshot) on one branch can
 * over-delete files still referenced by another branch's lineage. These tests pin that the
 * protection actually fires.
 */
public class BranchAwareDeletionTest extends PrimaryKeyTableTestBase {

    @Override
    protected Options tableOptions() {
        Options options = new Options();
        // Dynamic bucket so prepareTable produces HASH_INDEX files we can track. The same
        // skipping logic applies to DELETION_VECTORS index files; we use HASH_INDEX here only
        // because it is cheaper to provoke index file churn.
        options.set(CoreOptions.BUCKET, -1);
        return options;
    }

    @Test
    public void testDeleteTagDoesNotRemoveIndexFilesReferencedByBranch() throws Exception {
        prepareTable();
        // Snapshot lineage looks like: 1..6. Index files churn across them (see prepareTable).
        long branchedSnapshot = 3L;

        // Pin snapshot 3 with a tag, then fork a branch from that tag. The branch's lineage is
        // frozen at snapshot 3; main keeps advancing.
        table.createTag("fork", branchedSnapshot);
        table.createBranch("rt", "fork");

        // The shared physical files referenced by snapshot 3's index manifest must stay alive as
        // long as the branch points at snapshot 3.
        List<IndexManifestEntry> branchIndexEntries =
                readIndexManifest(table.snapshotManager().snapshot(branchedSnapshot));
        assertThat(branchIndexEntries).isNotEmpty();

        // Expire everything on main except the tag-pinned snapshot, then drop that tag too.
        // Pre-fix this is where shared index files were getting deleted.
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 7);
        table.deleteTag("fork");

        // All index files that the branch's snapshot still points at must remain on disk.
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        for (IndexManifestEntry entry : branchIndexEntries) {
            assertThat(indexFileHandler.existsIndexFile(entry))
                    .as(
                            "Index file %s referenced by branch 'rt' must not be deleted by main's tag/snapshot cleanup",
                            entry.indexFile().fileName())
                    .isTrue();
        }

        // The branch's own snapshot view should still be readable end-to-end.
        FileStoreTable branchTable = table.switchToBranch("rt");
        Snapshot branchLatest = branchTable.snapshotManager().latestSnapshot();
        assertThat(branchLatest).isNotNull();
        for (IndexManifestEntry entry : readIndexManifest(branchLatest)) {
            assertThat(indexFileHandler.existsIndexFile(entry))
                    .as(
                            "Index file %s referenced by branch's latest snapshot must exist",
                            entry.indexFile().fileName())
                    .isTrue();
        }
    }

    @Test
    public void testExpireSnapshotsDoesNotRemoveIndexFilesReferencedByBranch() throws Exception {
        prepareTable();
        long branchedSnapshot = 3L;

        // Branch off snapshot 3 (no tag involvement), then continue on main.
        table.createTag("fork", branchedSnapshot);
        table.createBranch("rt", "fork");
        // Drop the tag immediately — the branch alone is what should protect snapshot 3's files.
        // Without cross-branch awareness, expiring snapshots later would delete them.
        // (Note: deleteTag here only removes the tag metadata, since snapshot 3 still exists in
        // main's lineage at this point.)
        table.deleteTag("fork");

        List<IndexManifestEntry> branchIndexEntries =
                readIndexManifest(table.snapshotManager().snapshot(branchedSnapshot));
        assertThat(branchIndexEntries).isNotEmpty();

        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 7);

        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        for (IndexManifestEntry entry : branchIndexEntries) {
            assertThat(indexFileHandler.existsIndexFile(entry))
                    .as(
                            "Index file %s referenced by branch 'rt' must survive main snapshot expiration",
                            entry.indexFile().fileName())
                    .isTrue();
        }
    }

    @Test
    public void testTagManagerIsBranchAware() throws Exception {
        // Direct unit test on TagManager: the snapshotExists short-circuit should also check
        // other branches, so a tag pointing at a snapshot still live on a branch keeps its files.
        prepareTable();
        table.createTag("fork", 3L);
        table.createBranch("rt", "fork");

        // Expire snapshot 3 from main (the branch still has it). Then delete the tag.
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 7);

        // Sanity: snapshot 3 is gone from main but still exists on the branch.
        assertThat(table.snapshotManager().snapshotExists(3L)).isFalse();
        FileStoreTable branchTable = table.switchToBranch("rt");
        assertThat(branchTable.snapshotManager().snapshotExists(3L)).isTrue();

        TagManager tagManager = table.tagManager();
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();

        // Snapshot 3's index files are what we want to keep safe.
        List<IndexManifestEntry> branchIndexEntries =
                indexFileHandler.readManifest(
                        branchTable.snapshotManager().snapshot(3L).indexManifest());

        // Deleting the tag goes through the short-circuit because snapshot 3 is referenced by rt.
        tagManager.deleteTag(
                "fork",
                table.store().newTagDeletion(),
                table.snapshotManager(),
                table.store().createTagCallbacks(table));

        for (IndexManifestEntry entry : branchIndexEntries) {
            assertThat(indexFileHandler.existsIndexFile(entry))
                    .as(
                            "Branch-aware tag deletion must preserve index file %s",
                            entry.indexFile().fileName())
                    .isTrue();
        }
    }

    private void prepareTable() throws Exception {
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();

        // commit bucket 1,2,3
        write(write, mkRow(1, 1, 1, 1));
        write(write, mkRow(2, 2, 2, 2));
        write(write, mkRow(3, 3, 3, 3));
        commit.commit(0, write.prepareCommit(true, 0));

        // commit bucket 1 only
        write(write, mkRow(1, 1, 2, 2));
        commit.commit(1, write.prepareCommit(true, 1));

        // compact only
        write.compact(row(1), 1, true);
        commit.commit(2, write.prepareCommit(true, 2));

        // commit bucket 2 only
        write(write, mkRow(2, 2, 3, 3));
        commit.commit(3, write.prepareCommit(true, 3));

        // commit bucket 2 only
        write(write, mkRow(2, 2, 4, 4));
        commit.commit(4, write.prepareCommit(true, 4));

        // commit bucket 2 only
        write(write, mkRow(2, 2, 5, 5));
        commit.commit(5, write.prepareCommit(true, 5));

        write.close();
        commit.close();
    }

    private List<IndexManifestEntry> readIndexManifest(Snapshot snapshot) {
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        return indexFileHandler.readManifest(snapshot.indexManifest());
    }

    private Pair<GenericRow, Integer> mkRow(int partition, int bucket, int key, int value) {
        return Pair.of(GenericRow.of(partition, key, value), bucket);
    }

    private void write(StreamTableWrite write, Pair<GenericRow, Integer> rowWithBucket)
            throws Exception {
        write.write(rowWithBucket.getKey(), rowWithBucket.getValue());
    }
}
