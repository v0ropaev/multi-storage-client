# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import queue

from multistorageclient.sync.transfer_progress import TransferProgress, TransferSnapshot


def test_transfer_snapshots_are_bounded_and_remove_finished_files():
    progress = TransferProgress(lambda snapshot: None)
    paths = ["first.tar", *(f"file-{index}.tar" for index in range(127))]
    transfer_callback = progress.callback(paths)
    for index in range(len(paths)):
        transfer_callback(index, "started")
    snapshot = progress.snapshot()
    assert snapshot.count == 128
    assert [item.path for item in snapshot.oldest] == paths[:5]
    transfer_callback(0, "completed")
    assert progress.snapshot().count == 127
    assert [item.path for item in progress.snapshot().oldest] == paths[1:6]
    for index in range(1, len(paths)):
        transfer_callback(index, "completed")
    assert progress.snapshot() == TransferSnapshot(0, ())


def test_transfer_failure_and_retry_have_separate_lifetimes():
    progress = TransferProgress(lambda snapshot: None)
    transfer_callback = progress.callback(["file.tar"])
    transfer_callback(0, "started")
    first = progress.snapshot().oldest[0]
    transfer_callback(0, "failed")
    assert progress.snapshot().count == 0
    transfer_callback(0, "started")
    retry = progress.snapshot()
    assert retry.count == 1
    assert retry.oldest[0].started_at >= first.started_at
    transfer_callback(0, "completed")
    assert progress.snapshot().count == 0


def test_transfer_reporting_refreshes_without_completions_and_stops():
    snapshots = queue.Queue()
    progress = TransferProgress(snapshots.put)
    progress.start()
    transfer_callback = progress.callback(["large.tar"])
    try:
        transfer_callback(0, "started")
        snapshot = snapshots.get(timeout=3.0)
        assert snapshot.count == 1
        assert snapshot.oldest[0].path == "large.tar"
        assert snapshots.get(timeout=3.0).count == 1
    finally:
        progress.close()
    assert snapshots.get(timeout=1.0) == TransferSnapshot(0, ())
    assert not progress._thread.is_alive()
    transfer_callback(0, "started")
    assert progress.snapshot().count == 0


def test_transfer_callbacks_keep_batches_and_reporters_independent():
    progress = TransferProgress(lambda snapshot: None)
    first = progress.callback(["logical/first.tar", "logical/second.tar"])
    second = progress.callback(["another/file.tar"])
    first(0, "started")
    first(1, "started")
    second(0, "started")
    assert progress.snapshot().count == 3
    first(0, "completed")
    assert {item.path for item in progress.snapshot().oldest} == {"logical/second.tar", "another/file.tar"}
    second(0, "failed")
    assert progress.snapshot().count == 1
    first(1, "failed")
    assert progress.snapshot().count == 0
    first(1, "started")
    assert progress.snapshot().count == 1
    first(1, "completed")
    other = TransferProgress(lambda snapshot: None)
    assert progress.snapshot().count == other.snapshot().count == 0
    progress.close()
    first(0, "started")
    assert progress.snapshot().count == 0
