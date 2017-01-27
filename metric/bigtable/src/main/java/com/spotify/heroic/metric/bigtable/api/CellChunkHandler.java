/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric.bigtable.api;

import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk.RowStatusCase;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class CellChunkHandler {

    // Downstream consumer of committed cells
    private final Consumer<List<CellChunk>> commitCells;
    // Cell chunk data from incomplete cell kept from last invocation of consumeChunks
    private final List<ByteString> partialCellCarryOver;
    // Cells last invocation of consumeChunks that have not been committed
    private final List<CellChunk> nonCommittedCellsCarryOver;

    CellChunkHandler(Consumer<List<CellChunk>> commitCells) {
        this(commitCells, Collections.emptyList(), Collections.emptyList());
    }

    CellChunkHandler consumeChunks(List<CellChunk> cellChunks) {
        log.trace("Got {} cell chunks", cellChunks.size());
        List<ByteString> incompleteCellData = Collections.emptyList();
        if (!partialCellCarryOver.isEmpty() ||
            cellChunks.stream().anyMatch(cc -> cc.getValueSize() > 0)) {

            log.debug("Creating cells from {} chunks (and {} carry over chunks)", cellChunks.size(),
                partialCellCarryOver.size());

            List<CellChunk> cells = new ArrayList<>(cellChunks.size());
            List<ByteString> partialCell = new ArrayList<>(2);
            partialCell.addAll(partialCellCarryOver);

            cellChunks.forEach(cc -> {
                if (cc.getValueSize() > 0) { // partial cell
                    partialCell.add(cc.getValue());
                } else if (partialCell.isEmpty()) { // no cell in progress
                    cells.add(cc);
                } else { // complete chunked cell
                    partialCell.add(cc.getValue());
                    cells.add(cc
                        .toBuilder()
                        .setValue(ByteString.copyFrom(partialCell))
                        .setValueSize(0)
                        .build());
                    partialCell.clear();
                }
            });

            incompleteCellData = partialCell;
            cellChunks = cells;
        }
        // All cellChunks are complete cells
        if (!nonCommittedCellsCarryOver.isEmpty()) {
            List<CellChunk> cells = new ArrayList<>(nonCommittedCellsCarryOver);
            cells.addAll(cellChunks);
            cellChunks = cells;
        }
        List<CellChunk> cellsCarryOver = consumeCells(cellChunks);
        return new CellChunkHandler(commitCells, incompleteCellData, cellsCarryOver);
    }

    private List<CellChunk> consumeCells(List<CellChunk> cells) {
        int nextListStart = 0;
        for (int i = 0; i < cells.size(); i++) {
            CellChunk cellChunk = cells.get(i);
            RowStatusCase rowStatusCase = cellChunk.getRowStatusCase();

            if (rowStatusCase == RowStatusCase.RESET_ROW && cellChunk.getResetRow()) {
                nextListStart = i + 1;
            } else if (rowStatusCase == RowStatusCase.COMMIT_ROW && cellChunk.getCommitRow()) {
                int thisListStart = nextListStart;
                nextListStart = i + 1;
                if (thisListStart == 0 && nextListStart == cells.size()) {
                    commitCells.accept(cells);
                } else {
                    commitCells.accept(cells.subList(thisListStart, nextListStart));
                }
            }
        }
        return cells.subList(nextListStart, cells.size());
    }
}
