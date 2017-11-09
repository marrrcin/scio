/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bigtable;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import java.util.List;

public class BigtableDoFnExample extends BigtableDoFn<String, String> {
  public BigtableDoFnExample(BigtableOptions options) {
    super(options);
  }

  @Override
  public ListenableFuture<String> processElement(BigtableSession session, String input) {
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
            .setTableName("table")
            .setRows(RowSet.newBuilder()
                    .addRowKeys(ByteString.copyFromUtf8("rowkey")))
            .setFilter(RowFilter.newBuilder()
                    .setFamilyNameRegexFilter("family")
                    .setColumnQualifierRegexFilter(ByteString.copyFromUtf8("column"))
                    .build())
            .build();
    ListenableFuture<List<Row>> result = session.getDataClient().readRowsAsync(request);
    return Futures.transform(result, rows ->
            rows.get(0)
                    .getFamilies(0)
                    .getColumns(0)
                    .getCells(0)
                    .getValue()
                    .toStringUtf8());
  }
}
