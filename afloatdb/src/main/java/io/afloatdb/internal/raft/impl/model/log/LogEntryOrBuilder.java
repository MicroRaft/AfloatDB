/*
 * Copyright (c) 2020, AfloatDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.afloatdb.internal.raft.impl.model.log;

import io.afloatdb.raft.proto.LogEntryProto;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.LogEntry.LogEntryBuilder;

import javax.annotation.Nonnull;

public class LogEntryOrBuilder implements LogEntry, LogEntryBuilder {

    private LogEntryProto.Builder builder;
    private LogEntryProto entry;

    public LogEntryOrBuilder() {
        this.builder = LogEntryProto.newBuilder();
    }

    public LogEntryOrBuilder(LogEntryProto entry) {
        this.entry = entry;
    }

    public LogEntryProto getEntry() {
        return entry;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setIndex(long index) {
        builder.setIndex(index);
        return this;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setOperation(@Nonnull Object operation) {
        builder.setOperation(Operations.wrap(operation));
        return this;
    }

    @Nonnull
    @Override
    public LogEntry build() {
        entry = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcLogEntryBuilder{builder=" + builder + "}";
        }

        return "GrpcLogEntry{" + "index=" + getIndex() + ", term=" + getTerm() + ", operation=" + getOperation() + '}';
    }

    @Override
    public long getIndex() {
        return entry.getIndex();
    }

    @Override
    public int getTerm() {
        return entry.getTerm();
    }

    @Nonnull
    @Override
    public Object getOperation() {
        return Operations.unwrap(entry.getOperation());
    }

}
