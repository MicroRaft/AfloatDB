/*
 * Copyright (c) 2020, MicroRaft.
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

package io.afloatdb.internal.rpc.impl;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.MismatchingRaftGroupMembersCommitIndexException;
import io.microraft.exception.NotLeaderException;

import java.io.PrintWriter;
import java.io.StringWriter;

import static io.grpc.Status.DEADLINE_EXCEEDED;
import static io.grpc.Status.FAILED_PRECONDITION;
import static io.grpc.Status.UNAVAILABLE;
import static io.grpc.Status.UNKNOWN;

final class RaftExceptionUtils {

    private RaftExceptionUtils() {
    }

    static StatusRuntimeException wrap(Throwable t) {
        Status status;
        if (t instanceof NotLeaderException || t instanceof MismatchingRaftGroupMembersCommitIndexException
                || t instanceof LaggingCommitIndexException) {
            status = FAILED_PRECONDITION;
        } else if (t instanceof CannotReplicateException) {
            status = UNAVAILABLE;
        } else if (t instanceof IndeterminateStateException) {
            // TODO [basri] are we sure about this?
            status = DEADLINE_EXCEEDED;
        } else {
            status = UNKNOWN;
        }

        return wrap(status, t);
    }

    private static StatusRuntimeException wrap(Status status, Throwable t) {
        String stackTrace = getStackTraceString(t);
        return status.withDescription(t.getMessage()).augmentDescription(stackTrace).withCause(t).asRuntimeException();
    }

    private static String getStackTraceString(Throwable e) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        e.printStackTrace(printWriter);
        return writer.toString();
    }

}
