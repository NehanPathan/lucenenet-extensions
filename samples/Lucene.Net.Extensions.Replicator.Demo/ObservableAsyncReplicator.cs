/*
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

using Lucene.Net.Replicator;

namespace Lucene.Net.Extensions.Replicator.Demo;

/// <summary>
/// Wraps a real <see cref="IAsyncReplicator"/> (e.g. <c>HttpReplicator</c>) purely to make the
/// replication lifecycle observable for this demo: it logs every call and counts checks/releases
/// so the demo can assert "ReleaseAsync is always called for every session that was opened".
/// Wired in via <see cref="Lucene.Net.Extensions.Replicator.Client.Options.ReplicationClientOptions.ReplicatorFactory"/>
/// -- no library code needed to change for this to work.
/// </summary>
public sealed class ObservableAsyncReplicator : IAsyncReplicator
{
    private readonly IAsyncReplicator _inner;

    public int SessionsOpened { get; private set; }
    public int SessionsReleased { get; private set; }
    public int FilesObtained { get; private set; }

    public ObservableAsyncReplicator(IAsyncReplicator inner)
    {
        _inner = inner;
    }

    public async Task<SessionToken?> CheckForUpdateAsync(string? currentVersion, CancellationToken cancellationToken = default)
    {
        DemoLog.Write($"Client polling... (currentVersion={currentVersion ?? "<none>"})");
        SessionToken? session = await _inner.CheckForUpdateAsync(currentVersion, cancellationToken).ConfigureAwait(false);
        if (session is null)
        {
            DemoLog.Write("No new revision found.");
        }
        else
        {
            SessionsOpened++;
            DemoLog.Write($"New revision found! version={session.Version}, sessionId={session.Id}");
        }
        return session;
    }

    public async Task<Stream> ObtainFileAsync(string sessionId, string source, string fileName, CancellationToken cancellationToken = default)
    {
        DemoLog.Write($"Downloading file '{fileName}' (source={source})...");
        Stream stream = await _inner.ObtainFileAsync(sessionId, source, fileName, cancellationToken).ConfigureAwait(false);
        FilesObtained++;
        return stream;
    }

    public async Task ReleaseAsync(string sessionId, CancellationToken cancellationToken = default)
    {
        await _inner.ReleaseAsync(sessionId, cancellationToken).ConfigureAwait(false);
        SessionsReleased++;
        DemoLog.Write($"ReleaseAsync complete (sessionId={sessionId})");
    }

    public Task PublishAsync(IRevision revision, CancellationToken cancellationToken = default) =>
        _inner.PublishAsync(revision, cancellationToken);

    public SessionToken? CheckForUpdate(string? currentVersion) => _inner.CheckForUpdate(currentVersion);

    public Stream ObtainFile(string sessionId, string source, string fileName) => _inner.ObtainFile(sessionId, source, fileName);

    public void Release(string sessionId) => _inner.Release(sessionId);

    public void Publish(IRevision revision) => _inner.Publish(revision);

    public void Dispose() => _inner.Dispose();
}
