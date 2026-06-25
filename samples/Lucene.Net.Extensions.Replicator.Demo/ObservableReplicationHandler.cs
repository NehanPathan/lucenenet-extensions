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
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Extensions.Replicator.Demo;

/// <summary>
/// Wraps a real <see cref="IReplicationHandler"/> (e.g. <c>IndexReplicationHandler</c>) so the demo
/// can observe exactly when a revision was applied, without polling/sleeping blindly. Raises
/// <see cref="Applied"/> after delegating to the inner handler, so callers can await a
/// <see cref="TaskCompletionSource{TResult}"/> for deterministic verification.
/// Wired in via <see cref="Lucene.Net.Extensions.Replicator.Client.Options.ReplicationClientOptions.ReplicationHandlerFactory"/>.
/// </summary>
public sealed class ObservableReplicationHandler : IReplicationHandler
{
    private readonly IReplicationHandler _inner;

    public event Action<string, int>? Applied;

    public ObservableReplicationHandler(IReplicationHandler inner)
    {
        _inner = inner;
    }

    public string? CurrentVersion => _inner.CurrentVersion;

    public IDictionary<string, IList<RevisionFile>>? CurrentRevisionFiles => _inner.CurrentRevisionFiles;

    public void RevisionReady(
        string version,
        IDictionary<string, IList<RevisionFile>> revisionFiles,
        IDictionary<string, IList<string>> copiedFiles,
        IDictionary<string, LuceneDirectory> sourceDirectory)
    {
        DemoLog.Write($"RevisionReady called -> installing version {version}");
        _inner.RevisionReady(version, revisionFiles, copiedFiles, sourceDirectory);

        int totalFiles = copiedFiles.Sum(kv => kv.Value.Count);
        DemoLog.Write($"Revision {version} installed ({totalFiles} file(s) copied).");
        Applied?.Invoke(version, totalFiles);
    }
}
