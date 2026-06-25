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

using System.Buffers;
using Lucene.Net.Replicator;
using Microsoft.Extensions.Logging;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Extensions.Replicator.Client.Services;

/// <summary>
/// Performs a single asynchronous replication cycle: checks the remote
/// <see cref="IAsyncReplicator"/> for a new revision, copies any required
/// files into a per-session <see cref="LuceneDirectory"/>, and notifies the
/// configured <see cref="IReplicationHandler"/>. Holds no state between calls
/// and creates no threads or timers of its own, so it can be invoked directly
/// from a hosted polling loop, a manual trigger, or a unit test.
/// </summary>
public class ReplicationWorker
{
    private readonly IAsyncReplicator _replicator;
    private readonly IReplicationHandler _handler;
    private readonly ISourceDirectoryFactory _directoryFactory;
    private readonly ILogger _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplicationWorker"/> class.
    /// </summary>
    /// <param name="replicator">The async replicator used to check for and fetch updates.</param>
    /// <param name="handler">The handler notified once a revision's files have been copied.</param>
    /// <param name="directoryFactory">Resolves the per-session <see cref="LuceneDirectory"/> files are copied into.</param>
    /// <param name="logger">Logger for diagnostic messages about the cycle.</param>
    public ReplicationWorker(
        IAsyncReplicator replicator,
        IReplicationHandler handler,
        ISourceDirectoryFactory directoryFactory,
        ILogger logger)
    {
        _replicator = replicator ?? throw new ArgumentNullException(nameof(replicator));
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _directoryFactory = directoryFactory ?? throw new ArgumentNullException(nameof(directoryFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Runs one replication cycle to completion. Returns <c>false</c> if there was
    /// no new revision to apply, or <c>true</c> if a revision was fetched and the
    /// handler was notified.
    /// </summary>
    public virtual async Task<bool> RunCycleAsync(CancellationToken cancellationToken = default)
    {
        SessionToken? session = await _replicator.CheckForUpdateAsync(_handler.CurrentVersion, cancellationToken)
            .ConfigureAwait(false);

        if (session is null)
        {
            _logger.LogDebug("No new revision available (handlerVersion={HandlerVersion})", _handler.CurrentVersion);
            return false;
        }

        var sourceDirectories = new Dictionary<string, LuceneDirectory>();
        var copiedFiles = new Dictionary<string, IList<string>>();
        bool succeeded = false;

        try
        {
            IDictionary<string, IList<RevisionFile>> requiredFiles = ResolveRequiredFiles(session.SourceFiles);

            foreach (var (source, files) in requiredFiles)
            {
                LuceneDirectory directory = _directoryFactory.GetDirectory(session.Id, source);
                sourceDirectories[source] = directory;

                var copied = new List<string>();
                copiedFiles[source] = copied;

                foreach (RevisionFile file in files)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await CopyFileAsync(session.Id, source, file.FileName, directory, cancellationToken)
                        .ConfigureAwait(false);
                    copied.Add(file.FileName);
                }
            }

            succeeded = true;
        }
        finally
        {
            try
            {
                await _replicator.ReleaseAsync(session.Id, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (!succeeded)
                {
                    DisposeAndCleanup(session.Id, sourceDirectories);
                }
            }
        }

        try
        {
            // Mirrors ReplicationClient.DoUpdate()'s final "if (notify && !disposed)" guard: a
            // cancellation requested after all files were copied but before the handler was
            // notified still aborts the notification (the finally below still cleans up).
            cancellationToken.ThrowIfCancellationRequested();

            // RevisionReady is synchronous: it is not part of IAsyncReplicator/IReplicationHandler
            // and installs already-downloaded segment files, which is local disk work, not network I/O.
            _handler.RevisionReady(session.Version, session.SourceFiles, copiedFiles, sourceDirectories);
            _logger.LogInformation("Replicated revision {Version} ({FileCount} files)",
                session.Version, copiedFiles.Sum(kv => kv.Value.Count));
        }
        finally
        {
            DisposeAndCleanup(session.Id, sourceDirectories);
        }

        return true;
    }

    private async Task CopyFileAsync(string sessionId, string source, string fileName, LuceneDirectory directory, CancellationToken cancellationToken)
    {
        using Stream input = await _replicator.ObtainFileAsync(sessionId, source, fileName, cancellationToken)
            .ConfigureAwait(false);
        using var output = directory.CreateOutput(fileName, Lucene.Net.Store.IOContext.DEFAULT);

        byte[] buffer = ArrayPool<byte>.Shared.Rent(16384);
        try
        {
            int numBytes;
            while ((numBytes = await input.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false)) > 0)
            {
                output.WriteBytes(buffer, 0, numBytes);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Returns only the files in <paramref name="newRevisionFiles"/> that the handler
    /// does not already have, mirroring <c>ReplicationClient.RequiredFiles</c>.
    /// </summary>
    private IDictionary<string, IList<RevisionFile>> ResolveRequiredFiles(IDictionary<string, IList<RevisionFile>> newRevisionFiles)
    {
        IDictionary<string, IList<RevisionFile>>? handlerFiles = _handler.CurrentRevisionFiles;
        if (handlerFiles is null)
            return newRevisionFiles;

        var required = new Dictionary<string, IList<RevisionFile>>();
        foreach (var (source, existing) in handlerFiles)
        {
            var existingNames = new HashSet<string>(existing.Select(f => f.FileName));
            required[source] = newRevisionFiles[source].Where(f => !existingNames.Contains(f.FileName)).ToList();
        }

        return required;
    }

    private void DisposeAndCleanup(string? sessionId, Dictionary<string, LuceneDirectory> sourceDirectories)
    {
        foreach (LuceneDirectory directory in sourceDirectories.Values)
        {
            directory.Dispose();
        }

        _directoryFactory.CleanupSession(sessionId);
    }
}
