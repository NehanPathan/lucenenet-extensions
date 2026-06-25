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

using Lucene.Net.Extensions.Replicator.Client.Options;
using Lucene.Net.Index;
using Lucene.Net.Replicator;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using LuceneDirectory = Lucene.Net.Store.Directory;
using MMapDirectory = Lucene.Net.Store.MMapDirectory;
using SimpleFSDirectory = Lucene.Net.Store.SimpleFSDirectory;

namespace Lucene.Net.Extensions.Replicator.Client.Services;

/// <summary>
/// A background service that polls a replication server using <see cref="HttpReplicator"/>'s
/// async API and applies updates via a <see cref="ReplicationWorker"/>. Owns process-lifetime
/// resources (the HTTP client, the replica <see cref="LuceneDirectory"/>, the <see cref="DirectoryReader"/>)
/// and the polling schedule; the per-cycle replication protocol itself lives in <see cref="ReplicationWorker"/>.
/// </summary>
public class AsyncReplicationClientService : BackgroundService
{
    private readonly ILogger<AsyncReplicationClientService> _logger;
    private readonly ReplicationClientOptions _options;
    private readonly HttpClient _httpClient;
    private LuceneDirectory? _replicaDirectory;
    private DirectoryReader? _reader;

    /// <summary>
    /// Initializes a new instance of the <see cref="AsyncReplicationClientService"/> class.
    /// </summary>
    /// <param name="logger">The logger used for diagnostic messages.</param>
    /// <param name="options">The replication client options.</param>
    /// <param name="httpClientFactory">Factory for creating <see cref="HttpClient"/> instances.</param>
    /// <exception cref="InvalidOperationException">Thrown when required options such as
    /// <see cref="ReplicationClientOptions.ServerUrl"/> are not provided.</exception>
    public AsyncReplicationClientService(
        ILogger<AsyncReplicationClientService> logger,
        IOptions<ReplicationClientOptions> options,
        IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _options = options.Value;
        _httpClient = httpClientFactory.CreateClient();

        if (string.IsNullOrWhiteSpace(_options.ServerUrl))
            throw new InvalidOperationException("ServerUrl must be provided for the replication client.");

        if (_options.PullInterval <= TimeSpan.Zero)
        {
            _logger.LogWarning("PullInterval <= 0, defaulting to 10s");
            _options.PullInterval = TimeSpan.FromSeconds(10);
        }

        if (!string.IsNullOrWhiteSpace(_options.TempPath))
        {
            System.IO.Directory.CreateDirectory(_options.TempPath);
        }
    }

    /// <summary>
    /// Polls the replication server at <see cref="ReplicationClientOptions.PullInterval"/>,
    /// delegating each cycle to a <see cref="ReplicationWorker"/>, and refreshes the
    /// <see cref="DirectoryReader"/> whenever a new revision was applied.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ReplicationWorker worker = CreateWorker();

        _logger.LogInformation("Starting async replication client from {ServerUrl} to {IndexPath}",
            _options.ServerUrl, _options.IndexPath);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                bool updated = await worker.RunCycleAsync(stoppingToken).ConfigureAwait(false);
                if (updated)
                {
                    RefreshReader();
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(ex, "Replication failed due to network issue. Retrying in {PullInterval}s",
                    _options.PullInterval.TotalSeconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Replication cycle failed");
            }

            try
            {
                await Task.Delay(_options.PullInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Builds the per-run replication collaborators (replica directory, per-session
    /// directory factory, handler, replicator, worker) from <see cref="ReplicationClientOptions"/>.
    /// Kept separate from the polling loop in <see cref="ExecuteAsync"/> so the two concerns
    /// (wiring collaborators vs. scheduling/cancellation) aren't interleaved.
    /// </summary>
    private ReplicationWorker CreateWorker()
    {
        _replicaDirectory = _options.DirectoryFactory?.Invoke(_options.IndexPath)
                            ?? FSDirectory.Open(_options.IndexPath);

        var directoryFactory = new PerSessionDirectoryFactory(
            IsDiskBasedDirectory(_replicaDirectory)
                ? _options.TempPath is { Length: > 0 } ? _options.TempPath : Path.GetTempPath()
                : Path.GetTempPath());

        IReplicationHandler handler = _options.ReplicationHandlerFactory(_replicaDirectory);
        IAsyncReplicator replicator = _options.ReplicatorFactory(_httpClient, _options.ServerUrl);

        return new ReplicationWorker(replicator, handler, directoryFactory, _logger);
    }

    private void RefreshReader()
    {
        if (_reader is null)
        {
            _reader = DirectoryReader.Open(_replicaDirectory);
        }
        else
        {
            DirectoryReader? newReader = DirectoryReader.OpenIfChanged(_reader);
            if (newReader is not null)
            {
                _reader.Dispose();
                _reader = newReader;
            }
        }

        _logger.LogInformation("Index at {IndexPath} now has {DocCount} docs", _options.IndexPath, _reader.NumDocs);
    }

    /// <summary>
    /// Releases resources used by the service, including the current
    /// <see cref="DirectoryReader"/> and the replica <see cref="LuceneDirectory"/>.
    /// </summary>
    public override void Dispose()
    {
        base.Dispose();
        _reader?.Dispose();
        _replicaDirectory?.Dispose();
    }

    private static bool IsDiskBasedDirectory(LuceneDirectory directory) =>
        directory is FSDirectory or MMapDirectory or SimpleFSDirectory;
}
