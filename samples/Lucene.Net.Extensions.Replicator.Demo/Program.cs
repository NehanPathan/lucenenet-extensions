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

using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Extensions.Replicator.Client;
using Lucene.Net.Extensions.Replicator.Demo;
using Lucene.Net.Extensions.SelfHost.Replicator;
using Lucene.Net.Index;
using Lucene.Net.Replicator;
using Lucene.Net.Replicator.Http;
using Lucene.Net.Util;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using IODirectory = System.IO.Directory;

const int ServerPort = 17171;
const string ShardName = "index";
var failures = new List<string>();

string demoRoot = Path.Combine(Path.GetTempPath(), "LuceneNet.Replicator.Demo", Guid.NewGuid().ToString("N"));
string publisherPath = Path.Combine(demoRoot, "publisher");
string replicaPath = Path.Combine(demoRoot, "replica");
string replicaTempPath = Path.Combine(demoRoot, "replica-temp");
IODirectory.CreateDirectory(publisherPath);
IODirectory.CreateDirectory(replicaPath);
IODirectory.CreateDirectory(replicaTempPath);

DemoLog.Section("Step 1-3: Creating publisher index and publishing Revision 1");

using FSDirectory publisherDirectory = FSDirectory.Open(publisherPath);
var snapshotPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
var writerConfig = new IndexWriterConfig(LuceneVersion.LUCENE_48, new StandardAnalyzer(LuceneVersion.LUCENE_48))
{
    IndexDeletionPolicy = snapshotPolicy,
};
using var publisherWriter = new IndexWriter(publisherDirectory, writerConfig);

AddDocument(publisherWriter, "1", "first document");
publisherWriter.Commit();

var localReplicator = new LocalReplicator();
DemoLog.Write("Publishing Revision 1...");
localReplicator.Publish(new IndexRevision(publisherWriter));

var appliedFirst = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
var appliedSecond = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
int appliedCount = 0;
ObservableAsyncReplicator? observableReplicator = null;
ObservableReplicationHandler? observableHandler = null;
var capturingLogger = new CapturingLoggerProvider();

var builder = Host.CreateApplicationBuilder();
builder.Logging.ClearProviders();
builder.Logging.AddProvider(capturingLogger);
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Logging.SetMinimumLevel(LogLevel.Information);
// NOTE: ReplicationServerService hosts its own embedded Kestrel WebApplication with an
// independent ASP.NET Core logging pipeline, so framework request logs (Microsoft.AspNetCore.*)
// can't be filtered from here. The demo's own narrative is every line prefixed with "[HH:mm:ss.fff]".

builder.Services.AddHttpClient();

builder.Services.AddLuceneReplicationServer(o =>
{
    o.Port = ServerPort;
    o.Replicators[ShardName] = localReplicator;
});

builder.Services.AddLuceneAsyncReplicationClient(o =>
{
    o.ServerUrl = $"http://localhost:{ServerPort}/replicate/{ShardName}";
    o.IndexPath = replicaPath;
    o.TempPath = replicaTempPath;
    o.PullInterval = TimeSpan.FromSeconds(1);

    o.ReplicationHandlerFactory = dir =>
    {
        observableHandler = new ObservableReplicationHandler(new IndexReplicationHandler(dir, null));
        observableHandler.Applied += (_, _) =>
        {
            int n = Interlocked.Increment(ref appliedCount);
            if (n == 1) appliedFirst.TrySetResult(n);
            else if (n == 2) appliedSecond.TrySetResult(n);
        };
        return observableHandler;
    };

    o.ReplicatorFactory = (httpClient, serverUrl) =>
    {
        observableReplicator = new ObservableAsyncReplicator(new HttpReplicator(serverUrl, httpClient));
        return observableReplicator;
    };
});

using IHost host = builder.Build();

try
{
    DemoLog.Section("Step 4: Starting replication server + AsyncReplicationClientService");
    await host.StartAsync();

    DemoLog.Section("Step 5: Waiting for the client to detect and apply Revision 1");
    await WaitOrFail(appliedFirst.Task, "Revision 1 was never applied to the replica within the timeout.");
    VerifyReplicaDocCount(1);
    VerifyNoLeakedTempDirectories("after Revision 1");

    DemoLog.Section("Step 6-7: Adding a second document and publishing Revision 2");
    AddDocument(publisherWriter, "2", "second document");
    publisherWriter.Commit();
    DemoLog.Write("Publishing Revision 2...");
    localReplicator.Publish(new IndexRevision(publisherWriter));

    DemoLog.Section("Step 8: Waiting for the client to detect and apply Revision 2 (no restart)");
    await WaitOrFail(appliedSecond.Task, "Revision 2 was never applied to the replica within the timeout.");
    VerifyReplicaDocCount(2);
    VerifyNoLeakedTempDirectories("after Revision 2");

    DemoLog.Section("Verification: replication protocol invariants");
    VerifyReleaseAlwaysCalled();
    VerifyNoErrorsLogged();
}
catch (Exception ex)
{
    failures.Add($"Unhandled exception: {ex}");
}
finally
{
    DemoLog.Section("Graceful shutdown");
    try
    {
        await host.StopAsync(TimeSpan.FromSeconds(10));
        DemoLog.Write("Host stopped gracefully.");
    }
    catch (Exception ex)
    {
        failures.Add($"StopAsync did not complete gracefully: {ex}");
    }

    localReplicator.Dispose();
}

DemoLog.Section("RESULT");
if (failures.Count == 0)
{
    Console.WriteLine("PASS - all verifications succeeded.");
    Console.WriteLine($"  Sessions opened/released: {observableReplicator?.SessionsOpened}/{observableReplicator?.SessionsReleased}");
    Console.WriteLine($"  Files downloaded: {observableReplicator?.FilesObtained}");
    Console.WriteLine($"  Final handler CurrentVersion: {observableHandler?.CurrentVersion}");
}
else
{
    Console.WriteLine($"FAIL - {failures.Count} verification(s) failed:");
    foreach (string failure in failures)
    {
        Console.WriteLine($"  - {failure}");
    }
}

try
{
    IODirectory.Delete(demoRoot, recursive: true);
}
catch
{
    // best-effort cleanup of the demo's own scratch directory; not a verification concern.
}

return failures.Count == 0 ? 0 : 1;

static void AddDocument(IndexWriter writer, string id, string title)
{
    var doc = new Document
    {
        new StringField("id", id, Field.Store.YES),
        new TextField("title", title, Field.Store.YES),
    };
    writer.AddDocument(doc);
}

async Task WaitOrFail(Task<int> task, string timeoutMessage)
{
    Task completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30)));
    if (completed != task)
    {
        failures.Add(timeoutMessage);
        throw new TimeoutException(timeoutMessage);
    }
}

void VerifyReplicaDocCount(int expected)
{
    using FSDirectory dir = FSDirectory.Open(replicaPath);
    using DirectoryReader reader = DirectoryReader.Open(dir);
    int actual = reader.NumDocs;
    DemoLog.Write($"Replica now contains {actual} document(s).");
    if (actual != expected)
    {
        failures.Add($"Expected replica to contain {expected} document(s), but found {actual}.");
    }
}

void VerifyNoLeakedTempDirectories(string when)
{
    string[] leftovers = IODirectory.GetDirectories(replicaTempPath);
    if (leftovers.Length > 0)
    {
        failures.Add($"Found {leftovers.Length} leaked per-session temp directory(ies) {when}: {string.Join(", ", leftovers)}");
    }
    else
    {
        DemoLog.Write($"No leaked temporary session directories {when}.");
    }
}

void VerifyReleaseAlwaysCalled()
{
    if (observableReplicator is null)
    {
        failures.Add("ReplicatorFactory was never invoked; cannot verify ReleaseAsync semantics.");
        return;
    }

    DemoLog.Write($"Sessions opened: {observableReplicator.SessionsOpened}, released: {observableReplicator.SessionsReleased}.");
    if (observableReplicator.SessionsOpened < 2)
    {
        failures.Add($"Expected at least 2 sessions to have been opened (one per revision), but observed {observableReplicator.SessionsOpened}.");
    }

    if (observableReplicator.SessionsOpened != observableReplicator.SessionsReleased)
    {
        failures.Add($"ReleaseAsync was not called for every opened session: opened={observableReplicator.SessionsOpened}, released={observableReplicator.SessionsReleased}.");
    }
}

void VerifyNoErrorsLogged()
{
    if (!capturingLogger.Errors.IsEmpty)
    {
        failures.Add($"{capturingLogger.Errors.Count} error(s) were logged during replication: {string.Join(" | ", capturingLogger.Errors)}");
    }
    else
    {
        DemoLog.Write("No errors were logged by the replication server or client.");
    }
}
