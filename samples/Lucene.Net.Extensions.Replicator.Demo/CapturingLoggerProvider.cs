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

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Lucene.Net.Extensions.Replicator.Demo;

/// <summary>
/// Records every Error/Critical log entry emitted by the host's services (e.g.
/// AsyncReplicationClientService, ReplicationServerService) so the demo can assert
/// "no exceptions thrown" by checking <see cref="Errors"/> at the end, instead of just
/// hoping nothing scrolled past in the console.
/// </summary>
public sealed class CapturingLoggerProvider : ILoggerProvider
{
    public ConcurrentQueue<string> Errors { get; } = new();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, Errors);

    public void Dispose()
    {
    }

    private sealed class CapturingLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly ConcurrentQueue<string> _errors;

        public CapturingLogger(string categoryName, ConcurrentQueue<string> errors)
        {
            _categoryName = categoryName;
            _errors = errors;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            _errors.Enqueue($"[{_categoryName}] {formatter(state, exception)} {exception}");
        }
    }
}
