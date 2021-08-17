/*
 * $Id$
 * $URL$
 * $Rev$
 * $Author$
 * $Date$
 *
 * SmartIrc4net - the IRC library for .NET/C# <http://smartirc4net.sf.net>
 *
 * Copyright (c) 2003-2009 Mirco Bauer <meebey@meebey.net> <http://www.meebey.net>
 * Copyright (c) 2008-2009 Thomas Bruderer <apophis@apophis.ch>
 * 
 * Full LGPL License: <http://www.gnu.org/licenses/lgpl.txt>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
// #define DEBUGGING
using System;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;
using System.IO;
using System.Text;

namespace Meebey.SmartIrc4net
{
    public partial class IrcConnection
    {
        private class ReadTask
        {
            private readonly IrcConnection connection;
            private readonly Task task;
            private readonly System.Threading.CancellationTokenSource cancellationTokenSource;
            private readonly StreamReader reader;
            public BlockingCollection<string> Read { get; private set; }

            public ReadTask(IrcConnection connection, Stream stream)
            {
                this.connection = connection;
                Read = new BlockingCollection<string>();
                cancellationTokenSource = new System.Threading.CancellationTokenSource();
                if (connection.EnableUTF8Recode) {
                    reader = new StreamReader(stream, new PrimaryOrFallbackEncoding(new UTF8Encoding(false, true), connection._Encoding));
                } else {
                    reader = new StreamReader(stream, connection._Encoding);
                }
                task = Task.Factory.StartNew(Worker, cancellationTokenSource.Token, cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                task.ContinueWith((workerTask) => { 
                    workerTask.Wait();
                    connection.Log("Read task finished: {0} / IsCompleted: {1} / Is Canceled: {2} / IsFaulted: {3}", workerTask.Status, workerTask.IsCompleted, workerTask.IsCanceled, workerTask.IsFaulted);
                    if (workerTask.Exception != null)
                    {
                        connection.LogError("Read task exception: {0}", workerTask.Exception);
                        Interlocked.Increment(ref connection.connectionErrorFlag);
                    }
                    connection.Log("Read task cleaning up...");
                    try {
                        reader.Close();
                    } catch (ObjectDisposedException) {
                    }
                    Read.CompleteAdding();
                });
            }

            ~ReadTask()
            {
                reader.Dispose();
                Read.Dispose();
                cancellationTokenSource.Dispose();
            }

            public void Stop()
            {
                connection.Log("Read task stopping...");
                cancellationTokenSource.Cancel();
            }


            private void Worker(object cto)
            {
                System.Threading.CancellationToken ct = (System.Threading.CancellationToken) cto;
                connection.Log("Read task started on " + Thread.CurrentThread.ManagedThreadId);
                if (!connection.IsConnected) throw new InvalidOperationException();
                if (reader.EndOfStream) throw new InvalidOperationException();
                try {
                    string data;
                    while ((data = reader.ReadLine()) != null)
                    {
                        Read.Add(data);
                        ct.ThrowIfCancellationRequested();
                    }
                    connection.Log("[IRC] Read_Worker is out of loop.");
                } catch (OperationCanceledException) {
                    connection.Log("<color=red>[IRC] Read_Worker is canceled.</color>");
                }
            }
        }
    }
}
