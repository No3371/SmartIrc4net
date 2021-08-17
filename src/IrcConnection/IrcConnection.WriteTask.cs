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
using System.IO;
using System.Text;

namespace Meebey.SmartIrc4net
{
    public partial class IrcConnection
    {
        /// <summary>
        /// 
        /// </summary>
        private class WriteTask
        {
            private readonly IrcConnection  connection;
            private readonly Task         task;
            private readonly System.Threading.CancellationTokenSource cancellationTokenSource;
            private readonly int[] counters;
            private readonly StreamWriter writer;

            [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
            private int ThresholdInBurstByPriority (Priority p)
            {
                switch (p)
                {
                    case Priority.AboveMedium: return 4;
                    case Priority.Medium: return 2;
                    case Priority.BelowMedium: return 1;
                    default: return 999;
                }
            }

            public WriteTask(IrcConnection connection, Stream stream)
            {
                this.connection = connection;
                cancellationTokenSource = new System.Threading.CancellationTokenSource();
                counters = new int[6];
                if (connection.EnableUTF8Recode)
                {
                    writer = new StreamWriter(stream, new UTF8Encoding(false, false));
                }
                else
                {
                    writer = new StreamWriter(stream, connection._Encoding);

                    if (connection._Encoding.GetPreamble().Length > 0) {
                        // HACK: we have an encoding that has some kind of preamble
                        // like UTF-8 has a BOM, this will confuse the IRCd!
                        // Thus we send a \r\n so the IRCd can safely ignore that
                        // garbage.
                        writer.WriteLine();
                        // make sure we flush the BOM+CRLF correctly
                        writer.Flush();
                    }
                }
                task = Task.Factory.StartNew(_Worker, cancellationTokenSource.Token, cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }

            ~WriteTask()
            {
                writer.Dispose();
                cancellationTokenSource.Dispose();
            }

            public void Stop()
            {
                connection.Log("[IRC] Stopping Write task...");
                cancellationTokenSource.Cancel();
                
            }

            private async Task _Worker(object cto)
            {
                System.Threading.CancellationToken ct = (System.Threading.CancellationToken) cto;
                connection.Log("[IRC] Write task started on " + Thread.CurrentThread.ManagedThreadId);
                if (!connection.IsConnected)
                {
                    connection.LogError("[IRC] Not connected after Write task started.");
                    throw new InvalidOperationException();
                }
                try {
                    while (connection.IsConnected) {
                        ct.ThrowIfCancellationRequested();
                        await _CheckBuffer();
                        await Task.Delay(connection.SendDelay, ct);
                    }
                    connection.Log("[IRC] Write task C");
                } catch (OperationCanceledException) {
                    connection.Log("<color=red>[IRC] Write_Worker is canceled.</color>");
                    return;
                } catch (Exception e){
                    connection.LogError("Write task exception: {0}", e);
                    connection.Log("Write task cleaning up...");
                    try {
                        writer.Close();
                    } catch (ObjectDisposedException) {
                    }
                }
            }

        public bool WriteLine(string data)
        {
            try {
                // lock (_Writer) {
                    writer.Write(data + "\r\n");
                    writer.Flush();
                // }
            } catch (IOException e) {
                connection.LogError("[IRC] _WriteLine results in a IOException: {0}, {1}", e.HResult);
                Interlocked.Increment(ref connection.connectionErrorFlag);
                return false;
            } catch (ObjectDisposedException) {
                connection.LogError("[IRC] sending data failed (stream error), connection lost");
                connection.LogError("[IRC] ObjectDisposedException happened in _WriteLine.");
                Interlocked.Increment(ref connection.connectionErrorFlag);
                return false;
            }

            connection.Log("[IRC] > "+data);

            connection.OnWriteLine?.Invoke(this, new WriteLineEventArgs(data));
            return true;
        }
    
#region WARNING: complex scheduler, don't even think about changing it!
            // WARNING: complex scheduler, don't even think about changing it!
            private async Task _CheckBuffer()
            {
                // only send data if we are succefully registered on the IRC network
                if (!connection._IsRegistered) {
                    return;
                }

                // This part basically try to make sure messages sending respect the priority, for example if only 3 priority (3:2:1)levels are defined, 6 messages in 3 priority levels will be sent in 3:2:1 ratio
                if (await _CheckBuffer(Priority.High) && await _CheckBuffer(Priority.AboveMedium) && await _CheckBuffer(Priority.Medium) && await _CheckBuffer(Priority.BelowMedium) && await _CheckBuffer(Priority.Low)) {
                    // everything is sent, resetting all counters
                    for (int i = 0; i < counters.Length; i++) counters[i] = 0;
                }
            }
            /// <summary> Only need to be awaited when failed sending.</summary>
            /// <returns> No pending message of the priority || Sent messages of the priority has reach threshold</returns>
            private async Task<bool> _CheckBuffer(Priority priority)
            {
                int pValue = (int)priority;
                switch (priority)
                {
                    case Priority.High: // Always send messages of high priority
                        break;
                    case Priority.Low: // If there's any message of higher priority is sent this round, skip
                        for (int i = (int) Priority.BelowMedium; i < (int) Priority.Critical; i++) if (counters[i] > 0) return true;
                        break;
                    default:
                        if (counters[pValue] >= ThresholdInBurstByPriority(priority)) return true;
                        else break;
                }
                if (!connection._SendBuffer[priority].Reader.TryRead(out string data)) return true; // Check again, if nothing pending in the channels, skip and signal should reset counters
                counters[pValue]++; // Whether it succeed or not, increase the counter
                bool success = WriteLine(data);
                if (!success)
                {
                    connection.Log("Sending data was not successful, data is requeued!");
                    await connection._SendBuffer[priority].Writer.WriteAsync(data);
                    return false;
                }
                switch (priority)
                {
                    case Priority.High: // If a message of High priority is successfully send, we assume there may be more, don't reset counters
                        if (success) return false;
                        else return true;
                    case Priority.Low: // If there's any message of higher priority succesfully sent, we don't even get here, so we assume the remainings are all Low priority messages, if sent successfully, don't reset counters
                        if (success) return false;
                        else return true;
                    default:
                        if (counters[pValue] >= ThresholdInBurstByPriority(priority)) return true; // If this priority has reach its threshold, we don't reset counters
                        else return false;
                }
            }

#endregion
        }
    }
}
