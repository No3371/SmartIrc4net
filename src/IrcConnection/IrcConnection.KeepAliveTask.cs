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

namespace Meebey.SmartIrc4net
{
    public partial class IrcConnection
    {
        /// <summary>
        /// 
        /// </summary>
        private class KeepAliveTask
        {
            private IrcConnection   connection;
            private Task          task;
            private System.Threading.CancellationTokenSource cancellationTokenSource;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="connection"></param>
            public KeepAliveTask(IrcConnection connection)
            {
                this.connection = connection;
                cancellationTokenSource = new System.Threading.CancellationTokenSource();
                connection.PingStopwatch.Reset();
                connection.NextPingStopwatch.Reset();
                connection.NextPingStopwatch.Start();
                task = Task.Factory.StartNew(_Worker, cancellationTokenSource.Token, cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }


            /// <summary>
            /// 
            /// </summary>
            public void Stop()
            {
                cancellationTokenSource.Cancel();
            }

            private async Task _Worker(object cto)
            {
                System.Threading.CancellationToken ct = (System.Threading.CancellationToken) cto;
                connection.Log("[IRC] IdleWorker started on " + Thread.CurrentThread.ManagedThreadId);
                if (!connection.IsConnected)
                {
                    connection.LogError("[IRC] Not connected after Idle Worker started.");
                    throw new InvalidOperationException();
                }
                try {
                    while (connection.IsConnected )
                    {
                        await Task.Delay(connection._IdleWorkerInterval * 1000, ct);
                        ct.ThrowIfCancellationRequested();

                        
                        // only send active pings if we are registered
                        if (!connection.IsRegistered) {
                            continue;
                        }

                        int last_ping_sent = (int)connection.PingStopwatch.Elapsed.TotalSeconds;
                        int last_pong_rcvd = (int)connection.NextPingStopwatch.Elapsed.TotalSeconds;
                        // determins if the resoponse time is ok
                        if (last_ping_sent < connection._PingTimeout) {
                            if (connection.PingStopwatch.IsRunning) {
                                // there is a pending ping request, we have to wait
                                continue;
                            }
                            
                            // determines if it need to send another ping yet
                            if (last_pong_rcvd > connection._PingInterval) {
                                connection.NextPingStopwatch.Stop();
                                connection.PingStopwatch.Reset();
                                connection.PingStopwatch.Start();
                                connection.WriteLine(Rfc2812.Ping(connection.Address), Priority.Critical);
                            } // else connection is fine, just continue
                        } else {
                            // only flag this as connection error if we are not
                            // cleanly disconnecting
                            Interlocked.Increment(ref connection.connectionErrorFlag);
                            connection.LogError("[IRC] Idle_Worker flagging connection error: timeout.");
                            break;
                        }
                    }
                } catch (OperationCanceledException) {
                    connection.Log("<color=red>[IRC] IdleWorkerTask is canceled.</color>");
                } catch (Exception e) {
                    connection.LogError("Idle task exception: {0}", e);
                }
            }
        }
    }
}
