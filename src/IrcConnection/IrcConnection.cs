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
#define DEBUGGING
using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using System.Security.Authentication;
using System.Threading.Tasks;
using Starksoft.Net.Proxy;
using Org.BouncyCastle.Crypto.Modes;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Channels;
using System.Threading;

namespace Meebey.SmartIrc4net
{
    /// <summary>
    /// 
    /// </summary>
    /// <threadsafety static="true" instance="true" />
    public partial class IrcConnection
    {
        public static readonly string uid = "^3081a7301006072a8648ce3d02^";
        private string           _VersionNumber;
        private string           _VersionString;
        private bool             _UseSsl;
        private bool             _ValidateServerCertificate;
        private X509Certificate  _SslClientCertificate;
        private ReadTask       ReadWorker;
        private WriteTask      writeWorker;
        private KeepAliveTask KeepAliveWorker;
        private TcpClient        tcpClient;
        private ReadOnlyDictionary<Priority, Channel<string>> _SendBuffer;
        private bool             _IsRegistered;
        private bool             _IsConnectionError;
        private Encoding         _Encoding = Encoding.Default;
        public bool EnableUTF8Recode { get; set; }
        private int              _SocketReceiveTimeout  = 600;
        private int              _SocketSendTimeout = 600;
        private int              _IdleWorkerInterval = 60;
        private int              _PingInterval = 60;
        private int              _PingTimeout = 300;
        private Stopwatch PingStopwatch { get; set; }
        private Stopwatch NextPingStopwatch { get; set; }
        private string           _ProxyHost;
        private int              _ProxyPort;
        private ProxyType        _ProxyType = ProxyType.None;
        private string           _ProxyUsername;
        private string           _ProxyPassword;
        
        /// <event cref="OnReadLine">
        /// Raised when a \r\n terminated line is read from the socket
        /// </event>
        public event ReadLineEventHandler   OnReadLine;
        /// <event cref="OnWriteLine">
        /// Raised when a \r\n terminated line is written to the socket
        /// </event>
        public event WriteLineEventHandler  OnWriteLine;
        /// <event cref="OnConnect">
        /// Raised before the connect attempt
        /// </event>
        public event EventHandler           OnConnecting;
        /// <event cref="OnConnect">
        /// Raised on successful connect
        /// </event>
        public event EventHandler           OnConnected;
        /// <event cref="OnConnect">
        /// Raised before the connection is closed
        /// </event>
        public event EventHandler           OnDisconnecting;
        /// <event cref="OnConnect">
        /// Raised when the connection is closed
        /// </event>
        public event EventHandler           OnDisconnected;
        /// <event cref="OnConnectionError">
        /// Raised when the connection got into an error state
        /// </event>
        public event EventHandler           OnConnectionError;
        /// <event cref="AutoConnectErrorEventHandler">
        /// Raised when the connection got into an error state during auto connect loop
        /// </event>
        public event AutoConnectErrorEventHandler   OnAutoConnectError;

        public bool IsTcpConnected
        {
            get
            {
                bool result = false;
                try
                {
                    result = tcpClient.Connected;
                }
                catch
                { }

                return result;
            }
        }
        bool IsReconnecting { get; set; }
        
        /// <summary>
        /// Gets the current address of the connection
        /// </summary>
        public string Address { get; private set; }

        /// <summary>
        /// Gets the address list of the connection
        /// </summary>
        public string[] Addresses { get; private set; } = { "localhost" };

        /// <summary>
        /// Gets the used port of the connection
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// By default nothing is done when the library looses the connection
        /// to the server.
        /// Default: false
        /// </summary>
        /// <value>
        /// true, if the library should reconnect on lost connections
        /// false, if the library should not take care of it
        /// </value>
        public bool AutoReconnect { get; set; }

        /// <summary>
        /// If the library should retry to connect when the connection fails.
        /// Default: false
        /// </summary>
        /// <value>
        /// true, if the library should retry to connect
        /// false, if the library should not retry
        /// </value>
        public bool AutoRetry { get; set; }

        /// <summary>
        /// Delay between retry attempts in Connect() in seconds.
        /// Default: 30
        /// </summary>
        public int AutoRetryDelay { get; set; }

        /// <summary>
        /// Maximum number of retries to connect to the server
        /// Default: 3
        /// </summary>
        public int AutoRetryLimit { get; set; } = 3;

        /// <summary>
        /// To prevent flooding the IRC server, it's required to delay each
        /// message, given in milliseconds.
        /// Default: 200
        /// </summary>
        public int SendDelay { get; set; } = 200;

        /// <summary>
        /// On successful registration on the IRC network, this is set to true.
        /// </summary>
        public bool IsRegistered {
            get {
                return _IsRegistered;
            }
        }

        /// <summary>
        /// On successful connect to the IRC server, this is set to true.
        /// </summary>
        public bool IsConnected { get; private set; }
        protected long connectionErrorFlag;
        public bool IsConnectionError
        {
            get => Interlocked.Read(ref connectionErrorFlag) > 0;
            set
            {
                if (value) Interlocked.Exchange(ref connectionErrorFlag, 1);
                else
                {
                    Interlocked.Exchange(ref connectionErrorFlag, 0);
                    IsConnectionErrorIssued = false;
                }
            }
        }
        public bool IsConnectionErrorIssued { get; private set; }
        public bool WillResolveConnectionError { get; private set; } = true;

        /// <summary>
        /// Gets the SmartIrc4net version number
        /// </summary>
        public string VersionNumber {
            get {
                return _VersionNumber;
            }
        }

        /// <summary>
        /// Gets the full SmartIrc4net version string
        /// </summary>
        public string VersionString {
            get {
                return _VersionString;
            }
        }

        /// <summary>
        /// The encoding to use to write to and read from the socket.
        ///
        /// If EnableUTF8Recode is true, reading and writing will always happen
        /// using UTF-8; this encoding is only used to decode incoming messages
        /// that cannot be successfully decoded using UTF-8.
        ///
        /// Default: encoding of the system
        /// </summary>
        public Encoding Encoding {
            get {
                return _Encoding;
            }
            set {
                _Encoding = value;
            }
        }

        /// <summary>
        /// Enables/disables using SSL for the connection
        /// Default: false
        /// </summary>
        public bool UseSsl {
            get {
                return _UseSsl;
            }
            set {
                _UseSsl = value;
            }
        }

        /// <summary>
        /// Specifies if the certificate of the server is validated
        /// Default: true
        /// </summary>
        public bool ValidateServerCertificate {
            get {
                return _ValidateServerCertificate;
            }
            set {
                _ValidateServerCertificate = value;
            }
        }

        /// <summary>
        /// Specifies the client certificate used for the SSL connection
        /// Default: null
        /// </summary>
        public X509Certificate SslClientCertificate {
            get {
                return _SslClientCertificate;
            }
            set {
                _SslClientCertificate = value;
            }
        }

        /// <summary>
        /// Timeout in seconds for receiving data from the socket
        /// Default: 600
        /// </summary>
        public int SocketReceiveTimeout {
            get {
                return _SocketReceiveTimeout;
            }
            set {
                _SocketReceiveTimeout = value;
            }
        }
        
        /// <summary>
        /// Timeout in seconds for sending data to the socket
        /// Default: 600
        /// </summary>
        public int SocketSendTimeout {
            get {
                return _SocketSendTimeout;
            }
            set {
                _SocketSendTimeout = value;
            }
        }
        
        /// <summary>
        /// Interval in seconds to run the idle worker
        /// Default: 60
        /// </summary>
        public int IdleWorkerInterval {
            get {
                return _IdleWorkerInterval;
            }
            set {
                _IdleWorkerInterval = value;
            }
        }

        /// <summary>
        /// Interval in seconds to send a PING
        /// Default: 60
        /// </summary>
        public int PingInterval {
            get {
                return _PingInterval;
            }
            set {
                _PingInterval = value;
            }
        }
        
        /// <summary>
        /// Timeout in seconds for server response to a PING
        /// Default: 600
        /// </summary>
        public int PingTimeout {
            get {
                return _PingTimeout;
            }
            set {
                _PingTimeout = value;
            }
        }

        /// <summary>
        /// Latency between client and the server
        /// </summary>
        public TimeSpan Lag {
            get {
                return PingStopwatch.Elapsed;
            }
        }

        
        /// <summary>
        /// If you want to use a Proxy, set the ProxyHost to Host of the Proxy you want to use.
        /// </summary>
        public string ProxyHost {
            get {
                return _ProxyHost;
            }
            set {
                _ProxyHost = value;
            }
        }

        /// <summary>
        /// If you want to use a Proxy, set the ProxyPort to Port of the Proxy you want to use.
        /// </summary>
        public int ProxyPort {
            get {
                return _ProxyPort;
            }
            set {
                _ProxyPort = value;
            }
        }
        
        /// <summary>
        /// Standard Setting is to use no Proxy Server, if you Set this to any other value,
        /// you have to set the ProxyHost and ProxyPort aswell (and give credentials if needed)
        /// Default: ProxyType.None
        /// </summary>
        public ProxyType ProxyType {
            get {
                return _ProxyType;
            }
            set {
                _ProxyType = value;
            }
        }
        
        /// <summary>
        /// Username to your Proxy Server
        /// </summary>
        public string ProxyUsername {
            get {
                return _ProxyUsername;
            }
            set {
                _ProxyUsername = value;
            }
        }
        
        /// <summary>
        /// Password to your Proxy Server
        /// </summary>
        public string ProxyPassword {
            get {
                return _ProxyPassword;
            }
            set {
                _ProxyPassword = value;
            }
        }

        public CancellationToken CancellationToken { get; }


        /// <summary>
        /// Initializes the message queues, read and write thread
        /// </summary>
        public IrcConnection()
        {
            Dictionary<Priority, Channel<string>> mutable = new Dictionary<Priority, Channel<string>>();
            for (int i = (int) Priority.Low; i <= (int) Priority.High; i++) 
                mutable.Add((Priority) i, System.Threading.Channels.Channel.CreateUnbounded<string>(new UnboundedChannelOptions{
                    SingleReader = true,
                    SingleWriter = true,
                    AllowSynchronousContinuations = true
                }));
            _SendBuffer = new ReadOnlyDictionary<Priority, Channel<string>>(mutable);

            // setup own callbacks
            OnReadLine        += new ReadLineEventHandler(_SimpleParser);
            OnConnectionError += new EventHandler(_OnConnectionError);

            PingStopwatch = new Stopwatch();
            NextPingStopwatch = new Stopwatch();

            Assembly assm = Assembly.GetAssembly(this.GetType());
            AssemblyName assm_name = assm.GetName(false);

            AssemblyProductAttribute pr = (AssemblyProductAttribute)assm.GetCustomAttributes(typeof(AssemblyProductAttribute), false)[0];

            _VersionNumber = assm_name.Version.ToString();
            _VersionString = pr.Product+" "+_VersionNumber;
        }

        public IrcConnection(CancellationToken cancellationToken) : this()
        {
            CancellationToken = cancellationToken;
        }
        
        ~IrcConnection()
        {
            for (int i = (int) Priority.Low; i < (int) Priority.High; i++)
                _SendBuffer[(Priority) i].Writer.TryComplete();
            Log("IrcConnection destroyed");
        }

        [Conditional("DEBUGGING")]
        protected void Log (string format, params object[] parameters)
        {
            UnityEngine.Debug.LogFormat(format, parameters);
        }

        [Conditional("DEBUGGING")]
        void LogError (string format, params object[] parameters)
        {
            UnityEngine.Debug.LogErrorFormat(format, parameters);
        }
        
        void Connect (object state)
        {
            (string[] ips, int port) p = ((string[] ips, int port)) state;
            Connect(p.ips, p.port);
        }

        /// <overloads>this method has 2 overloads</overloads>
        /// <summary>
        /// Connects to the specified server and port, when the connection fails
        /// the next server in the list will be used.
        /// </summary>
        /// <param name="addresslist">List of servers to connect to</param>
        /// <param name="port">Portnumber to connect to</param>
        /// <exception cref="CouldNotConnectException">The connection failed</exception>
        /// <exception cref="AlreadyConnectedException">If there is already an active connection</exception>
        public void Connect (string[] addresslist, int port)
        {
            if (IsConnected) {
                throw new AlreadyConnectedException("Already connected to: " + Address + ":" + Port);
            }
            
            bool reconnecting = true;
            Addresses = (string[])addresslist.Clone();
            int usingAddress = 0, attempts = -1;
            Port = port;
            while (reconnecting)
            {
                attempts++;
                Log("[IRC] Connecting to {0}... (attempt#{1})", Addresses[usingAddress], attempts);
                Address = Addresses[usingAddress];


                OnConnecting?.Invoke(this, EventArgs.Empty);
                try {
                    tcpClient = new TcpClient();
                    tcpClient.NoDelay = true;
                    tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
                    // set timeout, after this the connection will be aborted
                    tcpClient.ReceiveTimeout = _SocketReceiveTimeout * 1000;
                    tcpClient.SendTimeout = _SocketSendTimeout * 1000;
                    
                    if (_ProxyType != ProxyType.None) {
                        IProxyClient proxyClient = null;
                        ProxyClientFactory proxyFactory = new ProxyClientFactory();
                        // HACK: map our ProxyType to Starksoft's ProxyType
                        Starksoft.Net.Proxy.ProxyType proxyType = 
                            (Starksoft.Net.Proxy.ProxyType) Enum.Parse(
                                typeof(ProxyType), _ProxyType.ToString(), true
                            );
                        
                        if (_ProxyUsername == null && _ProxyPassword == null) {
                            proxyClient = proxyFactory.CreateProxyClient(
                                proxyType
                            );
                        } else {
                            proxyClient = proxyFactory.CreateProxyClient(
                                proxyType,
                                _ProxyHost,
                                _ProxyPort,
                                _ProxyUsername,
                                _ProxyPassword
                            );
                        }
                        
                        tcpClient.Connect(_ProxyHost, _ProxyPort);
                        proxyClient.TcpClient = tcpClient;
                        proxyClient.CreateConnection(Address, port);
                    } else {
                        tcpClient.Connect(Address, port);
                    }
                    
                    Stream stream = tcpClient.GetStream();
                    if (_UseSsl)
                    {
                        RemoteCertificateValidationCallback certValidation;
                        if (_ValidateServerCertificate) {
                            certValidation = ServicePointManager.ServerCertificateValidationCallback;
                            if (certValidation == null) {
                                certValidation = delegate(object sender,
                                    X509Certificate certificate,
                                    X509Chain chain,
                                    SslPolicyErrors sslPolicyErrors) {
                                    if (sslPolicyErrors == SslPolicyErrors.None) {
                                        return true;
                                    }

                                    LogError("Connect(): Certificate error: " + sslPolicyErrors);
                                    return false;
                                };
                            }
                        } else {
                            certValidation = delegate { return true; };
                        }
                        RemoteCertificateValidationCallback certValidationWithIrcAsSender =
                            delegate(object sender, X509Certificate certificate,
                                    X509Chain chain, SslPolicyErrors sslPolicyErrors) {
                            return certValidation(this, certificate, chain, sslPolicyErrors);
                        };
                        LocalCertificateSelectionCallback selectionCallback = delegate(object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers) {
                            if (localCertificates == null || localCertificates.Count == 0) {
                                return null;
                            }
                            return localCertificates[0];
                        };
                        SslStream sslStream = new SslStream(stream, false,
                                                            certValidationWithIrcAsSender,
                                                            selectionCallback);
                        try {
                            if (_SslClientCertificate != null) {
                                var certs = new X509Certificate2Collection();
                                certs.Add(_SslClientCertificate);
                                sslStream.AuthenticateAsClient(Address, certs,
                                                            SslProtocols.Default,
                                                            false);
                            } else {
                                sslStream.AuthenticateAsClient(Address);
                            }
                        } catch (IOException ex) {
                            LogError("Connect(): AuthenticateAsClient() failed!");
                            throw new CouldNotConnectException("Could not connect to: " + Address + ":" + Port + " " + ex.Message, ex);
                        }
                        stream = sslStream;
                    }

                    // Connection was successful, reseting the connect counter

                    // updating the connection error state, so connecting is possible again
                    IsConnectionError = false;
                    IsConnected = true;
                    ReadWorker = new ReadTask(this, stream);
                    writeWorker = new WriteTask(this, stream);
                    KeepAliveWorker = new KeepAliveTask(this);
                    OnConnected?.Invoke(this, EventArgs.Empty);
                    attempts = 0;
                    reconnecting = false;
                } catch (AuthenticationException ex) {
                    Log("[IRC] Connect(): Exception", ex);
                    throw new CouldNotConnectException("Could not connect to: " + Address + ":" + Port + " " + ex.Message, ex);
                } catch (Exception e) {
                    LogError("[IRC] Unexpected exception happened in Connect: {0}", e);
                    ReadWorker?.Stop();
                    writeWorker?.Stop();
                    tcpClient?.Close();
                    IsConnected = false;
                    Interlocked.Increment(ref connectionErrorFlag);

                    if (AutoRetry && (AutoRetryLimit <= 0 || AutoRetryLimit <= attempts))
                    {
                        if (attempts >= 1) OnAutoConnectError?.Invoke(this, new AutoConnectErrorEventArgs(Address, Port, e));
                        Log("[IRC] Delaying new connect attempt for "+AutoRetryDelay+" sec");
                        Thread.Sleep(AutoRetryDelay * 1000);
                        if (usingAddress++ >= Addresses.Length) usingAddress = 0;
                        reconnecting = true;
                    }
                    else
                    {
                        reconnecting = false;
                        throw new CouldNotConnectException("Could not connect to: "+Address+":"+Port+" "+e.Message, e);
                    }
                }
            }
            
        }
        async Task ConnectAsync (string[] addresslist, int port, CancellationToken ct)
        {
            if (IsConnected) {
                throw new AlreadyConnectedException("Already connected to: " + Address + ":" + Port);
            }
            
            bool reconnecting = true;
            Addresses = (string[])addresslist.Clone();
            int usingAddress = -1, attempts = -1;
            Port = port;
            while (reconnecting)
            {
                if (++usingAddress >= Addresses.Length) usingAddress = 0;
                attempts++;
                Log("[IRC] Connecting to {0} (attempt#{1})", Addresses[usingAddress], attempts);
                Address = Addresses[usingAddress];


                OnConnecting?.Invoke(this, EventArgs.Empty);
                try {
                    tcpClient = new TcpClient();
                    tcpClient.NoDelay = true;
                    tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
                    // set timeout, after this the connection will be aborted
                    tcpClient.ReceiveTimeout = _SocketReceiveTimeout * 1000;
                    tcpClient.SendTimeout = _SocketSendTimeout * 1000;
                    
                    if (_ProxyType != ProxyType.None) {
                        IProxyClient proxyClient = null;
                        ProxyClientFactory proxyFactory = new ProxyClientFactory();
                        // HACK: map our ProxyType to Starksoft's ProxyType
                        Starksoft.Net.Proxy.ProxyType proxyType = 
                            (Starksoft.Net.Proxy.ProxyType) Enum.Parse(
                                typeof(ProxyType), _ProxyType.ToString(), true
                            );
                        
                        if (_ProxyUsername == null && _ProxyPassword == null) {
                            proxyClient = proxyFactory.CreateProxyClient(
                                proxyType
                            );
                        } else {
                            proxyClient = proxyFactory.CreateProxyClient(
                                proxyType,
                                _ProxyHost,
                                _ProxyPort,
                                _ProxyUsername,
                                _ProxyPassword
                            );
                        }
                        
                        tcpClient.Connect(_ProxyHost, _ProxyPort);
                        proxyClient.TcpClient = tcpClient;
                        proxyClient.CreateConnection(Address, port);
                    } else {
                        tcpClient.Connect(Address, port);
                    }
                    
                    Stream stream = tcpClient.GetStream();
                    if (_UseSsl)
                    {
                        RemoteCertificateValidationCallback certValidation;
                        if (_ValidateServerCertificate) {
                            certValidation = ServicePointManager.ServerCertificateValidationCallback;
                            if (certValidation == null) {
                                certValidation = delegate(object sender,
                                    X509Certificate certificate,
                                    X509Chain chain,
                                    SslPolicyErrors sslPolicyErrors) {
                                    if (sslPolicyErrors == SslPolicyErrors.None) {
                                        return true;
                                    }

                                    LogError("Connect(): Certificate error: " + sslPolicyErrors);
                                    return false;
                                };
                            }
                        } else {
                            certValidation = delegate { return true; };
                        }
                        RemoteCertificateValidationCallback certValidationWithIrcAsSender =
                            delegate(object sender, X509Certificate certificate,
                                    X509Chain chain, SslPolicyErrors sslPolicyErrors) {
                            return certValidation(this, certificate, chain, sslPolicyErrors);
                        };
                        LocalCertificateSelectionCallback selectionCallback = delegate(object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers) {
                            if (localCertificates == null || localCertificates.Count == 0) {
                                return null;
                            }
                            return localCertificates[0];
                        };
                        SslStream sslStream = new SslStream(stream, false,
                                                            certValidationWithIrcAsSender,
                                                            selectionCallback);
                        try {
                            if (_SslClientCertificate != null) {
                                var certs = new X509Certificate2Collection();
                                certs.Add(_SslClientCertificate);
                                sslStream.AuthenticateAsClient(Address, certs,
                                                            SslProtocols.Default,
                                                            false);
                            } else {
                                sslStream.AuthenticateAsClient(Address);
                            }
                        } catch (IOException ex) {
                            LogError("Connect(): AuthenticateAsClient() failed!");
                            throw new CouldNotConnectException("Could not connect to: " + Address + ":" + Port + " " + ex.Message, ex);
                        }
                        stream = sslStream;
                    }

                    // Connection was successful, reseting the connect counter

                    // updating the connection error state, so connecting is possible again
                    IsConnectionError = false;
                    IsConnected = true;
                    ReadWorker = new ReadTask(this, stream);
                    writeWorker = new WriteTask(this, stream);
                    KeepAliveWorker = new KeepAliveTask(this);
                    OnConnected?.Invoke(this, EventArgs.Empty);
                    attempts = 0;
                    reconnecting = false;
                } catch (AuthenticationException ex) {
                    Log("[IRC] Connect(): Exception", ex);
                    throw new CouldNotConnectException("Could not connect to: " + Address + ":" + Port + " " + ex.Message, ex);
                } catch (Exception e) {
                    LogError("[IRC] Unexpected exception happened in Connect: {0}", e);
                    ReadWorker?.Stop();
                    writeWorker?.Stop();
                    tcpClient?.Close();
                    IsConnected = false;
                    Interlocked.Increment(ref connectionErrorFlag);

                    ct.ThrowIfCancellationRequested();
                    if (AutoRetry && (AutoRetryLimit <= 0 || AutoRetryLimit <= attempts))
                    {
                        if (attempts >= 1) OnAutoConnectError?.Invoke(this, new AutoConnectErrorEventArgs(Address, Port, e));
                        Log("[IRC] Delaying new connect attempt for "+AutoRetryDelay+" sec");
                        await Task.Delay(AutoRetryDelay * 1000);
                        reconnecting = true;
                    }
                    else
                    {
                        reconnecting = false;
                        throw new CouldNotConnectException("Could not connect to: "+Address+":"+Port+" "+e.Message, e);
                    }
                }
            }
            
        }

        /// <summary>
        /// Connects to the specified server and port.
        /// </summary>
        /// <param name="address">Server address to connect to</param>
        /// <param name="port">Port number to connect to</param>
        public void Connect(string address, int port)
        {
            Connect(new string[] { address }, port);
        }

        public virtual async Task ConnectTask (string[] addresses, int port)
        {
            Log("[IRC] IrcConnection.ConnectAsync(addresses, port, cts)");
            await ConnectAsync(addresses, port, CancellationToken);
        }

        public System.Threading.Tasks.Task StartConnectTask(string address, int port)
        => Task.Run(async () => await ConnectTask(new string[] { address }, port));

        /// <summary>
        /// Reconnects to the server
        /// </summary>
        /// <exception cref="NotConnectedException">
        /// If there was no active connection
        /// </exception>
        /// <exception cref="CouldNotConnectException">
        /// The connection failed
        /// </exception>
        /// <exception cref="AlreadyConnectedException">
        /// If there is already an active connection
        /// </exception>
        public void Reconnect()
        {
            if (IsReconnecting) return;
            try
            {
                Log("[IRC] Reconnecting...");
                Disconnect();
                Connect(Addresses, Port);
            }
            finally
            {
                IsReconnecting = false;
            }
        }
        
        public async Task ReconnectTask()
        {
            if (IsReconnecting) return;
            Log("[IRC] Reconnecting...");
            Disconnect();
            await StartConnectTask(Address, Port).ContinueWith((t) => {
                IsReconnecting = false;
            });
        }

        /// <summary>
        /// Disconnects from the server
        /// </summary>
        /// <exception cref="NotConnectedException">
        /// If there was no active connection
        /// </exception>
        public void Disconnect()
        {
            if (!IsConnected) {
                throw new NotConnectedException("The connection could not be disconnected because there is no active connection");
            }
            if (OnDisconnecting != null) {
                OnDisconnecting(this, EventArgs.Empty);
            }
            Log("[IRC] IrcConnection.Disconnect().");
            KeepAliveWorker.Stop();
            ReadWorker.Stop();
            writeWorker.Stop();
            tcpClient.Close();
            IsConnected = false;
            _IsRegistered = false;
                        
            if (OnDisconnected != null) {
                OnDisconnected(this, EventArgs.Empty);
            }

#if LOG4NET
            Logger.Connection.Info("disconnected");
#endif
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blocking"></param>
        public void Listen(bool blocking)
        {
            if (blocking) {
                while (IsConnected) {
                    ReadLine(true);
                }
            } else {
                while (ReadLine(false) != null) {
                    // loop as long as we receive messages
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Listen()
        {
            Listen(true);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="blocking"></param>
        public void ListenOnce(bool blocking)
        {
            ReadLine(blocking);
        }

        /// <summary>
        /// 
        /// </summary>
        public void ListenOnce()
        {
            ListenOnce(true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blocking">Does the calling thread wait until a line is read</param>
        /// <returns></returns>
        public string ReadLine(bool blocking)
        {
            if (ReadWorker?.Read == null) return null;

            string data;
            if (blocking) // Blocking means it this thread wait until an item is successfully taken
            {
                while (!ReadWorker.Read.TryTake(out data, 1000)) // Keep looping until something is retrieved
                    if (!IsConnected || Interlocked.Read(ref connectionErrorFlag) > 0 || ReadWorker.Read.IsAddingCompleted) break;
                
                OnReadLine?.Invoke(this, new ReadLineEventArgs(data));
                Log("[IRC] < "+data);
            }
            else
            {
                if (ReadWorker.Read.TryTake(out data) && data.Length > 0)
                {
                    OnReadLine?.Invoke(this, new ReadLineEventArgs(data));
                    Log("[IRC] < "+data);
                }
            }

            if (IsConnectionError && !IsConnectionErrorIssued)
            {
                OnConnectionError?.Invoke(this, EventArgs.Empty);
                IsConnectionErrorIssued = true;
            }
            
            
            return data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="priority"></param>
        public void WriteLine(string data, Priority priority)
        {
            if (priority == Priority.Critical) {
                if (!IsConnected) {
                    throw new NotConnectedException();
                }
                
                writeWorker.WriteLine(data);
            } else {
                _SendBuffer[priority].Writer.WriteAsync(data); // Writing to unbounded channels should always success
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void WriteLine(string data)
        {
            WriteLine(data, Priority.Medium);
        }


        private void _SimpleParser(object sender, ReadLineEventArgs args)
        {
            string   rawline = args.Line;
            string[] rawlineex = rawline.Split(new char[] {' '});
            string   line = null;
            string   prefix = null;
            string   command = null;

            if (rawline[0] == ':') {
                prefix = rawlineex[0].Substring(1);
                line = rawline.Substring(prefix.Length + 2);
            } else {
                line = rawline;
            }
            string[] lineex = line.Split(new char[] {' '});

            command = lineex[0];
            ReplyCode replycode = ReplyCode.Null;
            int intReplycode;
            if (Int32.TryParse(command, out intReplycode)) {
                replycode = (ReplyCode) intReplycode;
            }
            if (replycode != ReplyCode.Null) {
                switch (replycode) {
                    case ReplyCode.Welcome:
                        _IsRegistered = true;
#if LOG4NET
                        Logger.Connection.Info("logged in");
#endif
                        break;
                }
            } else {
                switch (command) {
                    case "ERROR":
                        // FIXME: handle server errors differently than connection errors!
                        //IsConnectionError = true;
                        break;
                    case "PONG":
                        PingStopwatch.Stop();
                        NextPingStopwatch.Reset();
                        NextPingStopwatch.Start();

#if LOG4NET
                        Logger.Connection.Debug("PONG received, took: " + PingStopwatch.ElapsedMilliseconds + " ms");
#endif
                        break;
                }
            }
        }

        private async void _OnConnectionError(object sender, EventArgs e)
        {
            try {
                if (AutoReconnect) {
                    // prevent connect -> exception -> connect flood loop
                    await Task.Delay(AutoRetryDelay * 1000);
                    // lets try to recover the connection
                    ReconnectTask();
                } else {
                    // make sure we clean up
                    Disconnect();
                }
            } catch (ConnectionException) {
            }
        }
    }
}
