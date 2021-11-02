using AlyMq.Producer.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace AlyMq.Producer
{
    public class DefaultProducerService : IProducerService
    {
        private Socket _producter;
        private readonly List<Socket> _clients;
        private readonly ILogger<DefaultProducerService> _logger;

        public DefaultProducerService(ILogger<DefaultProducerService> logger)
        {
            _logger = logger;
            _clients = new List<Socket>();

            IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse(ProducerConfig.Instance.Address.Ip), ProducerConfig.Instance.Address.Port);
            _producter = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _producter.Bind(ipEndPoint);
        }

        private Task Startup()
        {
            try
            {
                _producter.Listen(ProducerConfig.Instance.Address.Backlog);

                _logger.LogInformation($"Producer server is listened: {_producter.LocalEndPoint} ...");


                SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                args.Completed += (s, e) => AcceptCallback(s, e);
                if (!_producter.AcceptAsync(args)) { AcceptCallback(this, args); }

                return Task.CompletedTask;
            }
            catch (NotSupportedException nse) { throw nse; }
            catch (ObjectDisposedException ode) { throw ode; }
            catch (SocketException se) { throw se; }
            catch (Exception e) { throw e; }
        }

        private Task AcceptCallback(object sender, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success)
            {
                SocketAsyncEventArgs rArgs = new SocketAsyncEventArgs();
                rArgs.Completed += (s, e) => ReceiveCallback(s, e);
                rArgs.AcceptSocket = args.AcceptSocket;
                rArgs.SetBuffer(new byte[8912], 0, 8912);
                rArgs.UserToken = new MemoryStream();

                _clients.Add(args.AcceptSocket);
                _logger.LogInformation($"Client {args.AcceptSocket.RemoteEndPoint} is accepted ...");

                try
                {
                    if (!rArgs.AcceptSocket.ReceiveAsync(rArgs)) { ReceiveCallback(this, rArgs); }

                    args.AcceptSocket = null;
                    if (!_producter.AcceptAsync(args)) { AcceptCallback(this, args); }
                }
                catch (NotSupportedException nse) { throw nse; }
                catch (ObjectDisposedException ode) { throw ode; }
                catch (SocketException se) { throw se; }
                catch (Exception e) { throw e; }
            }
            else
            {
                _logger.LogInformation($"Client {args.AcceptSocket.RemoteEndPoint} is closed ...");
                _clients.Remove(args.AcceptSocket);
                args.AcceptSocket.Close();
            }

            return Task.CompletedTask;
        }

        private Task ReceiveCallback(object sender, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success && args.BytesTransferred > 0)
            {
                MemoryStream ms = args.UserToken as MemoryStream;
                ms.Write(args.Buffer, args.Offset, args.BytesTransferred);

                if (args.AcceptSocket.Available == 0)
                {
                    ApartMessage(args.AcceptSocket, ms, 0);
                    ms.Seek(0, SeekOrigin.Begin);
                    ms.SetLength(0);
                }

                try
                {
                    if (!args.AcceptSocket.ReceiveAsync(args)) { ReceiveCallback(this, args); }
                }
                catch (NotSupportedException nse) { throw nse; }
                catch (ObjectDisposedException ode) { throw ode; }
                catch (SocketException se) { throw se; }
                catch (Exception e) { throw e; }
            }
            else
            {
                if (!args.AcceptSocket.SafeHandle.IsClosed)
                {
                    _logger.LogInformation($"Client {args.AcceptSocket.RemoteEndPoint} is accepted ...");
                    args.AcceptSocket.Shutdown(SocketShutdown.Both);
                    args.AcceptSocket.Close();
                }
            }

            return Task.CompletedTask;
        }

        private Task SendCallback(object sender, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success && args.BytesTransferred > 0)
            {
                byte[] keyBytes = new byte[16];
                Array.Copy(args.Buffer, 0, keyBytes, 0, 16);
                Guid key = new Guid(keyBytes);

                byte[] lenghtBytes = new byte[4];
                Array.Copy(args.Buffer, 16, lenghtBytes, 0, 4);
                int length = BitConverter.ToInt32(lenghtBytes);

                byte[] msgBytes = new byte[length];
                Array.Copy(args.Buffer, 20, msgBytes, 0, args.BytesTransferred - 20);

                //Todo Sent ...
            }
            else
            {
                if (!args.AcceptSocket.SafeHandle.IsClosed)
                {
                    //Todo client off ...
                    _clients.Remove(args.AcceptSocket);
                    args.AcceptSocket.Shutdown(SocketShutdown.Both);
                    args.AcceptSocket.Close();
                }
            }

            return Task.CompletedTask;
        }

        public Task Start()
        {
            return Startup();
        }

        public Task Stop()
        {
            _logger.LogInformation("Producer is stoped ...");

            return Task.CompletedTask;
        }

        private Task ApartMessage(Socket socket, MemoryStream ms, int offset)
        {
            ms.Seek(offset, SeekOrigin.Begin);

            byte[] topicKeyLengthBuffer = new byte[4];
            ms.Read(topicKeyLengthBuffer, 0, 4);
            int topicKeyLength = BitConverter.ToInt32(topicKeyLengthBuffer);

            byte[] topicTagLengthBuffer = new byte[4];
            ms.Read(topicTagLengthBuffer, 0, 4);
            int topicTagLength = BitConverter.ToInt32(topicTagLengthBuffer);


            byte[] bodyLengthBuffer = new byte[4];
            ms.Read(bodyLengthBuffer, 0, 4);
            int bodyLength = BitConverter.ToInt32(bodyLengthBuffer);


            byte[] topicKeyBuffer = new byte[topicKeyLength];
            ms.Read(topicKeyBuffer, 0, topicKeyLength);
            Guid topicKey = new Guid(topicKeyBuffer);

            byte[] topicTagBuffer = new byte[topicTagLength];
            ms.Read(topicTagBuffer, 0, topicTagLength);
            string topicTag = Encoding.UTF8.GetString(topicTagBuffer);

            byte[] bodyBuffer = new byte[bodyLength];
            ms.Read(bodyBuffer, 0, bodyLength);


            //Todo Reviced ...

            offset = offset + topicKeyLength + topicTagLength + bodyLength + 12;

            if (ms.Length > offset)
            {
                ApartMessage(socket, ms, offset);
            }

            return Task.CompletedTask;
        }

        private Task PullBrokerAsync() {

            Timer timer = new Timer(30000);
            timer.Enabled = true;

            timer.Elapsed += (s, e) => {
                IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse(ProducerConfig.Instance.AdapterAddress.Ip), ProducerConfig.Instance.AdapterAddress.Port);
                Socket adapter = new Socket(ipEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                args.Completed += (s, e) => PullBrokerCallbackAsync(s, e);
                args.RemoteEndPoint = ipEndPoint;

                try
                {
                    if (!adapter.ConnectAsync(args)) {
                        PullBrokerCallbackAsync(this, args);
                    }
                }
                catch (NotSupportedException nse) { throw nse; }
                catch (ObjectDisposedException oe) { throw oe; }
                catch (SocketException se) { throw se; }
                catch (Exception ex) { throw ex; };
            };

            return Task.CompletedTask;
        }

        private Task PullBrokerCallbackAsync(object send, SocketAsyncEventArgs args) {

            if (args.SocketError == SocketError.Success)
            {
                SocketAsyncEventArgs rArgs = new SocketAsyncEventArgs();
                rArgs.Completed += (s, e) => ReviceBrokerCallbackAsync(s, e);
                rArgs.AcceptSocket = args.ConnectSocket;
                rArgs.SetBuffer(new byte[8912], 0, 8912);
                rArgs.UserToken = new MemoryStream();

                _logger.LogInformation($"Adapter {args.AcceptSocket.RemoteEndPoint} is connected ...");

                try
                {
                    if (!rArgs.ConnectSocket.ReceiveAsync(rArgs)) { ReviceBrokerCallbackAsync(this, rArgs); }
                }
                catch (NotSupportedException nse) { throw nse; }
                catch (ObjectDisposedException ode) { throw ode; }
                catch (SocketException se) { throw se; }
                catch (Exception e) { throw e; }
            }
            else
            {
                _logger.LogInformation($"Adapter {args.AcceptSocket.RemoteEndPoint} is closed ...");
                args.ConnectSocket.Close();
            }

            return Task.CompletedTask;
        }

        private Task ReviceBrokerCallbackAsync(object send, SocketAsyncEventArgs args) {

            if (args.SocketError == SocketError.Success && args.BytesTransferred > 0)
            {
                MemoryStream ms = args.UserToken as MemoryStream;
                ms.Write(args.Buffer, args.Offset, args.BytesTransferred);

                if (args.AcceptSocket.Available == 0)
                {
                    //Todo update broker and topic ....

                    ms.Seek(0, SeekOrigin.Begin);
                    ms.SetLength(0);
                }

                try
                {
                    if (!args.AcceptSocket.ReceiveAsync(args)) { ReviceBrokerCallbackAsync(this, args); }
                }
                catch (NotSupportedException nse) { throw nse; }
                catch (ObjectDisposedException ode) { throw ode; }
                catch (SocketException se) { throw se; }
                catch (Exception e) { throw e; }
            }
            else
            {
                if (!args.AcceptSocket.SafeHandle.IsClosed)
                {
                    _logger.LogInformation($"Adapter {args.AcceptSocket.RemoteEndPoint} is closed ...");
                    args.AcceptSocket.Shutdown(SocketShutdown.Both);
                    args.AcceptSocket.Close();
                }
            }

            return Task.CompletedTask;
        }
    }
}
