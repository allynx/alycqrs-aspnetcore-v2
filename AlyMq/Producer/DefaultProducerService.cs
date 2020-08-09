using AlyMq.Producer.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

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

            byte[] keyBytes = new byte[16];
            ms.Read(keyBytes, 0, 16);
            Guid key = new Guid(keyBytes);


            byte[] lenghtBytes = new byte[4];
            ms.Read(lenghtBytes, 0, 4);
            int length = BitConverter.ToInt32(lenghtBytes);

            byte[] msgBytes = new byte[length];
            ms.Read(msgBytes, 0, length);

            //Todo Reviced ...

            offset = offset + keyBytes.Length + lenghtBytes.Length + msgBytes.Length;

            if (ms.Length > offset)
            {
                ApartMessage(socket, ms, offset);
            }

            return Task.CompletedTask;
        }
    }
}
