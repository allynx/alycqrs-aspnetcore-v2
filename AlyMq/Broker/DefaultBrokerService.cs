using AlyMq.Broker.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace AlyMq.Broker
{
    public class DefaultBrokerService : IBrokerService
    {
        private Socket _broker;
        private readonly List<Socket> _clients;
        private readonly BrokerInfo _brokerInfo;
        private readonly List<Topic> _topics;
        private readonly List<MsgQueue> _queues;
        private readonly List<SocketAdapter> _adapters;
        private readonly ILogger<DefaultBrokerService> _logger;

        public DefaultBrokerService(ILogger<DefaultBrokerService> logger)
        {
            _logger = logger;
            _brokerInfo = new BrokerInfo
            {
                Ip = BrokerConfig.Instance.Address.Ip,
                Key = BrokerConfig.Instance.Key,
                Backlog = BrokerConfig.Instance.Address.Backlog,
                Name = BrokerConfig.Instance.Name,
                Port = BrokerConfig.Instance.Address.Port,
                CreateOn = DateTime.Now
            };
            _topics = new List<Topic>();
            _queues = new List<MsgQueue>();
            _clients = new List<Socket>();
            _adapters = new List<SocketAdapter>();

            IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse(BrokerConfig.Instance.Address.Ip), BrokerConfig.Instance.Address.Port);
            _broker = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _broker.Bind(ipEndPoint);
        }

        private Task Startup()
        {
            try
            {
                _broker.Listen(BrokerConfig.Instance.Address.Backlog);

                _logger.LogInformation($"Broker server is listened: {_broker.LocalEndPoint} ...");

                AdapterConnect();

                InitDefaultTopicAndQueue();

                SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                args.Completed += (s, e) => AcceptCallback(s, e);
                if (!_broker.AcceptAsync(args)) { AcceptCallback(this, args); }

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
                    if (!_broker.AcceptAsync(args)) { AcceptCallback(this, args); }
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
            _logger.LogInformation("Broker is stoped ...");

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

        private Task AdapterConnect()
        {
            BrokerConfig.Instance.AdapterAddresses.ForEach(m =>
            {
                IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse(m.Ip), m.Port);
                Socket adapter = new Socket(ipEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                args.Completed += (s, e) => AdapterConnectCallback(s, e);
                args.RemoteEndPoint = ipEndPoint;
                
                try
                {
                    if (!adapter.ConnectAsync(args))
                    {
                        AdapterConnectCallback(this, args);
                    }
                }
                catch (NotSupportedException nse) { throw nse; }
                catch (ObjectDisposedException oe) { throw oe; }
                catch (SocketException se) { throw se; }
                catch (Exception e) { throw e; }

            });
            return Task.CompletedTask;
        }

        private Task AdapterConnectCallback(object send, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success)
            {
                _adapters.Add(new SocketAdapter { Socket = args.ConnectSocket });
                _logger.LogInformation($"Adapter {args.RemoteEndPoint} is connected ...");

                ReportBrokerTopics(args.ConnectSocket);

                BrokerPulse(args.ConnectSocket);
            }
            else
            {
                _logger.LogInformation($"Adapter {args.RemoteEndPoint} connect is failed ...");
            }
            return Task.CompletedTask;
        }

        private Task BrokerPulse(Socket adapter)
        {
            Timer timer = new Timer(1000 * 30);
            timer.Elapsed += (s, e) => ReportBrokerTopics(adapter,()=> timer.Dispose());
            timer.Enabled = true;
            return Task.CompletedTask;
        }

        private Task ReportBrokerTopics(Socket adapter, Action exceptioncallback = null)
        {
            using (var msBuffer = new MemoryStream())
            {
                using (var msBroker = new MemoryStream())
                {
                    using (var msTopic = new MemoryStream())
                    {
                        IFormatter iFormatter = new BinaryFormatter();

                        _brokerInfo.PulseOn = DateTime.Now;

                        iFormatter.Serialize(msBroker, _brokerInfo);
                        byte[] brokerBuffer = msBroker.GetBuffer();

                        iFormatter.Serialize(msTopic, _topics);
                        byte[] topicBuffer = msTopic.GetBuffer();

                        msBuffer.Write(BitConverter.GetBytes((int)MsgType.Broker));
                        msBuffer.Write(BitConverter.GetBytes(brokerBuffer.Length));
                        msBuffer.Write(BitConverter.GetBytes(topicBuffer.Length));
                        msBuffer.Write(brokerBuffer);
                        msBuffer.Write(topicBuffer);

                        byte[] buffer = msBuffer.GetBuffer();
                        SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                        args.AcceptSocket = adapter;
                        args.SetBuffer(buffer, 0, buffer.Length);

                        try
                        {
                            adapter.SendAsync(args);
                            _logger.LogInformation($"Report topic on: {DateTime.Now.ToString()} ...");
                        }
                        catch (NotSupportedException nse)
                        {
                            exceptioncallback?.Invoke(); throw nse;
                        }
                        catch (ObjectDisposedException oe)
                        {
                            exceptioncallback?.Invoke(); throw oe;
                        }
                        catch (SocketException se)
                        {
                            exceptioncallback?.Invoke(); throw se;
                        }
                        catch (Exception ex)
                        {
                            exceptioncallback?.Invoke(); throw ex;
                        }
                    }
                }
            }

            return Task.CompletedTask;
        }

        #region Default Broker Info

        private Task InitDefaultTopicAndQueue()
        {
            IConfiguration config = new ConfigurationBuilder()
              .AddJsonFile("brokerconfig.json", true, true)
              .Build();

            config.Bind("Topics", _topics);

            _topics.ForEach(t=> {
                _queues.Add(new MsgQueue { Key = Guid.NewGuid(), TopicKey = t.Key, Name = $"{t.Name}QueueA", Queue = new ConcurrentQueue<Msg>(), CreateOn = DateTime.Now });
                _queues.Add(new MsgQueue { Key = Guid.NewGuid(), TopicKey = t.Key, Name = $"{t.Name}QueueB", Queue = new ConcurrentQueue<Msg>(), CreateOn = DateTime.Now });
            });

            return Task.CompletedTask;
        }

        #endregion
    }
}
