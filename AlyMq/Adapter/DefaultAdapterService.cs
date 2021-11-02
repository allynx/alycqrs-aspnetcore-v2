using AlyMq.Adapter.Configuration;
using AlyMq.Broker;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;

namespace AlyMq.Adapter
{
    public class DefaultAdapterService : IAdapterService
    {
        private Socket _adapter;
        private TopicComparer _topicComparer;
        private readonly IDictionary<Socket,KeyValuePair<BrokerInfo,HashSet<Topic>>> _brokers;
        private readonly ILogger<DefaultAdapterService> _logger;

        public DefaultAdapterService(ILogger<DefaultAdapterService> logger, TopicComparer topicComparer)
        {
            _logger = logger;
            _topicComparer = topicComparer;
            _brokers = new Dictionary<Socket, KeyValuePair<BrokerInfo, HashSet<Topic>>>();

            IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse(AdapterConfig.Instance.Address.Ip), AdapterConfig.Instance.Address.Port);
            _adapter = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _adapter.Bind(ipEndPoint);
        }

        private Task Startup()
        {
            try
            {
                _adapter.Listen(AdapterConfig.Instance.Address.Backlog);

                _logger.LogInformation($"Adapter server is listened: {_adapter.LocalEndPoint.ToString()} ...");

                SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                args.Completed += (s, e) => AcceptCallback(s, e);
                if (!_adapter.AcceptAsync(args)) { AcceptCallback(this, args); }

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

                _logger.LogInformation($"Client {args.AcceptSocket.RemoteEndPoint} is accepted ...");

                try
                {
                    if (!rArgs.AcceptSocket.ReceiveAsync(rArgs)) { ReceiveCallback(this, rArgs); }

                    args.AcceptSocket = null;
                    if (!_adapter.AcceptAsync(args)) { AcceptCallback(this, args); }
                }
                catch (NotSupportedException nse) { throw nse; }
                catch (ObjectDisposedException ode) { throw ode; }
                catch (SocketException se) { throw se; }
                catch (Exception e) { throw e; }
            }
            else
            {
                _logger.LogInformation($"Client {args.AcceptSocket.RemoteEndPoint} is closed ...");
                _brokers.Remove(args.AcceptSocket);
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
                    _brokers.Remove(args.AcceptSocket);
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
               
                //Todo Sent ...
            }
            else
            {
                if (!args.AcceptSocket.SafeHandle.IsClosed)
                {
                    //Todo client off ...
                    _brokers.Remove(args.AcceptSocket);
                    args.AcceptSocket.Shutdown(SocketShutdown.Both);
                    args.AcceptSocket.Close();
                }
            }

            return Task.CompletedTask;
        }

        public Task Start()
        {
           return  Startup();
        }

        public Task Stop()
        {
            _logger.LogInformation("Addapter is stoped ...");

            return Task.CompletedTask;
        }

        #region Helper

        private Task ApartMessage(Socket socket, MemoryStream ms, int offset)
        {
            if (ms.Length > offset)
            {
                ms.Seek(offset, SeekOrigin.Begin);

                byte[] typeBuffer = new byte[4];
                ms.Read(typeBuffer, 0, 4);
                MsgType type = (MsgType)BitConverter.ToInt32(typeBuffer);

                switch (type)
                {
                    case MsgType.Broker:
                        ApartBrokerPulse(socket, ms);
                        break;
                    default:
                        break;
                }
            }

            return Task.CompletedTask;
        }

        private Task ApartBrokerPulse(Socket socket, MemoryStream memoryStream)
        {
            byte[] brokerLengthBuffer = new byte[4];
            memoryStream.Read(brokerLengthBuffer, 0, 4);
            int brokerLength = BitConverter.ToInt32(brokerLengthBuffer);

            byte[] topicLengthBuffer = new byte[4];
            memoryStream.Read(topicLengthBuffer, 0, 4);
            int topicLength = BitConverter.ToInt32(topicLengthBuffer);

            byte[] brokerBuffer = new byte[brokerLength];
            memoryStream.Read(brokerBuffer, 0, brokerLength);

            byte[] topicBuffer = new byte[topicLength];
            memoryStream.Read(topicBuffer, 0, topicLength);

            using (MemoryStream msBroker = new MemoryStream(brokerBuffer))
            {
                using (MemoryStream msTopic = new MemoryStream(topicBuffer))
                {
                    IFormatter iFormatter = new BinaryFormatter();

                    BrokerInfo brokerInfo = iFormatter.Deserialize(msBroker) as BrokerInfo;

                    List<Topic> topics = iFormatter.Deserialize(msTopic) as List<Topic>;

                    KeyValuePair<BrokerInfo, HashSet<Topic>> kv;
                    if (_brokers.TryGetValue(socket, out kv))
                    {
                        kv.Key.CreateOn = DateTime.Now;
                        kv.Value.Intersect(topics);
                    }
                    else {
                        HashSet<Topic> hsTopics = new HashSet<Topic>(topics,new TopicComparer());
                        _brokers.Add(socket, new KeyValuePair<BrokerInfo, HashSet<Topic>>(brokerInfo,hsTopics));
                    }

                    _logger.LogInformation($"Broker [{brokerInfo.Name} -> {brokerInfo.Ip}:{brokerInfo.Port.ToString()}] is reported ...");
                }
            }

            int offset = brokerLength + topicLength + 12;//12 = RvcTyle length byte + BrokerInfo length byte + Topics length byte

            ApartMessage(socket, memoryStream, offset);

            return Task.CompletedTask;
        }

        #endregion
    }
}
