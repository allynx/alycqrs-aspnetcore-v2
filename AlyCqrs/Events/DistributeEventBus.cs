using AlyCqrs.Configuration;
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

namespace AlyCqrs.Events
{
    public class DistributeEventBus : IEventBus
    {
        private readonly ILogger<DistributeEventBus> _logger;
        private readonly ConcurrentQueue<Event> _eventQueue;
        private readonly Socket _producer;

        public DistributeEventBus(ILogger<DistributeEventBus> logger) {
            _logger = logger;
            _eventQueue = new ConcurrentQueue<Event>();
            _producer = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            ConnectProducerAsync();
        }

        public Task PublishAsync<T>(T evnt) where T : Event
        {
            _eventQueue.Enqueue(evnt);
            return Task.CompletedTask;
        }

        private Task ConnectProducerAsync()
        {
            IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse(CqrsProducerConfig.Instance.Ip), CqrsProducerConfig.Instance.Port);

            SocketAsyncEventArgs args = new SocketAsyncEventArgs();
            args.Completed += (s, e) => ConnectCallbackAsync(s, e);
            args.RemoteEndPoint = ipEndPoint;

            try
            {
                if (!_producer.ConnectAsync(args))
                {
                    ConnectCallbackAsync(this, args);
                }

                return Task.CompletedTask;
            }
            catch (NotSupportedException nse) { throw nse; }
            catch (ObjectDisposedException oe) { throw oe; }
            catch (SocketException se) { throw se; }
            catch (Exception ex) { throw ex; }
        }


        private Task ConnectCallbackAsync(object send, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success)
            {
                _logger.LogInformation($"Producer {args.RemoteEndPoint} is connected ...");


                Timer connectTimer = new Timer(30000);
                connectTimer.Enabled = true;
                connectTimer.Elapsed += (s, e) =>
                {
                    if (!_producer.Connected)
                    {
                        ConnectProducerAsync();
                    }
                };

                Timer sendTimer = new Timer();
                sendTimer.Enabled = true;
                sendTimer.Elapsed += (s, e) =>
                {
                    if (_eventQueue.Count > 0)
                    {
                        Event evnt;
                        if (_eventQueue.TryDequeue(out evnt))
                        {
                            using (MemoryStream msBuffer = new MemoryStream())
                            {
                                using (MemoryStream msBody = new MemoryStream())
                                {

                                    byte[] topicKeyBuffer = CqrsTopicConfig.Instance.EventTopicKey.ToByteArray();
                                    int topicKeyLength = topicKeyBuffer.Length;

                                    byte[] topicTagBuffer = Encoding.UTF8.GetBytes(evnt.AggregateKey.ToString());
                                    int topicTagLength = topicTagBuffer.Length;

                                    IFormatter iFormatter = new BinaryFormatter();
                                    iFormatter.Serialize(msBody, evnt);
                                    byte[] bodyBuffer = msBody.GetBuffer();
                                    int bodyLength = bodyBuffer.Length;

                                    msBuffer.Write(BitConverter.GetBytes(topicKeyLength));
                                    msBuffer.Write(BitConverter.GetBytes(topicTagLength));
                                    msBuffer.Write(BitConverter.GetBytes(bodyLength));
                                    msBuffer.Write(topicKeyBuffer);
                                    msBuffer.Write(topicTagBuffer);
                                    msBuffer.Write(bodyBuffer);

                                    byte[] buffer = msBuffer.GetBuffer();
                                    SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                                    args.AcceptSocket = _producer;
                                    args.SetBuffer(buffer, 0, buffer.Length);

                                    try
                                    {
                                        _producer.SendAsync(args);
                                    }
                                    catch (NotSupportedException nse) { throw nse; }
                                    catch (ObjectDisposedException oe) { throw oe; }
                                    catch (SocketException se) { throw se; }
                                    catch (Exception ex) { throw ex; }
                                }
                            }
                        }
                    }
                };
            }
            else
            {
                _logger.LogInformation($"Producer {args.RemoteEndPoint} connect is failed ...");
            }
            return Task.CompletedTask;
        }
    }
}
