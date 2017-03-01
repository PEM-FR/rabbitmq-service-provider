<?php

namespace fiunchinho\Silex\Provider;

use Pimple\Container;
use Pimple\ServiceProviderInterface;
// use Symfony\Component\Routing\Generator\UrlGenerator;
use OldSound\RabbitMqBundle\RabbitMq\Producer;
use OldSound\RabbitMqBundle\RabbitMq\Consumer;
use OldSound\RabbitMqBundle\RabbitMq\AnonConsumer;
use OldSound\RabbitMqBundle\RabbitMq\MultipleConsumer;
use OldSound\RabbitMqBundle\RabbitMq\RpcClient;
use OldSound\RabbitMqBundle\RabbitMq\RpcServer;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * Class RabbitServiceProvider
 * @package fiunchinho\Silex\Provider
 */
class RabbitServiceProvider implements ServiceProviderInterface
{
    /**
     * Default connection name
     */
    const DEFAULT_CONNECTION = 'default';

    /**
     * @param Container $pimple
     */
    public function register(Container $pimple)
    {
        $this->loadConnections($pimple);
        $this->loadProducers($pimple);
        $this->loadConsumers($pimple);
        $this->loadAnonymousConsumers($pimple);
        $this->loadMultipleConsumers($pimple);
        $this->loadRpcClients($pimple);
        $this->loadRpcServers($pimple);
    }

    /**
     * Return the name of the connection to use.
     * @param  Container $pimple      Options for the Producer or Consumer.
     * @param  array     $options     Options for the Producer or Consumer.
     * @param  array     $connections Connections defined in the config file.
     * @return AMQPStreamConnection   The connection name that will be used
     */
    private function getConnection(Container $pimple, $options, $connections)
    {
        $connection_name = @$options['connection']?: self::DEFAULT_CONNECTION;

        if (!isset($connections[$connection_name])) {
            throw new \InvalidArgumentException('Configuration for connection [' . $connection_name . '] not found');
        }

        return $pimple['rabbit.connection'][$connection_name];
    }

    /**
     * @param Container $pimple
     */
    private function loadConnections(Container $pimple)
    {
        $pimple['rabbit.connection'] = function (Container $pimple) {
            if (!isset($pimple['rabbit.connections'])) {
                throw new \InvalidArgumentException('You need to specify at least a connection in your configuration.');
            }

            $connections = [];
            foreach ($pimple['rabbit.connections'] as $name => $options) {
                $connection = new AMQPStreamConnection(
                    $pimple['rabbit.connections'][$name]['host'],
                    $pimple['rabbit.connections'][$name]['port'],
                    $pimple['rabbit.connections'][$name]['user'],
                    $pimple['rabbit.connections'][$name]['password'],
                    $pimple['rabbit.connections'][$name]['vhost']
                );

                $connections[$name] = $connection;
            }

            return $connections;
        };
    }

    /**
     * @param Container $pimple
     */
    private function loadProducers(Container $pimple)
    {
        $pimple['rabbit.producer'] = function (Container $pimple) {
            $producers = [];
            if (!isset($pimple['rabbit.producers'])) {
                return $producers;
            }

            foreach ($pimple['rabbit.producers'] as $name => $options) {
                $connection = $this->getConnection($pimple, $options, $pimple['rabbit.connections']);

                $producer = new Producer($connection);
                $producer->setExchangeOptions($options['exchange_options']);

                //this producer doesn't define a queue
                if (!isset($options['queue_options'])) {
                    $options['queue_options']['name'] = null;
                }
                $producer->setQueueOptions($options['queue_options']);

                if ((array_key_exists('auto_setup_fabric', $options)) && (!$options['auto_setup_fabric'])) {
                    $producer->disableAutoSetupFabric();
                }

                $producers[$name] = $producer;
            }

            return $producers;
        };
    }

    /**
     * @param Container $pimple
     */
    private function loadConsumers(Container $pimple)
    {
        $pimple['rabbit.consumer'] = function (Container $pimple) {
            $consumers = [];
            if (!isset($pimple['rabbit.consumers'])) {
                return $consumers;
            }

            foreach ($pimple['rabbit.consumers'] as $name => $options) {
                $connection = $this->getConnection($pimple, $options, $pimple['rabbit.connections']);
                $consumer = new Consumer($connection);
                $consumer->setExchangeOptions($options['exchange_options']);
                $consumer->setQueueOptions($options['queue_options']);
                $consumer->setCallback(array($pimple[$options['callback']], 'execute'));

                if (array_key_exists('qos_options', $options)) {
                    $consumer->setQosOptions(
                        $options['qos_options']['prefetch_size'],
                        $options['qos_options']['prefetch_count'],
                        $options['qos_options']['global']
                    );
                }

                if (array_key_exists('qos_options', $options)) {
                    $consumer->setIdleTimeout($options['idle_timeout']);
                }

                if ((array_key_exists('auto_setup_fabric', $options)) && (!$options['auto_setup_fabric'])) {
                    $consumer->disableAutoSetupFabric();
                }

                $consumers[$name] = $consumer;
            }

            return $consumers;
        };
    }

    /**
     * @param Container $pimple
     */
    private function loadAnonymousConsumers(Container $pimple)
    {
        $pimple['rabbit.anonymous_consumer'] = function (Container $pimple) {
            $consumers = [];
            if (!isset($pimple['rabbit.anon_consumers'])) {
                return $consumers;
            }

            foreach ($pimple['rabbit.anon_consumers'] as $name => $options) {
                $connection = $this->getConnection($pimple, $options, $pimple['rabbit.connections']);
                $consumer = new AnonConsumer($connection);
                $consumer->setExchangeOptions($options['exchange_options']);
                $consumer->setCallback(array($options['callback'], 'execute'));

                $consumers[$name] = $consumer;
            }

            return $consumers;
        };
    }

    /**
     * @param Container $pimple
     */
    private function loadMultipleConsumers(Container $pimple)
    {
        $pimple['rabbit.multiple_consumer'] = function (Container $pimple) {
            $consumers = [];
            if (!isset($pimple['rabbit.multiple_consumers'])) {
                return $consumers;
            }

            foreach ($pimple['rabbit.multiple_consumers'] as $name => $options) {
                $connection = $this->getConnection($pimple, $options, $pimple['rabbit.connections']);
                $consumer = new MultipleConsumer($connection);
                $consumer->setExchangeOptions($options['exchange_options']);
                $consumer->setQueues($options['queues']);

                if (array_key_exists('qos_options', $options)) {
                    $consumer->setQosOptions(
                        $options['qos_options']['prefetch_size'],
                        $options['qos_options']['prefetch_count'],
                        $options['qos_options']['global']
                    );
                }

                if (array_key_exists('qos_options', $options)) {
                    $consumer->setIdleTimeout($options['idle_timeout']);
                }

                if ((array_key_exists('auto_setup_fabric', $options)) && (!$options['auto_setup_fabric'])) {
                    $consumer->disableAutoSetupFabric();
                }

                $consumers[$name] = $consumer;
            }

            return $consumers;
        };
        
    }

    /**
     * @param Container $pimple
     */
    private function loadRpcClients(Container $pimple)
    {
        $pimple['rabbit.rpc_client'] = function (Container $pimple) {
            $clients = [];
            if (!isset($pimple['rabbit.rpc_clients'])) {
                return $clients;
            }

            foreach ($pimple['rabbit.rpc_clients'] as $name => $options) {
                $connection = $this->getConnection($pimple, $options, $pimple['rabbit.connections']);
                $client = new RpcClient($connection);

                if (array_key_exists('expect_serialized_response', $options)) {
                    $client->initClient($options['expect_serialized_response']);
                }

                $clients[$name] = $client;
            }

            return $clients;
        };
    }

    /**
     * @param Container $pimple
     */
    private function loadRpcServers(Container $pimple)
    {
        $pimple['rabbit.rpc_server'] = function (Container $pimple) {
            $servers = [];
            if (!isset($pimple['rabbit.rpc_servers'])) {
                return $servers;
            }

            foreach ($pimple['rabbit.rpc_servers'] as $name => $options) {
                $connection = $this->getConnection($pimple, $options, $pimple['rabbit.connections']);
                $server = new RpcServer($connection);
                $server->initServer($name);
                $server->setCallback(array($options['callback'], 'execute'));

                if (array_key_exists('qos_options', $options)) {
                    $server->setQosOptions(
                        $options['qos_options']['prefetch_size'],
                        $options['qos_options']['prefetch_count'],
                        $options['qos_options']['global']
                    );
                }

                $servers[$name] = $server;
            }

            return $servers;
        };
    }
}
