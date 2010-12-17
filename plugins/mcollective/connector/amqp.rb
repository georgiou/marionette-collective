require 'qpid'
require 'socket'

module MCollective
    module Connector
        class Amqp<Base

            def initialize
                @config = Config.instance
                @subscriptions = []

                @log = Log.instance
                @exchange = "mcollective.topic"
            end

            # Connects to the AMQP middleware
            def connect
                if @connection
                    @log.debug("Already connection, not re-initializing connection")
                    return
                end

                @rqueue_name="mc-"+Qpid::UUID::uuid4.to_s

                begin
                    @connection=Qpid::Connection.new(TCPSocket.new("localhost", "5672"), :mechanism=>"plain", :username=>"guest", :password=>"guest")
                    @connection.start(30)
                    @session = @connection.session( Qpid::UUID::uuid4.to_s )
                    @session.exchange_declare(@exchange, :type => "topic")

                    @session.queue_declare(:queue=>@rqueue_name, :exclusive=>true, :auto_delete=>true )
                    @queue=@session.incoming("local")
                    @session.message_subscribe(:queue=>@rqueue_name,:destination=>"local")
                    @queue.start()
                rescue Exception => e
                    raise("Could not connect to Stomp Server: #{e}")
                end
            end

            # Receives a message from the AMQP connection
            def receive
                @log.debug("Waiting for AMQP message")
                msg = @queue.get()
                Request.new(msg.body())
            end

            # Sends a message to the AMQP connection
            def send(target, msg)
                @log.debug("Sending a message to AMQP target '#{target}'")
                dp = @session.delivery_properties(:routing_key => target.sub(/\/topic\//, ""))
                mp = @session.message_properties(:content_type => "application/octet-stream")
                @session.message_transfer(:destination=>@exchange, :message => Qpid::Message.new(dp, mp, msg))
            end

            # Subscribe to a topic or queue
            def subscribe(source)
                unless @subscriptions.include?(source)
                    @log.debug("Subscribing to #{source}")
                    topic=source.sub(/\/topic\//, "")
                    @session.exchange_bind(:exchange=>@exchange, :queue=>@rqueue_name, :binding_key=>topic)
                    @subscriptions << source
                end
            end

            # Subscribe to a topic or queue
            def unsubscribe(source)
                @log.debug("Unsubscribing from #{source}")
                topic=source.sub(/\/topic\//, "")
                @session.exchange_unbind(:exchange=>@exchange, :queue=>@rqueue_name, :binding_key=>topic)
                @subscriptions.delete(source)
            end

            # Disconnects from the AMQP connection
            def disconnect
                @log.debug("Disconnecting from AMQP")
                @session.close
                @connection.close
            end
        end
    end
end

# vi:tabstop=4:expandtab:ai
