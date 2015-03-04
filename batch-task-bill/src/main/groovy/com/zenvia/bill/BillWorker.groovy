package com.zenvia.bill

import org.vertx.groovy.core.eventbus.Message
import org.vertx.groovy.platform.Verticle

public class BillWorker extends Verticle {

    private static final AMQP_BRIDGE_ADDR = 'to.bill.mq'
    private static final MONGO_BRIDGE_ADDR = 'to.bill.mongopersistor'

    def start() {

        def mqConfig = [
                uri               : "amqp://localhost:5672",
                address           : AMQP_BRIDGE_ADDR,
                defaultContentType: "application/json"
        ]

        def mongoConfig = [
                address : MONGO_BRIDGE_ADDR,
                database: "reactive-test"
        ]

        container.with {
            deployModule("io.vertx~mod-mongo-persistor~2.0.0-final", mongoConfig)
            
            deployWorkerVerticle("me.streamis.vertx.rabbitmq.RabbitMQMod", mqConfig, { event ->
                if (!event.succeeded()) {
                    container.logger.error("Deploy error: ${event.cause()}")
                } else {
                    container.logger.info("Deploy succeed: ${event.result()}")
                    
                    this.registerHandler()
                }
            })
        }
    }

    def registerHandler() {
        container.logger.info('Registering handler!')

        def eb = vertx.eventBus
        def handlerId = 'to.bill.handler'
        def queueName = 'to.bill.queue'

        def conf = [
                exchange   : "amq.direct",
                queueName  : queueName,
                routingKey : "*.*",
                forward    : handlerId,
                contentType: "application/json"
        ]

        eb.registerHandler(handlerId, { message ->
            container.logger.info("Received message: ${message.dump()}")
            processMessage(message.body())
            message.reply([status: "ok"]);
        })

        eb.send(AMQP_BRIDGE_ADDR + ".create-consumer", conf) { Message event ->
            container.logger.info('Consumer created: ' + event.body())
        }
    }

    def processMessage(message) {
        container.logger.info("Processing message: ${message}")
        def body = message.body.body
        def eb = vertx.eventBus

        eb.send(MONGO_BRIDGE_ADDR, [action: 'save', collection: 'closed', document: body]) { event ->
            if ('ok'.equals(event.body().status)) {
                container.logger.info("Persisted: ${body}")
            } else {
                container.logger.error("Do not persisted: ${event.dump()}")
            }
        }
    }
}
