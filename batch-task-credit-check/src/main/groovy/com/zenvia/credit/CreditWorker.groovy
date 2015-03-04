package com.zenvia.credit

import org.vertx.groovy.core.eventbus.Message
import org.vertx.groovy.platform.Verticle

class CreditWorker extends Verticle {
    private static final AMQP_BRIDGE_ADDR = 'credit.check.mq'
    private static final TASKS_QUEUE_NAME = 'tasks.queue'
    private static final BILL_QUEUE_NAME = 'to.bill.queue'

    def start() {
        def mqConfig = [
                uri               : "amqp://localhost:5672",
                address           : AMQP_BRIDGE_ADDR,
                defaultContentType: "application/json"
        ]

        container.with {
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
        def handlerId = 'tasks.handler'

        def conf = [
                exchange   : "amq.direct",
                queueName  : TASKS_QUEUE_NAME,
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
        def eb = vertx.eventBus

        def destMessage = [
                exchange   : "amq.direct",
                routingKey : BILL_QUEUE_NAME,
                contentType: "application/json",
                body: message
        ]

        eb.send(AMQP_BRIDGE_ADDR + ".send", destMessage) { Message reply ->
            container.logger.info("Received reply: ${reply.body()}")
        }
    }
}