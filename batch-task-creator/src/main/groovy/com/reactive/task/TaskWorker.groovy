package com.reactive.task

import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.math.RandomUtils
import org.vertx.groovy.core.eventbus.Message
import org.vertx.groovy.platform.Verticle

class TaskWorker extends Verticle {
    private static final AMQP_BRIDGE_ADDR = 'tasks.mq'
    private static final INSTRUCTION_BRIDGE_ADDR = 'instruction.bridge'
    private static final TASKS_QUEUE_NAME = 'tasks.queue'
    private static final MONGO_BRIDGE_ADDR = 'taks.mongopersistor'
    
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
                    registerHandler()
                }
            })
        }
    }

    def registerHandler() {
        def eb = vertx.eventBus
        eb.registerHandler(INSTRUCTION_BRIDGE_ADDR, { message ->
            container.logger.info("Received message: ${message.dump()}")
            sendMessage(message.body())
            message.reply([status: "ok"]);
        })
        
    }
    
    def sendMessage(instruction) {
        container.logger.info("Processing instruction ${instruction}")
        def eb = vertx.eventBus
        
        def name = instruction.name
        def message = [
                exchange   : "amq.direct",
                routingKey : TASKS_QUEUE_NAME,
                contentType: "application/json",
                body       : [name: name, status: 'opened']
        ]

        def amount = instruction.amount.toInteger()
        amount.times {

            message.body.order = it

            def body = message.body

            eb.send(MONGO_BRIDGE_ADDR, [action: 'save', collection: 'tasks', document: body]) { event ->
                if ('ok'.equals(event.body().status)) {
                    container.logger.info("Persisted: ${event.body()}")

                    message.properties = [headers: [id: event.body()._id]]
                    
                    container.logger.info("Sending task: ${message}")
                    eb.send(AMQP_BRIDGE_ADDR + ".send", message) { Message reply ->
                        container.logger.info("Received reply: ${reply.body()}")
                    }
                } else {
                    container.logger.error("Do not persisted: ${event.dump()}")
                }
            }

        }
    }
}