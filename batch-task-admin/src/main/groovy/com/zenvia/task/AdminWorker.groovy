package com.zenvia.task

import org.vertx.groovy.core.eventbus.Message
import org.vertx.groovy.core.http.impl.DefaultHttpServerRequest
import org.vertx.groovy.platform.Verticle

class AdminWorker extends Verticle {

    private static final INSTRUCTION_BRIDGE_ADDR = 'instruction.bridge'
    
    def start() {
        vertx.createHttpServer().requestHandler { DefaultHttpServerRequest req ->
            container.logger.info("Web request ${req.absoluteURI}")
            requestHandler(req)
            req.response.end()
        }.listen(8080)
    }
    
    def requestHandler(request) {
        def instruction = [:]
        instruction.name = request.params.name
        instruction.amount = request.params.amount

        def eb = vertx.eventBus
        container.logger.info("Sending instruction: ${instruction}")
        eb.send(INSTRUCTION_BRIDGE_ADDR, instruction) { Message reply ->
            container.logger.info("Received reply: ${reply.body()}")
        }

    }
}