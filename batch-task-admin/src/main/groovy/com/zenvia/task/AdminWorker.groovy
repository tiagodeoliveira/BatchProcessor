package com.zenvia.task

import org.vertx.groovy.core.eventbus.Message
import org.vertx.groovy.core.http.impl.DefaultHttpServerRequest
import org.vertx.groovy.platform.Verticle

class AdminWorker extends Verticle {

    private static final INSTRUCTION_BRIDGE_ADDR = 'instruction.bridge'
    private static final CREATOR_PATH = '/instruction/create'
    
    def start() {
        vertx.createHttpServer().requestHandler { DefaultHttpServerRequest req ->
            container.logger.info("Web request ${req.path}")
            
            if (CREATOR_PATH.equals(req.path)) {
                requestHandler(req)
            }
            
            req.response.end()
        }.listen(8080)
    }
    
    def requestHandler(request) {
        container.logger.info("Processing params: ${request.params.entries}")

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