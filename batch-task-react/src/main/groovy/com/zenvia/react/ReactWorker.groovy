package com.zenvia.react

import io.vertx.rxcore.java.http.RxHttpClient
import io.vertx.rxcore.java.http.RxHttpServer
import org.vertx.groovy.core.http.RouteMatcher
import org.vertx.groovy.platform.Verticle

class ReactWorker extends Verticle {

    def start() {

        def rm = new RouteMatcher()
        rm.get('/details/:user/:id') { req ->
            req.response.end "User: ${req.params['user']} ID: ${req.params['id']}"
        }
        rm.getWithRegEx('.*') { req ->
            req.response.sendFile "route_match/index.html"
        }

        RxHttpServer server = new RxHttpServer(vertx.createHttpServer().requestHandler(rm.asClosure()).toJavaServer())
                
        server.http().subscribe({ req ->
            println("http-server:new-request:${req.path()}")
            req.response().end("pong:" + req.path())
        }, { e -> 
            println("http-server:req-error:" + e)
        })
        server.coreHttpServer().listen(8080, "localhost");
    }
}