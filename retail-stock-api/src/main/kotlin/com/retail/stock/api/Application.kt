package com.retail.stock.api

import com.retail.stock.api.config.AppConfig
import com.retail.stock.api.plugins.configureHttp
import com.retail.stock.api.routes.*
import com.retail.stock.api.security.AUTH_KEYCLOAK
import com.retail.stock.api.security.configureSecurity
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.ApplicationStopping
import io.ktor.server.auth.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.routing.*
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.retail.stock.api.Application")

fun main() {
    val config = AppConfig.load()
    log.info(
        "Starting Retail Stock API on {}:{} (db={}, kafka={}, issuer={})",
        config.http.host, config.http.port, config.database.url, config.kafka.bootstrapServers, config.security.issuer,
    )
    embeddedServer(Netty, port = config.http.port, host = config.http.host, module = { retailStockModule(config) })
        .start(wait = true)
}

fun Application.retailStockModule(config: AppConfig) {
    val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    val runtime = RetailStockRuntime(config, registry)

    // Child of the application job: background workers die with the application, but one failing worker
    // (supervisor) never takes the others or the HTTP server down.
    val backgroundJob = SupervisorJob(coroutineContext[Job])
    val backgroundScope =
        CoroutineScope(coroutineContext + backgroundJob + Dispatchers.Default + CoroutineName("retail-stock-background"))
    runtime.start(backgroundScope)

    monitor.subscribe(ApplicationStopping) {
        log.info("Shutting down background workers")
        runtime.beginShutdown()
        runBlocking { withTimeoutOrNull(15_000) { backgroundJob.cancelAndJoin() } }
    }
    monitor.subscribe(ApplicationStopped) {
        runtime.close()
        registry.close()
    }

    configureHttp(config.http, registry)
    configureSecurity(config.security)

    routing {
        systemRoutes(runtime.database, runtime.relay, registry)
        authenticate(AUTH_KEYCLOAK) {
            route("/api/v1") {
                meRoutes()
                stockRoutes(runtime.stockService)
                reservationRoutes(runtime.stockService)
                adminRoutes(runtime.deadLetterService)
            }
        }
    }
}
