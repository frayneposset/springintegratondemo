package com.example.springintegrationdemo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.StandardIntegrationFlow
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.jdbc.store.JdbcMessageStore
import org.springframework.integration.jdbc.store.channel.ChannelMessageStoreQueryProvider
import org.springframework.integration.jdbc.store.channel.MySqlChannelMessageStoreQueryProvider
import org.springframework.integration.store.MessageGroupStore
import org.springframework.messaging.Message
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import java.io.Serializable
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

data class Submission(val submissionId : String,val description: String, val delay : Long, val status : String) : Serializable
@SpringBootApplication
class SpringIntegrationDemoApplication {

    @MessagingGateway(defaultRequestChannel = "submissions")
    interface SubmissionGateway {
        fun poll(submission: Submission)
    }

    @Bean
    fun jdbcChannelMessageStore(dataSource: DataSource): JdbcMessageStore {
        return JdbcMessageStore(dataSource)
    }

    @Bean
    fun queryProvider(): ChannelMessageStoreQueryProvider = MySqlChannelMessageStoreQueryProvider()
}

fun main(args: Array<String>) {
    runApplication<SpringIntegrationDemoApplication>(*args)
}

@Configuration
class ChannelConfiguration {

    @Bean
    fun submissions(): DirectChannel = MessageChannels.direct().get()

    @Bean
    fun ready(): DirectChannel = MessageChannels.direct().get()

    @Bean
    fun notReady(): DirectChannel = MessageChannels.direct().get()

    @Bean
    fun poll(): DirectChannel = MessageChannels.direct().get()

    @Bean
    fun timeout(): DirectChannel = MessageChannels.direct().get()
}



@Configuration
class SubmissionConfiguration(val channels: ChannelConfiguration) {

    val notReady = "NOT_READY"
    val ready = "READY"
    private val pollCount = "pollCount"

    @Bean
    fun submissionFlow(): StandardIntegrationFlow = integrationFlow(channels.submissions()) {

        enrichHeaders {
            headerFunction<Any>(pollCount) {
                AtomicInteger()
            }
        }
        handle<Submission> { p, _ -> poll(p) }

        route<Message<Submission>> {
            when (it.payload.status) {
                ready -> channels.ready()
                else -> channels.notReady()
            }
        }
    }


    fun poll(input: Submission): Submission {
        val status = if ((0..5).random() == 0) ready else notReady
        println("in polling input is $input, result is $status")
        return input.copy(status=status)
    }


    @Bean
    fun readyFlow(): StandardIntegrationFlow = integrationFlow(channels.ready()) {
        handle {
            println("Handling ready message $it")
        }
    }

    @Bean
    fun notReadyFlow(messageStore: MessageGroupStore): StandardIntegrationFlow = integrationFlow(channels.notReady()) {

        delay("delayer.messageGroupId") {
            messageStore(messageStore)
            delayFunction<Submission> {
                it.headers[pollCount, AtomicInteger::class.java]?.getAndIncrement()
                println(it.headers[pollCount])
                it.payload.delay
            }
        }

        channel("poll")
    }

    @Bean
    fun pollOrTimeOut(): StandardIntegrationFlow = integrationFlow(channels.poll()) {

        route<Message<*>, String>({
            val count = it.headers[pollCount].toString().toInt()
            if (count > 10) {
                "timeout"
            } else {
                "submissions"
            }

        }
        ) {


        }
    }


    @Bean
    fun timeoutFlow(): StandardIntegrationFlow = integrationFlow(channels.timeout()) {
        handle {
            println("Handling timeout message $it")
        }
    }
}


@RestController
class SubmissionController(val submissionGateway: SpringIntegrationDemoApplication.SubmissionGateway) {

    @PostMapping("/")
    fun save(@RequestBody subMission: Submission) {
        return submissionGateway.poll(subMission)
    }
}



