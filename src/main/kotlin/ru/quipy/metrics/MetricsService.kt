package ru.quipy.metrics

import org.springframework.stereotype.Service
import ru.quipy.config.MetricsConfig
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.Tag
import java.util.concurrent.TimeUnit

@Service
class MetricsService(
    val metricsConfig: MetricsConfig,
) {
    fun <T> collectMetric(tags: List<String>, block: () -> T): T {
        writeCounter(metricsConfig.incomingRequests, tags).increment()
        val response = block()
        writeCounter(metricsConfig.outgoingResponses, tags).increment()
        return response
    }

    fun incrementCompletedTask(method: String) {
        writeCounter(metricsConfig.completedTasks, listOf(method)).increment()
    }

    fun increaseRetryCounter() {
        Counter
            .builder(metricsConfig.retryRequests.name)
            .description(metricsConfig.retryRequests.description)
            .register(Metrics.globalRegistry).increment()
    }


    fun recordRequestDuration(durationMs: Long, success: Boolean) {
        Timer.builder(metricsConfig.requestDuration.name)
            .description(metricsConfig.requestDuration.description)
            .tags("success", success.toString())
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }

    private fun writeCounter(config: MetricsConfig.MetricProperties, tags: List<String>): Counter {
        val counterTags: List<Tag> =
            config.tags.mapIndexed { index, element ->
                Tag.of(element, tags[index])
            }
        return Counter
            .builder(config.name)
            .description(config.description)
            .tags(counterTags)
            .register(Metrics.globalRegistry)
    }
}