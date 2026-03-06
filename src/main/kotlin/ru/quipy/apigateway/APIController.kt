package ru.quipy.apigateway

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.metrics.MetricsService
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.time.Duration
import java.util.*
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

@RestController
class APIController(
    private val metricsService: MetricsService,
    private val mongoTemplate: MongoTemplate 
) {
    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    private lateinit var rateLimiter: SlidingWindowRateLimiter

    @PostConstruct
    fun init() {
        val limit = orderPayer.getMaxRateLimit()
        this.rateLimiter = SlidingWindowRateLimiter((limit * 4L / 5) / 10, Duration.ofMillis(100))
    }

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    suspend fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<PaymentSubmissionDto> {
        if (!rateLimiter.tick()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", 1.toString())
                .build()
        }

        val query = Query.query(Criteria.where("_id").`is`(orderId).and("status").ne(OrderStatus.PAYMENT_IN_PROGRESS))
        val update = Update.update("status", OrderStatus.PAYMENT_IN_PROGRESS)
        val options = FindAndModifyOptions().returnNew(true)
        val order = mongoTemplate.findAndModify(query, update, options, Order::class.java)

        if (order == null) {
            throw IllegalArgumentException("No such order $orderId or payment already in progress")
        }

        val paymentId = UUID.randomUUID()
        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
