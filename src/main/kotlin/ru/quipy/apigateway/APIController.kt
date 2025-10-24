package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = 3,
        window = Duration.ofMillis(1000),
        bucketSize = 100
    )

    private val paymentQueue = LinkedBlockingQueue<PaymentRequest>(100)
    private val paymentExecutor = Executors.newFixedThreadPool(3)

    init {
        startPaymentProcessor()
    }

    private fun startPaymentProcessor() {
        repeat(3) {
            paymentExecutor.submit {
                while (true) {
                    try {
                        val request = paymentQueue.poll(100, TimeUnit.MILLISECONDS)
                        if (request != null) {
                            Thread.sleep(333)
                            processPaymentInternal(request)
                        }
                    } catch (e: Exception) {
                        logger.error("Error in payment processor", e)
                    }
                }
            }
        }
    }

    private fun processPaymentInternal(request: PaymentRequest) {
        try {
            val createdAt = orderPayer.processPayment(
                request.orderId,
                request.orderPrice,
                request.paymentId,
                request.deadline
            )
            request.future.complete(PaymentSubmissionDto(createdAt, request.paymentId))
        } catch (e: Exception) {
            request.future.completeExceptionally(e)
        }
    }

    data class PaymentRequest(
        val orderId: UUID,
        val orderPrice: Int,
        val paymentId: UUID,
        val deadline: Long,
        val future: CompletableFuture<PaymentSubmissionDto>
    )

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
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<*> {
        if (!rateLimiter.tick()) {
            logger.debug("Rate limit exceeded, queue full")
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "1")
                .body(mapOf("error" to "Rate limit exceeded, please retry later"))
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(mapOf("error" to "No such order $orderId"))

        val future = CompletableFuture<PaymentSubmissionDto>()
        val request = PaymentRequest(
            orderId = orderId,
            orderPrice = order.price,
            paymentId = paymentId,
            deadline = deadline,
            future = future
        )

        if (!paymentQueue.offer(request, 500, TimeUnit.MILLISECONDS)) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "1")
                .body(mapOf("error" to "Payment queue full"))
        }

        return try {
            val result = future.get(30, TimeUnit.SECONDS)
            ResponseEntity.ok(result)
        } catch (e: Exception) {
            logger.error("Payment processing failed", e)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(mapOf("error" to "Payment processing failed"))
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
