# RabbitMQ Advanced Scenarios & Real-World Use Cases

## Complete Real-World Implementation Guide

---

## TABLE OF CONTENTS

1. E-Commerce Order Processing System
2. Social Media Notification System
3. Log Aggregation & Analytics Platform
4. Microservices Communication Architecture
5. IoT Sensor Data Processing
6. Banking Transaction System
7. Video Processing Pipeline
8. Email Marketing Automation

---

## SCENARIO 1: E-Commerce Order Processing System

### Business Context:
When a customer places an order, multiple systems need to be notified:
- Payment Service: Process payment
- Inventory Service: Reserve stock
- Shipping Service: Prepare shipment
- Notification Service: Send email confirmation
- Analytics Service: Track order metrics

### Exchange Type & Pattern:
**Exchange:** Topic Exchange  
**Pattern:** One Publisher → Multiple Subscribers (1-to-Many)  
**Reason:** Different services need different types of order events

### Architecture:

```
Customer places order
         ↓
Order Service (Publisher)
         ↓
    Topic Exchange: "orders_events"
         ↓
         ├─→ Queue: "payment_queue" (routing: "order.#")
         │        ↓
         │   Payment Service Consumer
         │   (Processes payment)
         │
         ├─→ Queue: "inventory_queue" (routing: "order.created")
         │        ↓
         │   Inventory Service Consumer
         │   (Reserves stock)
         │
         ├─→ Queue: "shipping_queue" (routing: "order.paid")
         │        ↓
         │   Shipping Service Consumer
         │   (Prepares shipment)
         │
         ├─→ Queue: "notification_queue" (routing: "order.#")
         │        ↓
         │   Notification Service Consumer
         │   (Sends emails)
         │
         └─→ Queue: "analytics_queue" (routing: "#")
                  ↓
              Analytics Service Consumer
              (Logs all events)
```

### Event Flow:

```
1. order.created
   └─→ payment_queue ✓
   └─→ inventory_queue ✓
   └─→ notification_queue ✓
   └─→ analytics_queue ✓

2. order.paid
   └─→ payment_queue ✓
   └─→ shipping_queue ✓
   └─→ notification_queue ✓
   └─→ analytics_queue ✓

3. order.shipped
   └─→ payment_queue ✓
   └─→ notification_queue ✓
   └─→ analytics_queue ✓

4. order.delivered
   └─→ notification_queue ✓
   └─→ analytics_queue ✓
```

### Publisher Code:

```javascript
const amqp = require('amqplib');

async function ecommercePublisher() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'orders_events';
  await channel.assertExchange(exchange, 'topic', { durable: true });

  // Simulate order events
  const events = [
    { routingKey: 'order.created', data: { orderId: 'O1001', customerId: 'C1', amount: 99.99 } },
    { routingKey: 'order.paid', data: { orderId: 'O1001', paymentId: 'P1001', amount: 99.99 } },
    { routingKey: 'order.shipped', data: { orderId: 'O1001', trackingId: 'TRK001' } },
    { routingKey: 'order.delivered', data: { orderId: 'O1001', deliveryDate: new Date() } }
  ];

  for (const event of events) {
    await channel.publish(
      exchange,
      event.routingKey,
      Buffer.from(JSON.stringify({
        timestamp: new Date(),
        ...event.data
      }))
    );
    console.log(`Published: ${event.routingKey}`);
    await new Promise(r => setTimeout(r, 1000));
  }

  await channel.close();
  await connection.close();
}

ecommercePublisher();
```

### Consumer Examples:

**Payment Service Consumer:**
```javascript
async function paymentConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'orders_events';
  const queue = 'payment_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, 'order.#');

  await channel.consume(queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    
    console.log('[Payment Service] Processing:', event);
    
    if (event.routingKey === 'order.created') {
      console.log('✓ Charging customer $' + event.amount);
    } else if (event.routingKey === 'order.paid') {
      console.log('✓ Payment confirmed');
    }
    
    channel.ack(msg);
  });
}
```

**Inventory Service Consumer:**
```javascript
async function inventoryConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'orders_events';
  const queue = 'inventory_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, 'order.created');

  await channel.consume(queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    
    console.log('[Inventory Service] Reserving stock for order:', event.orderId);
    console.log('✓ Stock reserved');
    
    channel.ack(msg);
  });
}
```

### Key Learnings:

1. **Topic Exchange** allows selective routing
2. **Pattern Binding Keys**:
   - `order.#` = receives all order events
   - `order.created` = only new order events
   - `#` = receives everything
3. **Independent Processing**: Each service works at its own pace
4. **Failure Isolation**: One service crash doesn't affect others
5. **Event Sourcing**: All events are logged for auditing

---

## SCENARIO 2: Social Media Notification System

### Business Context:
Users expect notifications on multiple channels:
- Push Notifications (mobile)
- Email (inbox)
- SMS (urgent)
- In-App Alerts
- Slack (business users)

### Exchange Type & Pattern:
**Exchange:** Fanout Exchange  
**Pattern:** One Publisher → Multiple Subscribers (Broadcast)  
**Reason:** All notification channels need to receive the same event

### Architecture:

```
Event occurs
(user followed, post liked, comment received)
         ↓
Event Service (Publisher)
         ↓
    Fanout Exchange: "notifications"
         ↓
         ├─→ Push Notification Queue
         │        ↓
         │   Push Service → Apple/Google APIs
         │
         ├─→ Email Queue
         │        ↓
         │   Email Service → SMTP Server
         │
         ├─→ SMS Queue
         │        ↓
         │   SMS Service → Twilio/AWS SNS
         │
         ├─→ In-App Queue
         │        ↓
         │   In-App Service → WebSocket
         │
         └─→ Slack Queue
                  ↓
              Slack Service → Slack API
```

### Event Messages:

```javascript
// User Follow Event
{
  "eventType": "user.followed",
  "userId": "U123",
  "followerId": "U456",
  "timestamp": "2024-04-20T10:30:00Z",
  "userPreferences": {
    "notifications": ["push", "email", "slack"]
  }
}

// Post Like Event
{
  "eventType": "post.liked",
  "postId": "P789",
  "authorId": "U123",
  "likedBy": "U456",
  "timestamp": "2024-04-20T10:32:00Z"
}

// Comment Event
{
  "eventType": "post.commented",
  "postId": "P789",
  "authorId": "U123",
  "commenter": "U456",
  "comment": "Great post!",
  "timestamp": "2024-04-20T10:35:00Z"
}
```

### Publisher Code:

```javascript
async function notificationPublisher() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'notifications_broadcast';
  await channel.assertExchange(exchange, 'fanout', { durable: true });

  // Broadcast to ALL subscribers
  const events = [
    {
      eventType: 'user.followed',
      userId: 'user123',
      message: 'You have a new follower!'
    },
    {
      eventType: 'post.liked',
      postId: 'post456',
      message: 'Someone liked your post!'
    }
  ];

  for (const event of events) {
    await channel.publish(
      exchange,
      '', // Empty routing key (fanout ignores it)
      Buffer.from(JSON.stringify(event))
    );
    console.log('Broadcasting:', event.eventType);
    await new Promise(r => setTimeout(r, 1000));
  }

  await channel.close();
  await connection.close();
}
```

### Consumer Examples:

**Push Notification Consumer:**
```javascript
async function pushNotificationConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'notifications_broadcast';
  const queue = 'push_notifications_queue';
  
  await channel.assertExchange(exchange, 'fanout', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, ''); // Empty for fanout

  await channel.consume(queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    
    console.log('[Push Service] Sending push notification');
    console.log('→ Calling Apple APNs / Google FCM');
    console.log('→ Message:', event.message);
    
    channel.ack(msg);
  });
}
```

**Email Notification Consumer:**
```javascript
async function emailNotificationConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'notifications_broadcast';
  const queue = 'email_notifications_queue';
  
  await channel.assertExchange(exchange, 'fanout', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, '');

  await channel.consume(queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    
    console.log('[Email Service] Sending email');
    console.log('→ Using SMTP server');
    console.log('→ Subject:', event.message);
    
    channel.ack(msg);
  });
}
```

### Key Learnings:

1. **Fanout Exchange** broadcasts to everyone
2. **All consumers receive same message**
3. **Independent processing** at different speeds
4. **Routing key ignored** in fanout
5. **Scalability** - add new notification channels without changing publisher

---

## SCENARIO 3: Log Aggregation & Analytics Platform

### Business Context:
Multiple services generate logs that need to be:
- Collected centrally
- Indexed for searching
- Analyzed for insights
- Stored for auditing
- Alerted on errors

### Exchange Type & Pattern:
**Exchange:** Topic Exchange  
**Pattern:** Multiple Publishers → One Subscriber (Many-to-1)  
**Reason:** Multiple services publish different log types, one logger collects all

### Log Hierarchy:

```
logs.*
├─ logs.error.*
│  ├─ logs.error.auth       → Authentication errors
│  ├─ logs.error.database   → Database errors
│  └─ logs.error.api        → API errors
├─ logs.warning.*
│  ├─ logs.warning.performance
│  └─ logs.warning.resource
├─ logs.info.*
│  ├─ logs.info.request
│  └─ logs.info.response
└─ logs.debug.*
   └─ logs.debug.trace
```

### Architecture:

```
Admin Service         Faculty Service        Student Service
     │                      │                       │
     ├─ logs.error.api      ├─ logs.error.database ├─ logs.warning.resource
     ├─ logs.info.request   ├─ logs.info.request   └─ logs.info.request
     └─ logs.debug.trace    └─ logs.warning.perf
                   │              │                   │
                   └──────────┬───┴───────────────────┘
                              │
                         Topic Exchange
                         "application_logs"
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   Error Logger          Analytics Engine     Alert System
   (logs.error.*)        (logs.#)            (logs.error.*)
   ↓                     ↓                   ↓
Store in DB          Calculate metrics    Send alerts
```

### Publishers Code (Multiple Services):

```javascript
// Admin Service
async function adminServiceLogger() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'application_logs';
  await channel.assertExchange(exchange, 'topic', { durable: true });

  const logs = [
    { key: 'logs.error.api', message: 'Failed to authenticate user' },
    { key: 'logs.info.request', message: 'Admin accessed dashboard' },
    { key: 'logs.debug.trace', message: 'Query executed in 45ms' }
  ];

  for (const log of logs) {
    await channel.publish(
      exchange,
      log.key,
      Buffer.from(JSON.stringify({
        service: 'admin-service',
        timestamp: new Date(),
        level: log.key.split('.')[1],
        message: log.message
      }))
    );
    console.log(`[Admin] Published: ${log.key}`);
  }

  await channel.close();
  await connection.close();
}

// Faculty Service
async function facultyServiceLogger() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'application_logs';
  await channel.assertExchange(exchange, 'topic', { durable: true });

  const logs = [
    { key: 'logs.error.database', message: 'Database connection timeout' },
    { key: 'logs.warning.performance', message: 'Query taking 5 seconds' }
  ];

  for (const log of logs) {
    await channel.publish(
      exchange,
      log.key,
      Buffer.from(JSON.stringify({
        service: 'faculty-service',
        timestamp: new Date(),
        level: log.key.split('.')[1],
        message: log.message
      }))
    );
    console.log(`[Faculty] Published: ${log.key}`);
  }

  await channel.close();
  await connection.close();
}
```

### Consumer Examples:

**Error Logger (Only errors):**
```javascript
async function errorLogger() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'application_logs';
  const queue = 'error_logs_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  
  // Only bind to error logs
  await channel.bindQueue(queue, exchange, 'logs.error.*');

  await channel.consume(queue, async (msg) => {
    const log = JSON.parse(msg.content.toString());
    
    console.log('[Error Logger] Received error from:', log.service);
    console.log('→ Error:', log.message);
    console.log('→ Time:', log.timestamp);
    console.log('→ Storing in database...');
    
    channel.ack(msg);
  });
}
```

**Analytics Engine (All logs):**
```javascript
async function analyticsEngine() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'application_logs';
  const queue = 'analytics_logs_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  
  // Bind to ALL logs
  await channel.bindQueue(queue, exchange, 'logs.#');

  let logCount = 0;

  await channel.consume(queue, async (msg) => {
    const log = JSON.parse(msg.content.toString());
    
    logCount++;
    console.log(`[Analytics] Received log #${logCount}`);
    console.log('→ Service:', log.service);
    console.log('→ Level:', log.level);
    console.log('→ Calculating metrics...');
    
    channel.ack(msg);
  });
}
```

**Alert System (Critical only):**
```javascript
async function alertSystem() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'application_logs';
  const queue = 'alerts_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  
  // Only critical errors
  await channel.bindQueue(queue, exchange, 'logs.error.*');

  await channel.consume(queue, async (msg) => {
    const log = JSON.parse(msg.content.toString());
    
    console.log('[🚨 ALERT SYSTEM] CRITICAL ERROR DETECTED!');
    console.log('Service:', log.service);
    console.log('Error:', log.message);
    console.log('→ Sending email alert...');
    console.log('→ Creating Slack notification...');
    console.log('→ Paging on-call engineer...');
    
    channel.ack(msg);
  });
}
```

### Key Learnings:

1. **Topic Exchange** enables selective routing with patterns
2. **Pattern Binding Keys**:
   - `logs.error.*` = only errors
   - `logs.*` = specific level
   - `logs.#` = everything
3. **Multiple subscribers** with different interests
4. **Wildcard patterns**: * (one word), # (multiple words)
5. **Scalable architecture**: Add new services without changing others

---

## SCENARIO 4: Microservices Communication Architecture

### Business Context:
In a microservices architecture, services need to:
- Communicate asynchronously
- Maintain loose coupling
- Handle failures gracefully
- Scale independently
- Share events

### Exchange Type & Pattern:
**Exchange:** Topic Exchange  
**Pattern:** Both 1-to-Many AND Many-to-1  
**Reason:** Complex system with multiple event types and processors

### Complete System:

```
User Service          Product Service       Order Service        Payment Service
    │                      │                     │                     │
    ├─ user.created        ├─ product.updated    ├─ order.created     ├─ payment.processing
    ├─ user.updated        └─ product.deleted    ├─ order.shipped     └─ payment.completed
    └─ user.deleted                              └─ order.cancelled
              │                  │                       │                   │
              └──────────────────┼───────────────────────┴───────────────────┘
                                 │
                            Topic Exchange
                         "microservices_events"
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
   Order Service            Notification Service    Analytics Service
   (user.# + product.#)     (order.# + payment.#)   (*.*)
   ↓                        ↓                        ↓
Process orders         Send notifications      Track metrics
Update inventory       Send emails             Build dashboards
```

### Event Types:

```
user.*
├─ user.created          → New user registered
├─ user.updated          → User profile changed
└─ user.deleted          → User account closed

product.*
├─ product.created       → New product added
├─ product.updated       → Product details changed
└─ product.deleted       → Product discontinued

order.*
├─ order.created         → New order placed
├─ order.paid            → Payment received
├─ order.processing      → Being prepared
├─ order.shipped         → Left warehouse
└─ order.delivered       → Reached customer

payment.*
├─ payment.initiated     → Payment started
├─ payment.processing    → Being processed
├─ payment.completed     → Successfully paid
├─ payment.failed        → Payment unsuccessful
└─ payment.refunded      → Money returned
```

### Implementation:

```javascript
// User Service Publisher
async function userServicePublisher() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'microservices_events';
  await channel.assertExchange(exchange, 'topic', { durable: true });

  const events = [
    {
      routing: 'user.created',
      data: { userId: 'U001', email: 'user@example.com', name: 'John' }
    },
    {
      routing: 'user.updated',
      data: { userId: 'U001', name: 'John Doe' }
    }
  ];

  for (const event of events) {
    await channel.publish(
      exchange,
      event.routing,
      Buffer.from(JSON.stringify({
        service: 'user-service',
        timestamp: new Date(),
        ...event.data
      }))
    );
    console.log(`Published: ${event.routing}`);
  }

  await channel.close();
  await connection.close();
}

// Order Service Consumer (listens to multiple event types)
async function orderServiceConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'microservices_events';
  const queue = 'order_service_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  
  // Listen to user and product changes
  await channel.bindQueue(queue, exchange, 'user.#');
  await channel.bindQueue(queue, exchange, 'product.#');
  await channel.bindQueue(queue, exchange, 'payment.completed');

  await channel.consume(queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    
    console.log('[Order Service] Received event:', event);
    
    // Handle based on event type
    if (event.routingKey.startsWith('user.')) {
      console.log('→ Updating user information in order system');
    } else if (event.routingKey.startsWith('product.')) {
      console.log('→ Updating product catalog');
    } else if (event.routingKey === 'payment.completed') {
      console.log('→ Shipping order');
    }
    
    channel.ack(msg);
  });
}

// Notification Service (listens to all critical events)
async function notificationServiceConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'microservices_events';
  const queue = 'notification_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  
  // Listen to order and payment events
  await channel.bindQueue(queue, exchange, 'order.#');
  await channel.bindQueue(queue, exchange, 'payment.completed');
  await channel.bindQueue(queue, exchange, 'user.created');

  await channel.consume(queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    
    console.log('[Notification Service] Sending notification');
    
    if (event.routingKey === 'order.created') {
      console.log('→ Email: Order confirmation');
    } else if (event.routingKey === 'order.shipped') {
      console.log('→ SMS: Shipment tracking');
    } else if (event.routingKey === 'payment.completed') {
      console.log('→ Push: Payment received');
    }
    
    channel.ack(msg);
  });
}

// Analytics Service (listens to everything)
async function analyticsServiceConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'microservices_events';
  const queue = 'analytics_queue';
  
  await channel.assertExchange(exchange, 'topic', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  
  // Listen to ALL events
  await channel.bindQueue(queue, exchange, '#');

  let eventCount = 0;

  await channel.consume(queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    
    eventCount++;
    console.log(`[Analytics] Event #${eventCount}: ${event.routingKey}`);
    console.log('→ Logging to data warehouse');
    console.log('→ Updating dashboards');
    
    channel.ack(msg);
  });
}
```

---

## SCENARIO 5: Video Processing Pipeline

### Business Context:
Users upload videos, which need to be:
1. Validated
2. Transcoded (multiple formats)
3. Thumbnails extracted
4. Metadata indexed
5. Notifications sent

### Exchange Type & Pattern:
**Exchange:** Direct Exchange  
**Pattern:** Task Queue (One Publisher → Multiple Workers)  
**Reason:** Tasks need to go to specific worker types

### Architecture:

```
User uploads video
         ↓
Upload Service (Publisher)
         ↓
    Direct Exchange: "video_tasks"
         ↓
         ├─ Validation Task Queue
         │     (routing: "video.validate")
         │     ├─ Worker 1
         │     ├─ Worker 2
         │     └─ Worker 3
         │
         ├─ Transcoding Task Queue
         │     (routing: "video.transcode")
         │     ├─ Worker 1
         │     ├─ Worker 2
         │     └─ Worker 3
         │
         ├─ Thumbnail Task Queue
         │     (routing: "video.thumbnail")
         │     ├─ Worker 1
         │     └─ Worker 2
         │
         └─ Metadata Task Queue
               (routing: "video.metadata")
               └─ Worker 1
```

### Publisher Code:

```javascript
async function videoUploadService() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'video_tasks';
  await channel.assertExchange(exchange, 'direct', { durable: true });

  // Simulate video upload
  const videoTasks = [
    {
      routing: 'video.validate',
      video: { id: 'V001', filename: 'tutorial.mp4', size: 500000000 }
    },
    {
      routing: 'video.transcode',
      video: { id: 'V001', formats: ['720p', '480p', '1080p'] }
    },
    {
      routing: 'video.thumbnail',
      video: { id: 'V001', timestamps: [0, 30, 60] }
    },
    {
      routing: 'video.metadata',
      video: { id: 'V001' }
    }
  ];

  for (const task of videoTasks) {
    await channel.publish(
      exchange,
      task.routing,
      Buffer.from(JSON.stringify({
        taskId: Math.random(),
        timestamp: new Date(),
        ...task.video
      }))
    );
    console.log(`Published task: ${task.routing}`);
    await new Promise(r => setTimeout(r, 500));
  }

  await channel.close();
  await connection.close();
}
```

### Worker Pool Example:

```javascript
async function transcodeWorker(workerId) {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();

  const exchange = 'video_tasks';
  const queue = `transcode_queue_${workerId}`;
  
  await channel.assertExchange(exchange, 'direct', { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, 'video.transcode');
  
  // Fair distribution - one task at a time
  await channel.prefetch(1);

  console.log(`Transcoding Worker #${workerId} started`);

  await channel.consume(queue, async (msg) => {
    const task = JSON.parse(msg.content.toString());
    
    console.log(`Worker #${workerId} - Transcoding video ${task.id}`);
    
    // Simulate transcoding work
    const processingTime = Math.random() * 5 + 2; // 2-7 seconds
    await new Promise(r => setTimeout(r, processingTime * 1000));
    
    console.log(`Worker #${workerId} - ✓ Completed in ${processingTime.toFixed(2)}s`);
    
    channel.ack(msg);
  });
}

// Start multiple workers
async function startWorkerPool() {
  for (let i = 1; i <= 3; i++) {
    transcodeWorker(i);
  }
}

startWorkerPool();
```

### Key Learnings:

1. **Direct Exchange** for task-specific routing
2. **Worker Pool Pattern**: Multiple workers consuming from same queue
3. **Fair Dispatch**: prefetch(1) ensures load balancing
4. **Task Queue Pattern**: Work items processed in order
5. **Scalability**: Add workers without changing publisher

---

## Best Practices Summary

### When to Use Each Exchange:

| Scenario | Exchange | Binding Key | Pattern |
|----------|----------|-------------|---------|
| Task distribution | Direct | exact match | P→Many workers |
| System announcements | Fanout | empty '' | P→Many subscribers |
| Event filtering | Topic | pattern with * # | P→Selective subscribers |
| Complex routing | Headers | key=value | P→Complex rules |

### Design Patterns:

1. **Task Queue**: Direct exchange, work distribution, worker pool
2. **Event Publishing**: Topic/Fanout exchange, loose coupling, independent processing
3. **Log Aggregation**: Topic exchange, many publishers, centralized consumer
4. **Notification Hub**: Fanout/Topic exchange, multi-channel delivery
5. **Microservices**: Topic exchange, bidirectional communication

### Error Handling:

```javascript
// Retry mechanism
if (processingFailed) {
  channel.nack(msg, false, true); // Requeue for retry
} else if (unrecoverableError) {
  // Send to dead letter queue
  channel.nack(msg, false, false); // Don't requeue
} else {
  channel.ack(msg); // Success
}
```

---

## Real-World Configuration Checklist

Before deploying to production:

- [ ] All queues set to `durable: true`
- [ ] All messages published with `persistent: true`
- [ ] Manual acknowledgment enabled
- [ ] Dead letter exchanges configured
- [ ] Retry mechanisms implemented
- [ ] Monitoring and alerting setup
- [ ] Consumer error handling in place
- [ ] Message timeout configured
- [ ] Prefetch count optimized
- [ ] Exchange and queue auto-recovery tested

---

This completes the advanced scenarios guide. Apply these patterns to your specific use cases!

