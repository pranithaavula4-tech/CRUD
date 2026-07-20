# RabbitMQ Quick Reference Cheat Sheet

## One-Page Reference for Common Tasks

---

## CONNECTION & SETUP

```javascript
const amqp = require('amqplib');

// Connect
const conn = await amqp.connect('amqp://guest:guest@localhost');
const ch = await conn.createChannel();

// Close
await ch.close();
await conn.close();
```

---

## EXCHANGES (4 Types)

### 1. Direct Exchange
```javascript
// Setup
await ch.assertExchange('ex_name', 'direct', { durable: true });

// Publish (routing key must match binding key exactly)
ch.publish('ex_name', 'exact_key', Buffer.from('message'));

// Consumer binding (must match routing key)
await ch.bindQueue('queue_name', 'ex_name', 'exact_key');
```

### 2. Fanout Exchange
```javascript
// Setup
await ch.assertExchange('ex_name', 'fanout', { durable: true });

// Publish (routing key ignored)
ch.publish('ex_name', '', Buffer.from('message'));

// Consumer binding (routing key ignored)
await ch.bindQueue('queue_name', 'ex_name', '');
```

### 3. Topic Exchange
```javascript
// Setup
await ch.assertExchange('ex_name', 'topic', { durable: true });

// Publish (use dot notation)
ch.publish('ex_name', 'user.admin.created', Buffer.from('msg'));

// Consumer binding (use patterns)
await ch.bindQueue('q1', 'ex_name', 'user.admin.*');      // admin events
await ch.bindQueue('q2', 'ex_name', 'user.#');            // all user events
await ch.bindQueue('q3', 'ex_name', '#');                 // everything
```

### 4. Headers Exchange
```javascript
// Setup
await ch.assertExchange('ex_name', 'headers', { durable: true });

// Publish with headers
ch.publish('ex_name', '', Buffer.from('msg'), {
  headers: { type: 'report', priority: 'high' }
});

// Consumer binding (key=value matching)
await ch.bindQueue('queue', 'ex_name', '', {
  'x-match': 'all',        // or 'any'
  'type': 'report',
  'priority': 'high'
});
```

---

## QUEUES

### Create Queue
```javascript
const q = await ch.assertQueue('queue_name', {
  durable: true,           // Survives broker restart
  exclusive: false,        // Accessible to all connections
  autoDelete: false,       // Not deleted when empty
  arguments: {}
});
```

### Queue with Options
```javascript
await ch.assertQueue('q', {
  durable: true,
  'x-message-ttl': 60000,         // Auto-expire messages (ms)
  'x-max-length': 1000,           // Max messages
  'x-dead-letter-exchange': 'dlx' // Route to DLX
});
```

### Bind Queue to Exchange
```javascript
// Direct/Fanout
await ch.bindQueue('queue', 'exchange', 'binding_key');

// Topic (with patterns)
await ch.bindQueue('queue', 'exchange', 'user.*.action');
```

---

## PUBLISHING

### Simple Message
```javascript
ch.publish('exchange', 'routing_key', Buffer.from('message'));
```

### JSON Message
```javascript
const data = { id: 1, name: 'John' };
ch.publish('exchange', 'key', Buffer.from(JSON.stringify(data)));
```

### With Options
```javascript
ch.publish(
  'exchange',
  'routing_key',
  Buffer.from('message'),
  {
    persistent: true,        // Survives broker restart
    contentType: 'application/json',
    contentEncoding: 'utf-8',
    headers: { custom: 'value' }
  }
);
```

---

## CONSUMING

### Basic Consumer
```javascript
await ch.consume('queue_name', (msg) => {
  if (msg) {
    console.log(msg.content.toString());
    ch.ack(msg);  // Acknowledge
  }
});
```

### Consumer with Options
```javascript
await ch.consume('queue_name', (msg) => {
  if (msg) {
    const data = JSON.parse(msg.content.toString());
    
    try {
      // Process
      channel.ack(msg);           // Success
    } catch (error) {
      channel.nack(msg, false, true);  // Requeue
      // or
      channel.nack(msg, false, false); // Discard
    }
  }
}, {
  noAck: false,     // Manual acknowledgment
  noLocal: false,   // Can receive own messages
  exclusive: false, // Multiple consumers allowed
  consumerTag: 'consumer1'
});
```

### Fair Dispatch
```javascript
await ch.prefetch(1);  // One message per consumer at a time

// Without prefetch, rapid consumers get many messages
// With prefetch(1), work is distributed fairly
```

---

## ACKNOWLEDGMENT MODES

```javascript
// Manual Acknowledgment (Safest)
await ch.consume(queue, (msg) => {
  try {
    // Process message
    ch.ack(msg);                  // Message processed successfully
  } catch (error) {
    ch.nack(msg, false, true);    // Requeue for retry
  }
});

// Automatic Acknowledgment (Fastest, least safe)
await ch.consume(queue, (msg) => {
  // Process immediately
}, { noAck: true });  // Automatic ack
```

---

## ERROR HANDLING

### Connection Error
```javascript
connection.on('error', (err) => {
  console.error('Connection error:', err);
});
```

### Channel Error
```javascript
channel.on('error', (err) => {
  console.error('Channel error:', err);
});
```

### Reconnection
```javascript
async function connectWithRetry(retries = 5) {
  for (let i = 0; i < retries; i++) {
    try {
      return await amqp.connect('amqp://guest:guest@localhost');
    } catch (error) {
      console.log(`Connection attempt ${i + 1} failed, retrying...`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  throw new Error('Failed to connect after retries');
}
```

---

## DEAD LETTER QUEUE (DLQ)

### Setup DLQ
```javascript
// Create DLX and DLQ
await ch.assertExchange('dlx', 'direct', { durable: true });
await ch.assertQueue('dlq', { durable: true });
await ch.bindQueue('dlq', 'dlx', 'dlx_key');

// Regular queue with DLX
await ch.assertQueue('regular_queue', {
  durable: true,
  'x-dead-letter-exchange': 'dlx',      // Route failed messages here
  'x-dead-letter-routing-key': 'dlx_key'
});
```

### Process DLQ Messages
```javascript
await ch.consume('dlq', async (msg) => {
  const failedMsg = JSON.parse(msg.content.toString());
  console.log('Failed message:', failedMsg);
  
  // Inspect, log, or manually retry
  ch.ack(msg);
});
```

---

## COMMON PATTERNS

### Task Queue
```javascript
// Publisher
ch.publish('tasks_ex', 'task_type', Buffer.from(taskData));

// Multiple Workers
for (let i = 0; i < 3; i++) {
  const ch = await conn.createChannel();
  await ch.prefetch(1);
  
  await ch.consume('task_queue', async (msg) => {
    // Do work
    ch.ack(msg);
  });
}
```

### Publish-Subscribe (Fan Out)
```javascript
// Publisher sends once
ch.publish('events', '', Buffer.from(event));

// Multiple subscribers each get a copy
await ch.consume('events_q1', (msg) => { /* process */ });
await ch.consume('events_q2', (msg) => { /* process */ });
await ch.consume('events_q3', (msg) => { /* process */ });
```

### Event Filtering (Topic)
```javascript
// Different subscribers want different events
await ch.bindQueue('logs_q', 'logs', 'error.*');       // Errors only
await ch.bindQueue('stats_q', 'logs', '#');            // Everything
await ch.bindQueue('perf_q', 'logs', '*.performance'); // Performance
```

---

## DIAGNOSTICS

### Check Connection
```javascript
try {
  const conn = await amqp.connect('amqp://guest:guest@localhost');
  console.log('✓ Connected');
  await conn.close();
} catch (err) {
  console.error('✗ Cannot connect:', err.message);
}
```

### Check Queue
```javascript
try {
  const q = await ch.checkQueue('queue_name');
  console.log('Messages:', q.messageCount);
  console.log('Consumers:', q.consumerCount);
} catch (err) {
  console.error('Queue not found');
}
```

### Check Exchange
```javascript
try {
  await ch.checkExchange('exchange_name');
  console.log('✓ Exchange exists');
} catch (err) {
  console.error('✗ Exchange not found');
}
```

---

## DOCKER COMMANDS

```bash
# Start RabbitMQ
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:latest

# Check if running
docker ps

# View logs
docker logs <container_id>

# Stop
docker stop <container_id>

# Remove
docker rm <container_id>

# Dashboard
http://localhost:15672
# Username: guest
# Password: guest
```

---

## NPM COMMANDS

```bash
# Install dependencies
npm install

# Install amqplib specifically
npm install amqplib

# Run a file
node publisher.js
node consumer.js

# Multiple terminals
# Terminal 1: node consumer.js
# Terminal 2: node publisher.js
```

---

## DECISION TREE - WHICH EXCHANGE?

```
Do you want BROADCAST to everyone?
├─ YES → FANOUT Exchange
└─ NO
   ├─ Do you want EXACT KEY MATCH?
   │  ├─ YES → DIRECT Exchange
   │  └─ NO
   │     ├─ Do you want PATTERN MATCHING?
   │     │  ├─ YES → TOPIC Exchange
   │     │  └─ NO → HEADERS Exchange
```

---

## DECISION TREE - WHICH PATTERN?

```
Do you have ONE source → MANY processors?
├─ YES → One-to-Many (Pub/Sub)
│        - Use Fanout or Topic Exchange
│        - Each processor gets own queue
└─ NO
   └─ Do you have MANY sources → ONE processor?
      ├─ YES → Many-to-One
      │        - Use Topic Exchange
      │        - All publish to same exchange
      │        - One processor gets all
      └─ NO → Consider your use case
```

---

## 5 MOST IMPORTANT THINGS

1. **Always Acknowledge Messages**
   ```javascript
   ch.ack(msg);  // Or nack with requeue
   ```

2. **Routing Key ≠ Binding Key**
   - Routing Key: Set by publisher
   - Binding Key: Set by consumer
   - Direct: Must match exactly
   - Topic: Pattern matching

3. **Start Consumers Before Publisher**
   ```bash
   # Terminal 1
   node consumer.js
   
   # Terminal 2 (after consumer started)
   node publisher.js
   ```

4. **RabbitMQ Must Be Running**
   ```bash
   docker ps  # Verify it's there
   ```

5. **Use Manual Ack for Safety**
   ```javascript
   await ch.consume(queue, (msg) => {
     // Process
     ch.ack(msg);  // Only after success
   }, { noAck: false });
   ```

---

## QUICK TEST

```javascript
// test_connection.js
const amqp = require('amqplib');

(async () => {
  try {
    const conn = await amqp.connect('amqp://guest:guest@localhost');
    const ch = await conn.createChannel();
    
    console.log('✓ RabbitMQ connection successful!');
    
    await ch.close();
    await conn.close();
  } catch (err) {
    console.error('✗ Connection failed:', err.message);
    console.error('Start RabbitMQ: docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:latest');
  }
})();
```

---

**Print this page for quick reference!** 📄

Last Updated: April 20, 2026
