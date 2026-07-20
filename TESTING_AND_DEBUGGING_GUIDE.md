# RabbitMQ Testing, Debugging & Troubleshooting Guide

## Complete Guide to Testing Your Implementation

---

## TABLE OF CONTENTS

1. Unit Testing
2. Integration Testing
3. System Testing
4. Debugging Techniques
5. Common Problems & Solutions
6. Performance Optimization
7. Monitoring Setup

---

## PART 1: Unit Testing

### Test Setup with Node Test Framework:

```javascript
// test_publisher.js
const assert = require('assert');
const amqp = require('amqplib');

async function testPublisherConnection() {
  console.log('TEST 1: Publisher can connect to RabbitMQ');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const channel = await connection.createChannel();
    
    assert(channel, 'Channel should be created');
    console.log('✓ PASSED: Connection successful\n');
    
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ FAILED:', error.message);
  }
}

async function testExchangeCreation() {
  console.log('TEST 2: Can create exchange');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const channel = await connection.createChannel();
    
    await channel.assertExchange('test_exchange', 'direct', { durable: true });
    console.log('✓ PASSED: Exchange created\n');
    
    // Cleanup
    await channel.deleteExchange('test_exchange');
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ FAILED:', error.message);
  }
}

async function testQueueCreation() {
  console.log('TEST 3: Can create queue');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const channel = await connection.createChannel();
    
    const queue = await channel.assertQueue('test_queue', { durable: true });
    assert(queue.queue, 'Queue should be created');
    console.log('✓ PASSED: Queue created\n');
    
    // Cleanup
    await channel.deleteQueue('test_queue');
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ FAILED:', error.message);
  }
}

async function testMessagePublishing() {
  console.log('TEST 4: Can publish message');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const channel = await connection.createChannel();
    
    const exchange = 'test_exchange';
    const queue = 'test_queue';
    
    await channel.assertExchange(exchange, 'direct', { durable: true });
    await channel.assertQueue(queue, { durable: true });
    await channel.bindQueue(queue, exchange, 'test_key');
    
    const message = 'Test message';
    const published = channel.publish(
      exchange,
      'test_key',
      Buffer.from(message)
    );
    
    assert(published === true, 'Message should be published');
    console.log('✓ PASSED: Message published\n');
    
    // Cleanup
    await channel.deleteQueue(queue);
    await channel.deleteExchange(exchange);
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ FAILED:', error.message);
  }
}

// Run all tests
async function runAllTests() {
  console.log('═══════════════════════════════════════════');
  console.log('         UNIT TESTS FOR PUBLISHER');
  console.log('═══════════════════════════════════════════\n');
  
  await testPublisherConnection();
  await testExchangeCreation();
  await testQueueCreation();
  await testMessagePublishing();
  
  console.log('═══════════════════════════════════════════');
  console.log('         ALL TESTS COMPLETED');
  console.log('═══════════════════════════════════════════');
}

runAllTests().catch(console.error);
```

### Consumer Tests:

```javascript
// test_consumer.js
const assert = require('assert');
const amqp = require('amqplib');

async function testConsumerConnection() {
  console.log('TEST 1: Consumer can connect');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const channel = await connection.createChannel();
    
    assert(channel, 'Channel should exist');
    console.log('✓ PASSED: Consumer connected\n');
    
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ FAILED:', error.message);
  }
}

async function testQueueBinding() {
  console.log('TEST 2: Consumer can bind to queue');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const channel = await connection.createChannel();
    
    const exchange = 'test_exchange';
    const queue = 'test_consumer_queue';
    
    await channel.assertExchange(exchange, 'direct', { durable: true });
    await channel.assertQueue(queue, { durable: true });
    await channel.bindQueue(queue, exchange, 'test_key');
    
    console.log('✓ PASSED: Queue binding successful\n');
    
    // Cleanup
    await channel.deleteQueue(queue);
    await channel.deleteExchange(exchange);
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ FAILED:', error.message);
  }
}

async function testMessageConsumption() {
  console.log('TEST 3: Consumer can receive messages');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const channel = await connection.createChannel();
    
    const exchange = 'test_exchange';
    const queue = 'test_consumer_queue';
    const testMessage = 'Test consumer message';
    
    // Setup
    await channel.assertExchange(exchange, 'direct', { durable: true });
    await channel.assertQueue(queue, { durable: true });
    await channel.bindQueue(queue, exchange, 'test_key');
    
    // Publish a message
    channel.publish(exchange, 'test_key', Buffer.from(testMessage));
    
    // Try to consume
    let received = false;
    await channel.consume(queue, (msg) => {
      if (msg) {
        received = msg.content.toString() === testMessage;
        channel.ack(msg);
      }
    }, { noAck: false });
    
    // Wait a moment for message to arrive
    await new Promise(r => setTimeout(r, 100));
    
    assert(received, 'Consumer should receive message');
    console.log('✓ PASSED: Message received\n');
    
    // Cleanup
    await channel.deleteQueue(queue);
    await channel.deleteExchange(exchange);
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ FAILED:', error.message);
  }
}

async function runAllConsumerTests() {
  console.log('═══════════════════════════════════════════');
  console.log('         UNIT TESTS FOR CONSUMER');
  console.log('═══════════════════════════════════════════\n');
  
  await testConsumerConnection();
  await testQueueBinding();
  await testMessageConsumption();
  
  console.log('═══════════════════════════════════════════');
  console.log('         ALL TESTS COMPLETED');
  console.log('═══════════════════════════════════════════');
}

runAllConsumerTests().catch(console.error);
```

---

## PART 2: Integration Testing

### End-to-End Test:

```javascript
// test_e2e.js - Complete flow test
const amqp = require('amqplib');
const assert = require('assert');

async function endToEndTest() {
  console.log('═══════════════════════════════════════════');
  console.log('       END-TO-END INTEGRATION TEST');
  console.log('═══════════════════════════════════════════\n');
  
  try {
    // Step 1: Setup
    console.log('STEP 1: Setting up exchange and queues...');
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const publisherChannel = await connection.createChannel();
    const consumerChannel = await connection.createChannel();
    
    const exchange = 'e2e_test_exchange';
    const queue = 'e2e_test_queue';
    const testMessage = { id: 1, text: 'E2E Test Message' };
    
    await publisherChannel.assertExchange(exchange, 'direct', { durable: true });
    await consumerChannel.assertQueue(queue, { durable: true });
    await consumerChannel.bindQueue(queue, exchange, 'e2e_key');
    console.log('✓ Setup complete\n');
    
    // Step 2: Publish message
    console.log('STEP 2: Publishing message...');
    publisherChannel.publish(
      exchange,
      'e2e_key',
      Buffer.from(JSON.stringify(testMessage))
    );
    console.log('✓ Message published\n');
    
    // Step 3: Consume message
    console.log('STEP 3: Consuming message...');
    let receivedMessage = null;
    
    await consumerChannel.consume(queue, (msg) => {
      if (msg) {
        receivedMessage = JSON.parse(msg.content.toString());
        consumerChannel.ack(msg);
      }
    });
    
    // Wait for message
    await new Promise(r => setTimeout(r, 500));
    
    assert.deepStrictEqual(receivedMessage, testMessage);
    console.log('✓ Message received and verified\n');
    
    // Step 4: Cleanup
    console.log('STEP 4: Cleaning up...');
    await consumerChannel.deleteQueue(queue);
    await publisherChannel.deleteExchange(exchange);
    await publisherChannel.close();
    await consumerChannel.close();
    await connection.close();
    console.log('✓ Cleanup complete\n');
    
    console.log('═══════════════════════════════════════════');
    console.log('✓ ALL INTEGRATION TESTS PASSED');
    console.log('═══════════════════════════════════════════');
    
  } catch (error) {
    console.error('✗ INTEGRATION TEST FAILED:');
    console.error(error.message);
    process.exit(1);
  }
}

endToEndTest();
```

---

## PART 3: System Testing

### Load Test (Multiple Messages):

```javascript
// test_load.js - Test with many messages
const amqp = require('amqplib');

async function loadTest() {
  console.log('═══════════════════════════════════════════');
  console.log('           LOAD TEST');
  console.log('═══════════════════════════════════════════\n');
  
  try {
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    const pubChannel = await connection.createChannel();
    const conChannel = await connection.createChannel();
    
    const exchange = 'load_test_exchange';
    const queue = 'load_test_queue';
    const messageCount = 100;
    
    // Setup
    await pubChannel.assertExchange(exchange, 'direct', { durable: true });
    await conChannel.assertQueue(queue, { durable: true });
    await conChannel.bindQueue(queue, exchange, 'load_test_key');
    
    let receivedCount = 0;
    
    // Setup consumer first
    console.log('Starting consumer...');
    await conChannel.consume(queue, (msg) => {
      if (msg) {
        receivedCount++;
        conChannel.ack(msg);
      }
    });
    
    // Publish many messages
    console.log(`Publishing ${messageCount} messages...\n`);
    const startTime = Date.now();
    
    for (let i = 0; i < messageCount; i++) {
      pubChannel.publish(
        exchange,
        'load_test_key',
        Buffer.from(JSON.stringify({ id: i, timestamp: new Date() }))
      );
      
      if ((i + 1) % 20 === 0) {
        console.log(`Published ${i + 1} messages...`);
      }
    }
    
    // Wait for all messages to be consumed
    console.log('\nWaiting for all messages to be consumed...');
    while (receivedCount < messageCount) {
      await new Promise(r => setTimeout(r, 100));
    }
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    const throughput = messageCount / duration;
    
    console.log(`\n✓ Test Results:`);
    console.log(`  Messages sent: ${messageCount}`);
    console.log(`  Messages received: ${receivedCount}`);
    console.log(`  Duration: ${duration.toFixed(2)} seconds`);
    console.log(`  Throughput: ${throughput.toFixed(0)} msg/sec\n`);
    
    // Cleanup
    await conChannel.deleteQueue(queue);
    await pubChannel.deleteExchange(exchange);
    await pubChannel.close();
    await conChannel.close();
    await connection.close();
    
    console.log('═══════════════════════════════════════════');
    console.log('✓ LOAD TEST COMPLETED');
    console.log('═══════════════════════════════════════════');
    
  } catch (error) {
    console.error('✗ LOAD TEST FAILED:', error.message);
  }
}

loadTest();
```

---

## PART 4: Debugging Techniques

### Verbose Logging:

```javascript
// Debug version with detailed logging
const amqp = require('amqplib');

async function debugPublisher() {
  console.log('[DEBUG] Starting publisher...');
  
  try {
    console.log('[DEBUG] Connecting to amqp://guest:guest@localhost');
    const connection = await amqp.connect('amqp://guest:guest@localhost');
    console.log('[DEBUG] ✓ Connected');
    
    console.log('[DEBUG] Creating channel');
    const channel = await connection.createChannel();
    console.log('[DEBUG] ✓ Channel created');
    
    const exchange = 'debug_exchange';
    const routingKey = 'debug_key';
    
    console.log(`[DEBUG] Asserting exchange: ${exchange} (type: direct)`);
    await channel.assertExchange(exchange, 'direct', { durable: true });
    console.log('[DEBUG] ✓ Exchange created/verified');
    
    const message = { test: 'message', timestamp: new Date() };
    console.log('[DEBUG] Message to publish:', JSON.stringify(message));
    
    console.log(`[DEBUG] Publishing to ${exchange} with routing key ${routingKey}`);
    const published = channel.publish(
      exchange,
      routingKey,
      Buffer.from(JSON.stringify(message))
    );
    
    console.log(`[DEBUG] Publish result: ${published ? 'SUCCESS' : 'FAILED'}`);
    
    console.log('[DEBUG] Closing channel');
    await channel.close();
    console.log('[DEBUG] ✓ Channel closed');
    
    console.log('[DEBUG] Closing connection');
    await connection.close();
    console.log('[DEBUG] ✓ Connection closed');
    
    console.log('[DEBUG] Publisher finished successfully');
    
  } catch (error) {
    console.error('[DEBUG] ERROR:', error.message);
    console.error('[DEBUG] Stack:', error.stack);
    process.exit(1);
  }
}

debugPublisher();
```

### Dashboard Inspection Script:

```javascript
// inspect_rabbitmq.js - Check RabbitMQ status
const http = require('http');

function getRabbitMQInfo() {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 15672,
      path: '/api/overview',
      method: 'GET',
      auth: 'guest:guest'
    };
    
    const req = http.request(options, (res) => {
      let data = '';
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    });
    
    req.on('error', reject);
    req.end();
  });
}

async function inspectRabbitMQ() {
  console.log('═══════════════════════════════════════════');
  console.log('      RABBITMQ STATUS INSPECTION');
  console.log('═══════════════════════════════════════════\n');
  
  try {
    const info = await getRabbitMQInfo();
    
    console.log('✓ RabbitMQ is running!\n');
    console.log('RabbitMQ Version:', info.rabbitmq_version);
    console.log('Memory Used:', (info.memory.used / 1024 / 1024).toFixed(2), 'MB');
    console.log('Memory Limit:', (info.memory.limit / 1024 / 1024).toFixed(2), 'MB');
    console.log('File Descriptor Used:', info.fd_used);
    console.log('File Descriptor Available:', info.fd_total);
    console.log('Processes:', info.os_pid);
    
  } catch (error) {
    console.error('✗ Cannot connect to RabbitMQ');
    console.error('Make sure RabbitMQ is running:');
    console.error('  docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:latest');
    process.exit(1);
  }
}

inspectRabbitMQ();
```

---

## PART 5: Common Problems & Solutions

### Problem 1: "Connection Refused"

**Symptoms:**
```
Error: connect ECONNREFUSED 127.0.0.1:5672
```

**Diagnosis:**
```bash
docker ps
# Should show rabbitmq container
```

**Solution:**
```bash
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:latest
```

---

### Problem 2: "Message Not Received"

**Symptoms:**
- Publisher shows "published"
- Consumer shows no messages

**Checklist:**
```javascript
// 1. Verify exchange names match
console.log('Publisher exchange:', publisherExchange);  // e.g., 'myexchange'
console.log('Consumer exchange:', consumerExchange);    // e.g., 'myexchange'

// 2. Verify binding keys/routing keys match
console.log('Routing key:', routingKey);                // e.g., 'mykey'
console.log('Binding key:', bindingKey);                // e.g., 'mykey'

// 3. For Direct: must match exactly
assert(routingKey === bindingKey);

// 4. For Topic: verify pattern
// If routing key is 'user.admin.created'
// Binding key could be 'user.admin.*' or 'user.#' or '#'
```

**Solution:**
```javascript
// Debug: Log all binding operations
channel.on('error', (error) => {
  console.error('Channel error:', error);
});

connection.on('error', (error) => {
  console.error('Connection error:', error);
});
```

---

### Problem 3: "Messages Stuck in Queue"

**Symptoms:**
- Messages appear in RabbitMQ Dashboard
- But consumer doesn't process them

**Cause:** Missing `channel.ack(msg)`

**Solution:**
```javascript
// WRONG
await channel.consume(queue, (msg) => {
  console.log(msg.content.toString());
  // Forgot ack!
});

// CORRECT
await channel.consume(queue, (msg) => {
  console.log(msg.content.toString());
  channel.ack(msg);  // Always acknowledge!
});
```

---

### Problem 4: "Duplicate Message Processing"

**Symptoms:**
- Same message processed multiple times
- Even though consumer acknowledged

**Cause:** Multiple consumers with same queue

**Solution:**
```javascript
// Wrong: Each consumer creates its own queue
const queue1 = 'my_queue';
const queue2 = 'my_queue';

// Correct: All consumers share same queue
const queue = 'my_queue';

// Distribute work:
// Consumer 1 gets message 1
// Consumer 2 gets message 2
// etc.
```

---

### Problem 5: "CPU Usage Spike"

**Symptoms:**
- One consumer consuming messages very fast
- Tight loop with no delay

**Solution:**
```javascript
// Add delay in processing
await channel.consume(queue, async (msg) => {
  const data = JSON.parse(msg.content.toString());
  
  // Do work
  await processMessage(data);
  
  // Add small delay to prevent CPU spike
  await new Promise(r => setTimeout(r, 10));
  
  channel.ack(msg);
});
```

---

## PART 6: Performance Optimization

### Optimize for Throughput:

```javascript
// High throughput configuration
async function highThroughputConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();
  
  await channel.assertQueue('fast_queue', { durable: true });
  
  // Key optimizations:
  await channel.prefetch(10);  // Process multiple at once
  
  let processed = 0;
  const startTime = Date.now();
  
  await channel.consume('fast_queue', async (msg) => {
    if (msg) {
      processed++;
      
      // Minimal processing
      const data = msg.content.toString();
      
      // Batch acknowledge for speed
      if (processed % 10 === 0) {
        channel.ack(msg);
      }
    }
  }, { noAck: false });
  
  setInterval(() => {
    const elapsed = (Date.now() - startTime) / 1000;
    const throughput = processed / elapsed;
    console.log(`Processed ${processed} messages (${throughput.toFixed(0)} msg/sec)`);
  }, 5000);
}
```

### Optimize for Reliability:

```javascript
// High reliability configuration
async function reliableConsumer() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();
  
  await channel.assertQueue('reliable_queue', { durable: true });
  
  // Key optimizations:
  await channel.prefetch(1);  // One at a time (slowest but safest)
  
  await channel.consume('reliable_queue', async (msg) => {
    if (msg) {
      try {
        const data = JSON.parse(msg.content.toString());
        
        // Process with full error handling
        await processWithRetry(data);
        
        // Only ack after successful processing
        channel.ack(msg);
        
      } catch (error) {
        console.error('Processing failed:', error);
        
        // Requeue for retry
        channel.nack(msg, false, true);
      }
    }
  }, { noAck: false });
}
```

---

## PART 7: Monitoring Setup

### Simple Monitoring Script:

```javascript
// monitor.js - Monitor queue depths
const amqp = require('amqplib');

async function monitorQueues() {
  const connection = await amqp.connect('amqp://guest:guest@localhost');
  const channel = await connection.createChannel();
  
  const queues = ['queue1', 'queue2', 'queue3'];
  
  console.log('═══════════════════════════════════════════');
  console.log('    RABBITMQ QUEUE MONITORING');
  console.log('═══════════════════════════════════════════\n');
  
  setInterval(async () => {
    console.clear();
    console.log(`Monitoring at ${new Date().toLocaleTimeString()}\n`);
    
    for (const queue of queues) {
      try {
        const queueInfo = await channel.checkQueue(queue);
        
        console.log(`Queue: ${queue}`);
        console.log(`  Messages ready: ${queueInfo.messageCount}`);
        console.log(`  Consumers: ${queueInfo.consumerCount}`);
        console.log('');
        
      } catch (error) {
        console.log(`Queue: ${queue}`);
        console.log(`  Status: Not found`);
        console.log('');
      }
    }
    
    console.log('═══════════════════════════════════════════');
    console.log('Refresh interval: 5 seconds');
    console.log('Press Ctrl+C to stop');
    
  }, 5000);
}

monitorQueues();
```

---

## Testing Checklist

Before deploying to production:

- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Load test throughput acceptable
- [ ] Error handling tested
- [ ] Message acknowledgment working
- [ ] Queue binding correct
- [ ] Exchange types correct
- [ ] Routing keys verified
- [ ] Consumer failures handled
- [ ] Monitoring setup complete
- [ ] Dashboard accessible
- [ ] Logs are clear and useful
- [ ] No memory leaks
- [ ] CPU usage acceptable
- [ ] Startup/shutdown clean

---

This completes the testing and debugging guide!

