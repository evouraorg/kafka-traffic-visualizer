// ------ CONSTANTS & CONFIGURATION ------
const CANVAS_WIDTH = 1100;
const CANVAS_HEIGHT = 700;
const PARTITION_WIDTH = 400;
const PARTITION_HEIGHT = 30;
const PARTITION_SPACING = 20;
const PARTITION_START_X = 200;
const PARTITION_START_Y = 120;
const PRODUCER_X = 120;
const PRODUCER_SPACING = 80;
const CONSUMER_X = PARTITION_START_X + PARTITION_WIDTH + 50;
const CONSUMER_SPACING = 80;
const MAX_RECORD_RADIUS = 15;
const MIN_RECORD_RADIUS = 5;
const RECORD_SPEED = 1.5;
const CONSUMER_RATE = 3;
const PRODUCER_EFFECT_DURATION = 400;

// ------ STATE VARIABLES ------
// Core simulation state
let partitionCount = 8;
let producerCount = 2;
let consumerCount = 2;
let produceRate = 1;
let minValueSize = 10;
let maxValueSize = 1000;
let keyRange = 100;
let produceRandomness = 0.2;
let processingSpeed = 3.0;
let recordCounter = 0;

// Dynamic canvas height based on content
let canvasHeightDynamic = CANVAS_HEIGHT;
let scrollY = 0; // For scrolling when many partitions

// Data structures
let partitions = [];
let producers = [];
let consumers = [];
let producerEffects = [];

// Metrics tracking
let startTime;
let lastMetricsUpdateTime = 0;
const METRICS_UPDATE_INTERVAL = 500;

// UI Controls (now referencing HTML elements)
let partitionSlider, producerSlider, consumerSlider;
let produceRateSlider, minValueSizeSlider, maxValueSizeSlider;
let keyRangeSlider, produceRandomnessSlider, processingSpeedSlider;
let partitionInput, producerInput, consumerInput;
let produceRateInput, minValueSizeInput, maxValueSizeInput;
let keyRangeInput, produceRandomnessInput, processingSpeedInput;

// ------ SETUP & INITIALIZATION ------
function setup() {
  // Create canvas and add it to the container div
  let canvas = createCanvas(CANVAS_WIDTH, canvasHeightDynamic);
  canvas.parent('canvas-container');
  
  startTime = millis();
  
  // Get references to HTML controls
  setupControlReferences();
  attachControlEventListeners();
  
  initializeState();
}

function setupControlReferences() {
  // Get references to slider elements
  partitionSlider = select('#partitionSlider');
  producerSlider = select('#producerSlider');
  consumerSlider = select('#consumerSlider');
  produceRateSlider = select('#produceRateSlider');
  keyRangeSlider = select('#keyRangeSlider');
  produceRandomnessSlider = select('#produceRandomnessSlider');
  processingSpeedSlider = select('#processingSpeedSlider');
  minValueSizeSlider = select('#minValueSizeSlider');
  maxValueSizeSlider = select('#maxValueSizeSlider');
  
  // Get references to input elements
  partitionInput = select('#partitionInput');
  producerInput = select('#producerInput');
  consumerInput = select('#consumerInput');
  produceRateInput = select('#produceRateInput');
  keyRangeInput = select('#keyRangeInput');
  produceRandomnessInput = select('#produceRandomnessInput');
  processingSpeedInput = select('#processingSpeedInput');
  minValueSizeInput = select('#minValueSizeInput');
  maxValueSizeInput = select('#maxValueSizeInput');
}

function attachControlEventListeners() {
  // Add event listeners to sliders
  partitionSlider.input(() => handleSliderInput(partitionSlider, partitionInput, 'partitions'));
  producerSlider.input(() => handleSliderInput(producerSlider, producerInput, 'producers'));
  consumerSlider.input(() => handleSliderInput(consumerSlider, consumerInput, 'consumers'));
  produceRateSlider.input(() => handleSliderInput(produceRateSlider, produceRateInput, 'rate'));
  keyRangeSlider.input(() => handleSliderInput(keyRangeSlider, keyRangeInput, 'keyRange'));
  produceRandomnessSlider.input(() => handleSliderInput(produceRandomnessSlider, produceRandomnessInput, 'randomness'));
  processingSpeedSlider.input(() => handleSliderInput(processingSpeedSlider, processingSpeedInput, 'speed'));
  minValueSizeSlider.input(() => handleSliderInput(minValueSizeSlider, minValueSizeInput, 'minSize'));
  maxValueSizeSlider.input(() => handleSliderInput(maxValueSizeSlider, maxValueSizeInput, 'maxSize'));
  
  // Add event listeners to text inputs
  partitionInput.input(() => handleTextInput(partitionInput, partitionSlider, 'partitions'));
  producerInput.input(() => handleTextInput(producerInput, producerSlider, 'producers'));
  consumerInput.input(() => handleTextInput(consumerInput, consumerSlider, 'consumers'));
  produceRateInput.input(() => handleTextInput(produceRateInput, produceRateSlider, 'rate'));
  keyRangeInput.input(() => handleTextInput(keyRangeInput, keyRangeSlider, 'keyRange'));
  produceRandomnessInput.input(() => handleTextInput(produceRandomnessInput, produceRandomnessSlider, 'randomness'));
  processingSpeedInput.input(() => handleTextInput(processingSpeedInput, processingSpeedSlider, 'speed'));
  minValueSizeInput.input(() => handleTextInput(minValueSizeInput, minValueSizeSlider, 'minSize'));
  maxValueSizeInput.input(() => handleTextInput(maxValueSizeInput, maxValueSizeSlider, 'maxSize'));
}

function handleSliderInput(slider, textInput, type) {
  // Update text input when slider changes
  let value = slider.value();
  
  // Format the value for display
  if (type === 'randomness') {
    textInput.value(parseFloat(value).toFixed(3));
  } else {
    textInput.value(value);
  }
}

function handleTextInput(textInput, slider, type) {
  // Validate and update slider when text input changes
  let value = parseFloat(textInput.value());
  
  // Check if input is a valid number
  if (isNaN(value)) {
    // Restore to slider value if invalid
    textInput.value(slider.value());
    return;
  }
  
  // Ensure value is within slider range
  const min = slider.attribute('min');
  const max = slider.attribute('max');
  const step = slider.attribute('step') || 1;
  
  value = constrain(value, min, max);
  
  // Round to nearest step if needed
  if (step != 1) {
    value = Math.round(value / step) * step;
  } else {
    // For integer sliders, ensure integer value
    if (['partitions', 'producers', 'consumers', 'keyRange'].includes(type)) {
      value = Math.round(value);
    }
  }
  
  // Update slider with validated value
  slider.value(value);
  textInput.value(value);
}

function initializeState() {
  // Reset counters and state
  recordCounter = 0;
  producerEffects = [];
  
  // Initialize data structures
  initializePartitions();
  initializeProducers();
  initializeConsumers();
  
  // Update canvas height to accommodate partitions
  updateCanvasHeight();
}

function initializePartitions() {
  partitions = [];
  
  for (let i = 0; i < partitionCount; i++) {
    partitions.push({
      id: i,
      y: PARTITION_START_Y + i * (PARTITION_HEIGHT + PARTITION_SPACING),
      records: []
    });
  }
}

function initializeProducers() {
  producers = [];
  
  for (let i = 0; i < producerCount; i++) {
    // Calculate y position
    const y = PARTITION_START_Y + (partitionCount * (PARTITION_HEIGHT + PARTITION_SPACING)) / 2 * i / (producerCount || 1);
    
    // Generate a stable color based on index
    const hue = map(i, 0, producerCount, 0, 360);
    const color = colorFromHSB(hue, 70, 90);
    
    producers.push({
      id: i,
      y: y + PARTITION_HEIGHT / 2,
      color: color,
      nextProduceTime: frameCount + i * 10, // Stagger initial production
      recordsProduced: 0,
      bytesProduced: 0,
      produceRate: 0,
      recordsRate: 0,
      producedSinceLastUpdate: 0,
      bytesSinceLastUpdate: 0
    });
  }
}

function initializeConsumers() {
  consumers = [];
  
  // Create a temporary array to help with partition assignment
  let partitionAssignments = [];
  for (let i = 0; i < partitionCount; i++) {
    partitionAssignments.push(i % Math.max(1, consumerCount));
  }
  
  // Shuffle partition assignments for more realistic distribution
  if (consumerCount > 1) {
    shuffleArray(partitionAssignments);
  }
  
  for (let i = 0; i < consumerCount; i++) {
    // Find partitions assigned to this consumer
    const assignedPartitions = [];
    for (let j = 0; j < partitionCount; j++) {
      if (partitionAssignments[j] === i) {
        assignedPartitions.push(j);
      }
    }
    
    // Calculate average y position based on assigned partitions
    let avgY = 0;
    for (const partitionId of assignedPartitions) {
      avgY += partitions[partitionId].y + PARTITION_HEIGHT / 2;
    }
    avgY = avgY / Math.max(1, assignedPartitions.length);
    
    // Generate a stable color based on index (distinct from producers)
    const hue = map(i, 0, consumerCount, 180, 360);
    const color = colorFromHSB(hue, 70, 80);
    
    consumers.push({
      id: i,
      y: avgY,
      color: color,
      assignedPartitions: assignedPartitions,
      nextConsumeTime: frameCount + i * 5, // Stagger initial consumption
      recordsConsumed: 0,
      bytesConsumed: 0,
      consumeRate: 0,
      recordsRate: 0,
      consumedSinceLastUpdate: 0,
      bytesSinceLastUpdate: 0,
      transitRecords: []
    });
  }
}

function updateCanvasHeight() {
  const minHeight = 700; // Minimum canvas height
  const requiredHeight = PARTITION_START_Y + 
                        partitionCount * (PARTITION_HEIGHT + PARTITION_SPACING) + 
                        100; // Extra space for bottom margin
  
  canvasHeightDynamic = max(minHeight, requiredHeight);
  resizeCanvas(CANVAS_WIDTH, canvasHeightDynamic);
}

function handleScrolling() {
  if (mouseIsPressed && mouseY < canvasHeightDynamic - 180) {
    scrollY += (pmouseY - mouseY) * 0.5;
    // Limit scrolling
    const minScroll = 0;
    const maxScroll = max(0, PARTITION_START_Y + partitionCount * (PARTITION_HEIGHT + PARTITION_SPACING) - (canvasHeightDynamic - 200));
    scrollY = constrain(scrollY, minScroll, maxScroll);
  }
}

function handleControlChanges() {
  // Get values from sliders
  if (parseInt(partitionSlider.value()) !== partitionCount) {
    partitionCount = parseInt(partitionSlider.value());
    updatePartitions();
  }
  
  if (parseInt(producerSlider.value()) !== producerCount) {
    producerCount = parseInt(producerSlider.value());
    updateProducers();
  }
  
  if (parseInt(consumerSlider.value()) !== consumerCount) {
    consumerCount = parseInt(consumerSlider.value());
    updateConsumers();
  }
  
  // Update simple settings
  produceRate = parseInt(produceRateSlider.value());
  keyRange = parseInt(keyRangeSlider.value());
  produceRandomness = parseFloat(produceRandomnessSlider.value());
  processingSpeed = parseFloat(processingSpeedSlider.value());
  
  // Handle value size validation
  let newMinValueSize = parseInt(minValueSizeSlider.value());
  let newMaxValueSize = parseInt(maxValueSizeSlider.value());
  
  // Ensure max value is always >= min value
  if (newMaxValueSize < newMinValueSize) {
    if (minValueSize !== newMinValueSize) {
      // Min changed, update max to match
      maxValueSizeSlider.value(newMinValueSize);
      maxValueSizeInput.value(newMinValueSize);
      newMaxValueSize = newMinValueSize;
    } else {
      // Max changed, update min to match
      minValueSizeSlider.value(newMaxValueSize);
      minValueSizeInput.value(newMaxValueSize);
      newMinValueSize = newMaxValueSize;
    }
  }
  
  minValueSize = newMinValueSize;
  maxValueSize = newMaxValueSize;
}

// ------ STATE UPDATES ------
function updatePartitions() {
  // Save existing records
  const oldRecords = {};
  for (let i = 0; i < partitions.length; i++) {
    if (i < partitionCount) {
      oldRecords[i] = partitions[i].records;
    }
  }
  
  // Reinitialize partitions
  initializePartitions();
  
  // Restore records where possible
  for (let i = 0; i < partitionCount; i++) {
    if (oldRecords[i]) {
      partitions[i].records = oldRecords[i];
    }
  }
  
  // Reassign consumers
  initializeConsumers();
  
  // Update canvas height
  updateCanvasHeight();
}

function updateProducers() {
  // Keep the metrics for existing producers
  const oldProducers = [...producers];
  
  // Initialize new producers
  initializeProducers();
  
  // Copy metrics from old producers to new ones where applicable
  for (let i = 0; i < producerCount && i < oldProducers.length; i++) {
    producers[i].recordsProduced = oldProducers[i].recordsProduced;
    producers[i].bytesProduced = oldProducers[i].bytesProduced;
    producers[i].produceRate = oldProducers[i].produceRate;
    producers[i].recordsRate = oldProducers[i].recordsRate;
  }
}

function updateConsumers() {
  // Keep the metrics for existing consumers
  const oldConsumers = [...consumers];
  
  // Initialize new consumers
  initializeConsumers();
  
  // Copy metrics from old consumers to new ones where applicable
  for (let i = 0; i < consumerCount && i < oldConsumers.length; i++) {
    consumers[i].recordsConsumed = oldConsumers[i].recordsConsumed;
    consumers[i].bytesConsumed = oldConsumers[i].bytesConsumed;
    consumers[i].consumeRate = oldConsumers[i].consumeRate;
    consumers[i].recordsRate = oldConsumers[i].recordsRate;
    
    // Also handle any transit records (only those for partitions still assigned to this consumer)
    consumers[i].transitRecords = oldConsumers[i].transitRecords.filter(record => {
      return consumers[i].assignedPartitions.includes(record.partitionId);
    });
  }
}

function updateSimulation() {
  produceRecords();
  updateRecordPositions();
  consumeRecords();
  updateTransitRecords();
  
  // Update metrics periodically
  const currentTime = millis();
  if (currentTime - lastMetricsUpdateTime > METRICS_UPDATE_INTERVAL) {
    updateMetrics(currentTime);
    lastMetricsUpdateTime = currentTime;
  }
}

// ------ BUSINESS LOGIC ------
function produceRecords() {
  // Process producer effects first
  updateProducerEffects();
  
  // Check for new records to produce
  for (const producer of producers) {
    if (frameCount >= producer.nextProduceTime) {
      // Create and add a new record
      createRecord(producer);
      
      // Schedule next production time
      scheduleNextProduction(producer);
    }
  }
}

function updateProducerEffects() {
  // Remove expired producer effects
  for (let i = producerEffects.length - 1; i >= 0; i--) {
    if (millis() >= producerEffects[i].endTime) {
      producerEffects.splice(i, 1);
    }
  }
}

function createRecord(producer) {
  // Generate record characteristics
  const recordSize = random(minValueSize, maxValueSize);
  const recordRadius = calculateRecordRadius(recordSize);
  const recordKey = int(random(1, keyRange + 1));
  const partitionId = recordKey % partitionCount;
  const recordSpeed = calculateRecordSpeed(recordSize);
  
  // Create the record object
  const record = {
    id: recordCounter++,
    key: recordKey,
    value: recordSize,
    radius: recordRadius,
    x: PARTITION_START_X + recordRadius, // Start position
    color: producer.color,
    producerId: producer.id,
    partitionId: partitionId,
    speed: recordSpeed
  };
  
  // Add the record to the partition
  partitions[partitionId].records.push(record);
  
  // Update producer metrics
  producer.recordsProduced++;
  producer.bytesProduced += recordSize;
  producer.producedSinceLastUpdate++;
  producer.bytesSinceLastUpdate += recordSize;
  
  // Add visual effect for production
  addProducerEffect(producer, partitionId);
  
  // Log record production to console
  console.log(`Record produced: {"id": ${record.id}, "key": ${recordKey}, "valueBytes": ${Math.round(recordSize)}, "partition": ${partitionId}, "producer": ${producer.id}}`);
}

function calculateRecordRadius(size) {
  // Handle edge case when min and max are too close
  if (minValueSize >= maxValueSize || Math.abs(maxValueSize - minValueSize) < 2) {
    return (MIN_RECORD_RADIUS + MAX_RECORD_RADIUS) / 2; // Return average radius
  }
  
  // Map size to radius logarithmically
  return map(
    log(size), 
    log(minValueSize), 
    log(maxValueSize), 
    MIN_RECORD_RADIUS, 
    MAX_RECORD_RADIUS
  );
}

function calculateRecordSpeed(size) {
  // Calculate the radius for this record size
  const radius = calculateRecordRadius(size);
  
  // Calculate the actual travel distance (accounting for radius at start and end)
  const travelDistance = PARTITION_WIDTH - 2 * radius;
  
  // For a 1000-byte record, we want it to take 1 second (60 frames) to traverse
  // For larger records, time should increase proportionally
  // Speed = Distance / Time, where Time = Size / 1000 * 60 frames
  const frames = (size / 1000) * 60;
  const speed = travelDistance / frames * processingSpeed;
  
  return speed;
}

function scheduleNextProduction(producer) {
  // Base time between records in frames (60 frames per second)
  const framesPerRecord = 60 / produceRate;
  
  // Apply randomness in seconds, capped at 1 second maximum
  let randomTimeFrames = 0;
  
  // Apply randomness individually for each producer
  // Use the produceRandomness value which is now constrained to 0-1 seconds
  const randomDelaySec = random(0, produceRandomness);
  randomTimeFrames = randomDelaySec * 60;
  
  // Set next production time
  producer.nextProduceTime = frameCount + int(framesPerRecord + randomTimeFrames);
}

function addProducerEffect(producer, partitionId) {
  // Create a visual effect line from producer to partition
  const effect = {
    startX: PRODUCER_X + 15,
    startY: producer.y,
    endX: PARTITION_START_X,
    endY: partitions[partitionId].y + PARTITION_HEIGHT / 2,
    color: producer.color,
    endTime: millis() + PRODUCER_EFFECT_DURATION
  };
  
  producerEffects.push(effect);
}

function updateRecordPositions() {
  // Update record positions within each partition
  for (const partition of partitions) {
    // Process records from right to left (newest to oldest)
    // This ensures that records maintain their relative order
    
    // First pass: Process records in FIFO order (oldest first)
    // This makes sure older records move first
    for (let i = 0; i < partition.records.length; i++) {
      const record = partition.records[i];
      
      // Check if we're at the front of the line
      if (i === 0) {
        // First record can always move forward up to the end
        const maxX = PARTITION_START_X + PARTITION_WIDTH - record.radius;
        record.x = min(record.x + record.speed, maxX);
      } else {
        // For other records, check the record directly ahead
        const recordAhead = partition.records[i - 1];
        const minDistance = recordAhead.radius + record.radius;
        const maxPossibleX = recordAhead.x - minDistance;
        
        // Only move if we won't collide with the record ahead
        if (record.x < maxPossibleX) {
          // Can move, but limited by the record ahead
          record.x = min(record.x + record.speed, maxPossibleX);
        }
      }
    }
    
    // Second pass: Sort records by x position to ensure display order matches processing order
    // This is a safety measure to prevent any records from visually passing each other
    partition.records.sort((a, b) => b.x - a.x);
  }
}

function consumeRecords() {
  for (const consumer of consumers) {
    if (frameCount >= consumer.nextConsumeTime) {
      let recordsConsumed = 0;
      
      // Check all assigned partitions for records to consume
      for (const partitionId of consumer.assignedPartitions) {
        if (checkPartitionForConsumption(consumer, partitionId)) {
          recordsConsumed++;
        }
      }
      
      // Set next consume time, with longer delay if we consumed multiple records
      // This helps smooth out consumption rate
      if (recordsConsumed > 1) {
        consumer.nextConsumeTime = frameCount + CONSUMER_RATE * recordsConsumed * 0.8;
      } else {
        consumer.nextConsumeTime = frameCount + CONSUMER_RATE;
      }
    }
  }
}

function checkPartitionForConsumption(consumer, partitionId) {
  const partition = partitions[partitionId];
  
  // Check if there's a record at the end of the partition
  if (partition.records.length > 0) {
    const firstRecord = partition.records[0];
    
    // Check if the record has reached the end of the partition
    if (firstRecord.x >= PARTITION_START_X + PARTITION_WIDTH - firstRecord.radius) {
      // Remove from partition and start transit to consumer
      const record = partition.records.shift();
      transferRecordToConsumer(consumer, record, partitionId);
      return true;
    }
  }
  return false;
}

function transferRecordToConsumer(consumer, record, partitionId) {
  // Calculate start and end points for transit
  const startX = PARTITION_START_X + PARTITION_WIDTH;
  const startY = partitions[partitionId].y + PARTITION_HEIGHT / 2;
  const endX = CONSUMER_X;
  const endY = consumer.y;
  
  // Add to transit records with path information
  consumer.transitRecords.push({
    ...record,
    x: startX,
    y: startY,
    startX: startX,
    startY: startY,
    endX: endX,
    endY: endY,
    progress: 0
  });
}

function updateTransitRecords() {
  for (const consumer of consumers) {
    for (let i = consumer.transitRecords.length - 1; i >= 0; i--) {
      const record = consumer.transitRecords[i];
      
      // Update progress
      record.progress += 0.05 * processingSpeed;
      
      // Calculate new position along the path
      record.x = lerp(record.startX, record.endX, record.progress);
      record.y = lerp(record.startY, record.endY, record.progress);
      
      // If record has reached the consumer, finalize consumption
      if (record.progress >= 1.0) {
        finalizeConsumption(consumer, record);
        consumer.transitRecords.splice(i, 1);
      }
    }
  }
}

function finalizeConsumption(consumer, record) {
  // Update consumer metrics
  consumer.recordsConsumed++;
  consumer.bytesConsumed += record.value;
  consumer.consumedSinceLastUpdate++;
  consumer.bytesSinceLastUpdate += record.value;
}

// Metrics calculation functions
function updateMetrics(currentTime) {
  const elapsedSeconds = (currentTime - lastMetricsUpdateTime) / 1000;
  
  if (elapsedSeconds <= 0) return;
  
  // Update producer metrics
  for (const producer of producers) {
    updateProducerMetrics(producer, elapsedSeconds);
  }
  
  // Update consumer metrics
  for (const consumer of consumers) {
    updateConsumerMetrics(consumer, elapsedSeconds);
  }
}

function updateProducerMetrics(producer, elapsedSeconds) {
  // Calculate actual produce rate from real data
  // We use a direct measurement of records produced
  if (elapsedSeconds > 0) {
    producer.produceRate = producer.bytesSinceLastUpdate / elapsedSeconds;
    producer.recordsRate = producer.producedSinceLastUpdate / elapsedSeconds;
    
    // If no records were produced in this interval but the system has a target rate,
    // gradually decay the displayed rate toward the target rate
    if (producer.producedSinceLastUpdate === 0 && producer.recordsProduced > 0) {
      // Gradually adjust shown rate to match target rate
      const targetRate = produceRate; // Records per second
      producer.recordsRate = lerp(producer.recordsRate, targetRate, 0.3);
    }
  }
  
  producer.producedSinceLastUpdate = 0;
  producer.bytesSinceLastUpdate = 0;
}

function updateConsumerMetrics(consumer, elapsedSeconds) {
  // Calculate B/s instead of KB/s (no division by 1024)
  consumer.consumeRate = consumer.bytesSinceLastUpdate / elapsedSeconds;
  
  // Update record rate based on actual consumption
  if (consumer.consumedSinceLastUpdate > 0) {
    consumer.recordsRate = consumer.consumedSinceLastUpdate / elapsedSeconds;
  } else {
    // If no records were consumed, gradually decrease the rate toward zero
    consumer.recordsRate = consumer.recordsRate * 0.5;
    
    // If the rate gets very small, just set it to zero
    if (consumer.recordsRate < 0.05) {
      consumer.recordsRate = 0;
    }
  }
  
  consumer.consumedSinceLastUpdate = 0;
  consumer.bytesSinceLastUpdate = 0;
}

// ------ RENDERING ------
function renderSimulation() {
  push();
  translate(0, -scrollY);
  
  // Draw simulation components
  drawPartitions();
  drawProducers();
  drawConsumers();
  drawProducerEffects();
  drawMetrics();
  
  pop(); // End scrolling transform
}

function drawPartitions() {
  // Set consistent styling for all partitions
  fill(255);
  stroke(0);
  strokeWeight(1);
  
  for (let i = 0; i < partitions.length; i++) {
    const partition = partitions[i];
    
    // Start fresh for each partition to ensure consistent styling
    push();
    fill(255);
    stroke(0);
    strokeWeight(1);
    
    // Draw partition rectangle - consistent style for all partitions
    rect(PARTITION_START_X, partition.y, PARTITION_WIDTH, PARTITION_HEIGHT);
    pop();
    
    // Draw partition label - consistent style and size
    fill(0);
    noStroke();
    textAlign(RIGHT, CENTER);
    textSize(12); // Fixed size for all partition labels
    text(`P${i}`, PARTITION_START_X - 10, partition.y + PARTITION_HEIGHT / 2);
    
    // Draw records in the partition
    drawPartitionRecords(partition);
  }
}

function drawPartitionRecords(partition) {
  for (const record of partition.records) {
    // Draw record circle
    fill(record.color);
    stroke(0);
    strokeWeight(1);
    ellipse(record.x, partition.y + PARTITION_HEIGHT / 2, record.radius * 2, record.radius * 2);
    
    // Draw record ID inside if large enough
    if (record.radius > 8) {
      fill(255);
      noStroke();
      textAlign(CENTER, CENTER);
      textSize(10);
      text(record.key, record.x, partition.y + PARTITION_HEIGHT / 2);
    }
  }
}

function drawProducers() {
  for (let i = 0; i < producers.length; i++) {
    const producer = producers[i];
    
    // Draw producer symbol (triangle)
    fill(producer.color);
    stroke(0);
    strokeWeight(1);
    
    push();
    translate(PRODUCER_X, producer.y);
    triangle(-15, -15, 15, 0, -15, 15);
    pop();
    
    // Draw producer ID inside the triangle, shifted 10px to the left
    fill(255);
    noStroke();
    textAlign(CENTER, CENTER);
    textSize(10);
    textStyle(BOLD);
    text(i, PRODUCER_X - 10, producer.y);
    textStyle(NORMAL);
  }
}

function drawConsumers() {
  for (let i = 0; i < consumers.length; i++) {
    const consumer = consumers[i];
    
    // Get the fixed y position (established in initialization)
    const avgY = consumer.y;
    
    // Draw consumer symbol (rectangle)
    fill(consumer.color);
    stroke(0);
    strokeWeight(1);
    rect(CONSUMER_X, avgY - 15, 30, 30);
    
    // Draw consumer ID inside the rectangle
    fill(255);
    noStroke();
    textAlign(CENTER, CENTER);
    textSize(10);
    textStyle(BOLD);
    text(i, CONSUMER_X + 15, avgY);
    textStyle(NORMAL);
    
    // Draw lines between consumer and its assigned partitions
    stroke(consumer.color);
    strokeWeight(1.8);
    drawingContext.setLineDash([5, 5]);
    for (const partitionId of consumer.assignedPartitions) {
      const partitionY = partitions[partitionId].y + PARTITION_HEIGHT / 2;
      line(PARTITION_START_X + PARTITION_WIDTH, partitionY, CONSUMER_X, avgY);
    }
    drawingContext.setLineDash([]);
    
    // Draw transit records
    drawTransitRecords(consumer);
  }
}

function drawTransitRecords(consumer) {
  for (const record of consumer.transitRecords) {
    // Draw record circle
    fill(record.color);
    stroke(0);
    strokeWeight(1);
    ellipse(record.x, record.y, record.radius * 2, record.radius * 2);
    
    // Draw record key inside if large enough
    if (record.radius > 8) {
      fill(255);
      noStroke();
      textAlign(CENTER, CENTER);
      textSize(10);
      text(record.key, record.x, record.y);
    }
  }
}

function drawProducerEffects() {
  // Draw any active producer effects
  for (const effect of producerEffects) {
    push();
    strokeWeight(2);
    stroke(effect.color);
    line(effect.startX, effect.startY, effect.endX, effect.endY);
    pop();
  }
}

function drawMetrics() {
  // Draw producer metrics
  for (let i = 0; i < producers.length; i++) {
    const producer = producers[i];
    const metricsY = producer.y - 20;
    
    fill(0);
    noStroke();
    textAlign(LEFT, CENTER); // Changed from RIGHT to LEFT for better visibility
    textSize(10);
    
    // Moved metrics further left to ensure visibility and avoid overlap with producer shape
    text(`Records: ${producer.recordsProduced}`, PRODUCER_X - 100, metricsY);
    text(`Sum B: ${formatBytes(producer.bytesProduced)}`, PRODUCER_X - 100, metricsY + 15);
    text(`${Math.round(producer.produceRate)} B/s`, PRODUCER_X - 100, metricsY + 30);
    text(`${Math.round(producer.recordsRate * 100) / 100} rec/s`, PRODUCER_X - 100, metricsY + 45);
  }
  
  // Draw consumer metrics
  for (let i = 0; i < consumers.length; i++) {
    const consumer = consumers[i];
    const metricsY = consumer.y - 20;
    
    fill(0);
    noStroke();
    textAlign(LEFT, CENTER);
    textSize(10);
    
    text(`Records: ${consumer.recordsConsumed}`, CONSUMER_X + 45, metricsY);
    text(`Sum B: ${formatBytes(consumer.bytesConsumed)}`, CONSUMER_X + 45, metricsY + 15);
    text(`${Math.round(consumer.consumeRate)} B/s`, CONSUMER_X + 45, metricsY + 30);
    text(`${Math.round(consumer.recordsRate * 100) / 100} rec/s`, CONSUMER_X + 45, metricsY + 45);
  }
}

// ------ UTILITIES ------
function formatBytes(bytes) {
  if (bytes < 1024) {
    return Math.round(bytes) + ' B';
  } else if (bytes < 1024 * 1024) {
    return (bytes / 1024).toFixed(2) + ' KB';
  } else {
    return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  }
}

function colorFromHSB(h, s, b) {
  colorMode(HSB, 360, 100, 100);
  const col = color(h, s, b);
  colorMode(RGB, 255, 255, 255);
  return col;
}

function shuffleArray(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

function draw() {
  background(240);
  
  // Handle events
  handleScrolling();
  handleControlChanges();
  
  // Update simulation
  updateSimulation();
  
  // Render simulation
  renderSimulation();
}