// ------ CONSTANTS & CONFIGURATION ------
const CANVAS_WIDTH = 1100;
const CANVAS_HEIGHT = 700;
const PARTITION_WIDTH = 400;
const PARTITION_HEIGHT = 30;
const PARTITION_SPACING = 20;
const PARTITION_START_X = 200;
const PARTITION_START_Y = 120;
const PRODUCER_X = 120;
const CONSUMER_X = PARTITION_START_X + PARTITION_WIDTH + 50;
const MAX_RECORD_RADIUS = 15;
const MIN_RECORD_RADIUS = 5;
const CONSUMER_RATE = 3;
const PRODUCER_EFFECT_DURATION = 400;

// ------ STATE VARIABLES ------
// Core simulation state
let globalRecordCounter = 0;

let partitionCount = 8;

let producerCount = 2;
let producerRate = 1;
let producerDelayRandomness = 0.2;

let consumerCount = 2;
let consumerProcessingSpeed = 1.0;
let consumerAssignmentStrategy = 'round-robin';

let recordValueSizeMin = 10;
let recordValueSizeMax = 1000;
let recordKeyRange = 100;

// Dynamic canvas height based on content
let canvasHeightDynamic = CANVAS_HEIGHT;

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
let assignmentStrategySelect;

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
  consumerAssignmentStrategy = select('#assignmentStrategySelect');
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

  consumerAssignmentStrategy.changed(handleAssignmentStrategyChange);
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
  globalRecordCounter = 0;
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

  // Calculate the top and bottom Y coordinates of partitions for centering
  const topPartitionY = PARTITION_START_Y;
  const bottomPartitionY = PARTITION_START_Y + (partitionCount - 1) * (PARTITION_HEIGHT + PARTITION_SPACING);

  for (let i = 0; i < producerCount; i++) {
    // Generate a stable color based on index
    const hue = map(i, 0, producerCount, 0, 360);
    const color = colorFromHSB(hue, 70, 90);

    // Initially position producers evenly across the partition range
    const y = map(i, 0, Math.max(1, producerCount - 1),
        topPartitionY + PARTITION_HEIGHT / 2,
        bottomPartitionY + PARTITION_HEIGHT / 2);

    producers.push({
      id: i,
      y: y,
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

  // Adjust producer positions to prevent overlap
  adjustProducerPositions();
}

function initializeConsumers() {
  // Create an empty array for consumers
  consumers = [];

  // If no consumers requested, just return
  if (consumerCount <= 0) return;

  // Get partition assignments using the rebalance algorithm
  let partitionAssignments = rebalanceConsumerGroup(partitionCount, consumerCount, consumerAssignmentStrategy);

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
    if (assignedPartitions.length > 0) {
      for (const partitionId of assignedPartitions) {
        avgY += partitions[partitionId].y + PARTITION_HEIGHT / 2;
      }
      avgY = avgY / assignedPartitions.length;
    } else {
      // Default position for unassigned consumers
      avgY = PARTITION_START_Y + partitionCount * (PARTITION_HEIGHT + PARTITION_SPACING) + 50 + i * 70;
    }

    // Generate a stable color based on index (distinct from producers)
    const hue = map(i, 0, Math.max(1, consumerCount - 1), 180, 360);
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

  // Only adjust positions if we have consumers
  if (consumerCount > 0) {
    adjustConsumerPositions();
  }
}

function adjustConsumerPositions() {
  // If no consumers, just return
  if (consumers.length === 0) return;

  // Keep track of original/ideal positions for each consumer
  const originalPositions = consumers.map(c => c.y);

  // Define minimum spacing between consumer centers
  const MIN_CONSUMER_SPACING = 70;

  // Sort consumers by their assigned position
  consumers.sort((a, b) => a.y - b.y);

  // For unassigned consumers (those with no partitions), distribute them evenly
  const unassignedConsumers = consumers.filter(c => c.assignedPartitions.length === 0);
  if (unassignedConsumers.length > 0) {
    const bottomY = PARTITION_START_Y + partitionCount * (PARTITION_HEIGHT + PARTITION_SPACING) + 50;
    const spacing = MIN_CONSUMER_SPACING;

    for (let i = 0; i < unassignedConsumers.length; i++) {
      unassignedConsumers[i].y = bottomY + i * spacing;
    }

    // Resort consumers by position after adjusting unassigned consumers
    consumers.sort((a, b) => a.y - b.y);
  }

  // Now fix overlaps while trying to keep each consumer as close as possible to its ideal position
  for (let i = 1; i < consumers.length; i++) {
    const prevConsumer = consumers[i-1];
    const currentConsumer = consumers[i];

    // Check if too close to previous consumer
    if (currentConsumer.y - prevConsumer.y < MIN_CONSUMER_SPACING) {
      // Position this consumer below the previous one with minimum spacing
      currentConsumer.y = prevConsumer.y + MIN_CONSUMER_SPACING;
    }
  }

  // Try to maintain assignment-based positioning for consumers with partitions
  // This helps ensure consumers stay aligned with their partitions
  const maxIterations = 3; // Limit optimization attempts
  for (let iter = 0; iter < maxIterations; iter++) {
    let improved = false;

    for (let i = 0; i < consumers.length; i++) {
      const consumer = consumers[i];

      // Only try to adjust consumers with assigned partitions
      if (consumer.assignedPartitions.length > 0) {
        const originalY = originalPositions[i];

        // Check if we can move this consumer closer to its ideal position
        // without violating spacing constraints
        if (i > 0 && i < consumers.length - 1) {
          // Consumer is between others, check both directions
          const minY = consumers[i-1].y + MIN_CONSUMER_SPACING; // Can't go above this
          const maxY = consumers[i+1].y - MIN_CONSUMER_SPACING; // Can't go below this

          if (originalY >= minY && originalY <= maxY) {
            // Can move directly to original position
            if (consumer.y !== originalY) {
              consumer.y = originalY;
              improved = true;
            }
          } else if (originalY < minY && consumer.y > minY) {
            // Try to move up as much as possible
            consumer.y = minY;
            improved = true;
          } else if (originalY > maxY && consumer.y < maxY) {
            // Try to move down as much as possible
            consumer.y = maxY;
            improved = true;
          }
        } else if (i === 0) {
          // First consumer, can only be constrained from below
          const maxY = consumers.length > 1 ? consumers[i+1].y - MIN_CONSUMER_SPACING : Infinity;
          if (originalY <= maxY && consumer.y !== originalY) {
            consumer.y = originalY;
            improved = true;
          }
        } else if (i === consumers.length - 1) {
          // Last consumer, can only be constrained from above
          const minY = consumers[i-1].y + MIN_CONSUMER_SPACING;
          if (originalY >= minY && consumer.y !== originalY) {
            consumer.y = originalY;
            improved = true;
          }
        }
      }
    }

    // If no improvements were made in this iteration, stop
    if (!improved) break;
  }
}

function updateCanvasHeight() {
  const minHeight = 700; // Minimum canvas height

  // Find the lowest element (partition, consumer, or producer)
  let lowestY = PARTITION_START_Y + partitionCount * (PARTITION_HEIGHT + PARTITION_SPACING);

  // Check if any consumers are lower
  for (const consumer of consumers) {
    // Add some margin below the consumer
    const consumerBottom = consumer.y + 50;
    if (consumerBottom > lowestY) {
      lowestY = consumerBottom;
    }
  }

  // Check if any producers are lower
  for (const producer of producers) {
    // Add some margin below the producer
    const producerBottom = producer.y + 50;
    if (producerBottom > lowestY) {
      lowestY = producerBottom;
    }
  }

  // Add some margin at the bottom
  const requiredHeight = lowestY + 100;

  canvasHeightDynamic = max(minHeight, requiredHeight);
  resizeCanvas(CANVAS_WIDTH, canvasHeightDynamic);
}

function handleAssignmentStrategyChange() {
  consumerAssignmentStrategy = assignmentStrategySelect.value();
  // Only update consumers if there are any
  if (consumerCount > 0) {
    updateConsumers();
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
  producerRate = parseInt(produceRateSlider.value());
  recordKeyRange = parseInt(keyRangeSlider.value());
  producerDelayRandomness = parseFloat(produceRandomnessSlider.value());
  consumerProcessingSpeed = parseFloat(processingSpeedSlider.value());

  // Handle value size validation
  let newMinValueSize = parseInt(minValueSizeSlider.value());
  let newMaxValueSize = parseInt(maxValueSizeSlider.value());

  // Ensure max value is always >= min value
  if (newMaxValueSize < newMinValueSize) {
    if (recordValueSizeMin !== newMinValueSize) {
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

  recordValueSizeMin = newMinValueSize;
  recordValueSizeMax = newMaxValueSize;
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

  // Update canvas height in case producers extend beyond current canvas
  updateCanvasHeight();
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

  // Update canvas height in case consumers extend beyond current canvas
  updateCanvasHeight();
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
  const recordSize = random(recordValueSizeMin, recordValueSizeMax);
  const recordRadius = calculateRecordRadius(recordSize);
  const recordKey = int(random(1, recordKeyRange + 1));
  const partitionId = recordKey % partitionCount;
  const recordSpeed = calculateRecordSpeed(recordSize);

  // Create the record object
  const record = {
    id: globalRecordCounter++,
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
  if (recordValueSizeMin >= recordValueSizeMax || Math.abs(recordValueSizeMax - recordValueSizeMin) < 2) {
    return (MIN_RECORD_RADIUS + MAX_RECORD_RADIUS) / 2; // Return average radius
  }

  // Map size to radius logarithmically
  return map(
      log(size),
      log(recordValueSizeMin),
      log(recordValueSizeMax),
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
  const speed = travelDistance / frames * consumerProcessingSpeed;

  return speed;
}

function scheduleNextProduction(producer) {
  // Base time between records in frames (60 frames per second)
  const framesPerRecord = 60 / producerRate;

  // Apply randomness in seconds, capped at 1 second maximum
  let randomTimeFrames = 0;

  // Apply randomness individually for each producer
  // Use the produceRandomness value which is now constrained to 0-1 seconds
  const randomDelaySec = random(0, producerDelayRandomness);
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
      record.progress += 0.05 * consumerProcessingSpeed;

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
      const targetRate = producerRate; // Records per second
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

  // Draw simulation components
  drawPartitions();
  drawProducers();
  drawConsumers();
  drawProducerEffects();

  pop(); // End scrolling transform
}

// Function to prevent producer overlapping
function adjustProducerPositions() {
  // Define minimum spacing between producer centers
  const MIN_PRODUCER_SPACING = 70;

  // Sort producers by their assigned position
  producers.sort((a, b) => a.y - b.y);

  // Now fix overlaps while trying to maintain even distribution
  for (let i = 1; i < producers.length; i++) {
    const prevProducer = producers[i-1];
    const currentProducer = producers[i];

    // Check if too close to previous producer
    if (currentProducer.y - prevProducer.y < MIN_PRODUCER_SPACING) {
      // Position this producer below the previous one with minimum spacing
      currentProducer.y = prevProducer.y + MIN_PRODUCER_SPACING;
    }
  }
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
    // Draw each producer as a component
    drawProducerComponent(producers[i], i);
  }
}

// New component-based function to draw producer and its metrics
function drawProducerComponent(producer, index) {
  push(); // Start a new drawing context
  translate(PRODUCER_X, producer.y); // Set the origin to the producer position

  // Producer metrics data
  const metricsData = [
    `Records: ${producer.recordsProduced}`,
    `Sum B: ${formatBytes(producer.bytesProduced)}`,
    `${Math.round(producer.produceRate)} B/s`,
    `${Math.round(producer.recordsRate * 100) / 100} rec/s`
  ];

  // Calculate metrics box dimensions
  textSize(10);
  const textHeight = 15; // Height per line of text
  const textPadding = 2; // Padding between text and border
  const metricsWidth = max(
      textWidth(metricsData[0]),
      textWidth(metricsData[1]),
      textWidth(metricsData[2]),
      textWidth(metricsData[3])
  ) + textPadding * 2;
  const metricsHeight = textHeight * metricsData.length + textPadding * 2;

  // Draw metrics box - positioned to touch the producer triangle
  noFill();
  stroke(producer.color);
  strokeWeight(1);
  rect(-metricsWidth - 15, -metricsHeight / 2, metricsWidth, metricsHeight);

  // Draw metrics text
  fill(0);
  noStroke();
  textAlign(LEFT, TOP);
  for (let i = 0; i < metricsData.length; i++) {
    text(
        metricsData[i],
        -metricsWidth - 15 + textPadding,
        -metricsHeight / 2 + i * textHeight + textPadding
    );
  }

  // Draw producer symbol (triangle)
  fill(producer.color);
  stroke(0);
  strokeWeight(1);
  triangle(-15, -15, 15, 0, -15, 15);

  // Draw producer ID inside the triangle
  fill(255);
  noStroke();
  textAlign(CENTER, CENTER);
  textSize(10);
  textStyle(BOLD);
  text(index, -10, 0);
  textStyle(NORMAL);

  pop(); // Restore the drawing context
}

function drawConsumers() {
  for (let i = 0; i < consumers.length; i++) {
    const consumer = consumers[i];

    // Draw consumer as a component with its metrics
    drawConsumerComponent(consumer, i);

    // Draw lines between consumer and its assigned partitions
    drawConsumerPartitionConnections(consumer);

    // Draw transit records
    drawTransitRecords(consumer);
  }
}

// Component-based function to draw consumer and its metrics
function drawConsumerComponent(consumer, index) {
  const consumerY = consumer.y;

  push(); // Start a new drawing context
  translate(CONSUMER_X, consumerY); // Set the origin to the consumer position

  // Consumer metrics data
  const metricsData = [
    `Records: ${consumer.recordsConsumed}`,
    `Sum B: ${formatBytes(consumer.bytesConsumed)}`,
    `${Math.round(consumer.consumeRate)} B/s`,
    `${Math.round(consumer.recordsRate * 100) / 100} rec/s`
  ];

  // Calculate metrics box dimensions
  textSize(10);
  const textHeight = 15; // Height per line of text
  const textPadding = 2; // Padding between text and border
  const metricsWidth = max(
      textWidth(metricsData[0]),
      textWidth(metricsData[1]),
      textWidth(metricsData[2]),
      textWidth(metricsData[3])
  ) + textPadding * 2;
  const metricsHeight = textHeight * metricsData.length + textPadding * 2;

  // Use gray color for unassigned consumers
  const borderColor = consumer.assignedPartitions.length === 0 ? color(200) : consumer.color;

  // Draw metrics box - vertically centered with the consumer square
  noFill();
  stroke(borderColor);
  strokeWeight(1);
  rect(30, -metricsHeight/2, metricsWidth, metricsHeight);

  // Draw metrics text
  fill(0);
  noStroke();
  textAlign(LEFT, TOP);
  for (let i = 0; i < metricsData.length; i++) {
    text(metricsData[i], 30 + textPadding, -metricsHeight/2 + textPadding + i * textHeight);
  }

  // Draw consumer rectangle
  fill(consumer.color);
  stroke(0);
  strokeWeight(1);
  rect(0, -15, 30, 30);

  // Draw consumer ID inside rectangle
  fill(255);
  noStroke();
  textAlign(CENTER, CENTER);
  textStyle(BOLD);
  text(index, 15, 0);
  textStyle(NORMAL);

  pop(); // Restore the drawing context
}

// Draw lines between consumer and partitions
function drawConsumerPartitionConnections(consumer) {
  stroke(consumer.color);
  strokeWeight(1.8);
  drawingContext.setLineDash([5, 5]);

  for (const partitionId of consumer.assignedPartitions) {
    const partitionY = partitions[partitionId].y + PARTITION_HEIGHT / 2;
    line(PARTITION_START_X + PARTITION_WIDTH, partitionY, CONSUMER_X, consumer.y);
  }

  drawingContext.setLineDash([]);
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

function rebalanceConsumerGroup(partitions, consumerCount, strategy = assignmentStrategy) {
  // Array to store partition assignments (which consumer owns which partition)
  let assignments = new Array(partitions).fill(-1);

  if (consumerCount <= 0) return assignments;

  switch (strategy) {
    case 'range':
      // Range strategy: divide partitions into ranges and assign each range to a consumer
      // This is similar to Kafka's default RangeAssignor
      const partitionsPerConsumer = Math.floor(partitions / consumerCount);
      const remainder = partitions % consumerCount;

      let startIndex = 0;
      for (let i = 0; i < consumerCount; i++) {
        // Calculate how many partitions this consumer gets
        const numPartitions = partitionsPerConsumer + (i < remainder ? 1 : 0);

        // Assign this range of partitions to the consumer
        for (let j = 0; j < numPartitions; j++) {
          if (startIndex + j < partitions) {
            assignments[startIndex + j] = i;
          }
        }

        startIndex += numPartitions;
      }
      break;

    case 'sticky':
      // Sticky strategy: attempt to maintain previous assignments when possible
      // In this simplified version, we distribute partitions evenly
      // but try to maintain a locality pattern (adjacent partitions)
      const partitionsPerConsumerSticky = Math.ceil(partitions / consumerCount);

      for (let i = 0; i < partitions; i++) {
        const consumerId = Math.floor(i / partitionsPerConsumerSticky);
        assignments[i] = consumerId < consumerCount ? consumerId : consumerCount - 1;
      }
      break;

    case 'cooperative-sticky':
      // Cooperative sticky strategy: similar to sticky but models the cooperative rebalancing
      // In a real implementation, this would be more complex with phased reassignments
      // For simulation, we'll create a balanced but slightly clustered distribution

      // First, do round-robin assignment
      for (let i = 0; i < partitions; i++) {
        assignments[i] = i % consumerCount;
      }

      // Then, adjust to create some locality clustering
      // This simulates the "stickiness" aspect
      if (partitions >= consumerCount * 2) {
        for (let c = 0; c < consumerCount; c++) {
          // Try to give each consumer a small cluster of partitions
          const clusterSize = Math.floor(partitions / consumerCount / 2);
          const startPos = c * clusterSize;

          for (let i = 0; i < clusterSize && startPos + i < partitions; i++) {
            // Only reassign if it doesn't create too much imbalance
            const currentOwner = assignments[startPos + i];
            if (currentOwner !== c) {
              // Count partitions owned by each consumer
              let consumerPartitionCounts = new Array(consumerCount).fill(0);
              for (let j = 0; j < partitions; j++) {
                consumerPartitionCounts[assignments[j]]++;
              }

              // Only reassign if it doesn't create too much imbalance
              if (consumerPartitionCounts[currentOwner] > consumerPartitionCounts[c]) {
                assignments[startPos + i] = c;
              }
            }
          }
        }
      }
      break;

    case 'round-robin':
    default:
      // Round-robin strategy: distribute partitions evenly across consumers
      // Similar to Kafka's RoundRobinAssignor
      for (let i = 0; i < partitions; i++) {
        assignments[i] = i % consumerCount;
      }
      break;
  }

  return assignments;
}

function draw() {
  background(240);

  // Handle events
  handleControlChanges();

  // Update simulation
  updateSimulation();

  // Render simulation
  renderSimulation();
}