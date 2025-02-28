// ------ Canvas, UI and Animations ------
const CANVAS_WIDTH = 1100;
const CANVAS_HEIGHT = 700;
const CANVAS_PARTITION_WIDTH = 400;
const CANVAS_PARTITION_HEIGHT = 30;
const CANVAS_PARTITION_HEIGHT_SPACING = 20;
const CANVAS_PARTITION_START_X = 200;
const CANVAS_PARTITION_START_Y = 120;
const CANVAS_PRODUCER_POSITION_X = 120;
const CANVAS_CONSUMER_POSITION_X = CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH + 50;
const CANVAS_RECORD_RADIUS_MAX = 15;
const CANVAS_RECORD_RADIUS_MIN = 5;
const ANIMATION_RECORD_SPEED = 2.0; // Visual speed for record movement (doesn't affect processing)
const ANIMATION_PRODUCER_LINE_DURATION = 400;

// Dynamic canvas height based on content
let canvasHeightDynamic = CANVAS_HEIGHT;

// Producer
let partitionCount = 8;
let producerCount = 2;
let producerRate = 1;
let producerDelayRandomFactor = 0.2; // randomly delays records between 0 and set value, in seconds

// Consumer
let consumerCount = 2;
let consumerRecordTransitSpeedAccelerator = 1.0; // boosts consumer speed by this factor
let consumerAssignmentStrategy = 'round-robin';
let consumerThroughputMaxInBytes = 5000; // Bytes per second processing capacity

// Record
let recordIDIncrementCounter = 0; // Counter for unique record IDs
let recordValueSizeMin = 10;
let recordValueSizeMax = 1000;
let recordKeyRange = 10;

// Runtime Data structures
let partitions = [];
let producers = [];
let consumers = [];
let producerEffects = [];

// Metrics tracking with last-updated timestamps
let metrics = {
  startTime: 0,
  lastUpdateTime: 0,
  producers: {},  // Map of producer ID -> metrics
  consumers: {},  // Map of consumer ID -> metrics
  global: {
    totalRecordsProduced: 0,
    totalRecordsConsumed: 0,
    totalBytesProduced: 0,
    totalBytesConsumed: 0,
    avgProcessingTimeMs: 0,
    processingTimeSamples: 0
  }
};

// UI Controls (now referencing HTML elements)
let partitionSlider, producerSlider, consumerSlider;
let produceRateSlider, minValueSizeSlider, maxValueSizeSlider;
let keyRangeSlider, produceRandomnessSlider, processingSpeedSlider;
let partitionInput, producerInput, consumerInput;
let produceRateInput, minValueSizeInput, maxValueSizeInput;
let keyRangeInput, produceRandomnessInput, processingSpeedInput;
let assignmentStrategySelect;
let processingCapacitySlider, processingCapacityInput;

// ------ EVENT SYSTEM ------
// Event types for the reactive system
const EVENTS = {
  RECORD_PRODUCED: 'record_produced',
  RECORD_REACHED_PARTITION_END: 'record_reached_partition_end',
  RECORD_PROCESSING_STARTED: 'record_processing_started',
  RECORD_PROCESSING_COMPLETED: 'record_processing_completed',
  CONSUMER_THROUGHPUT_UPDATED: 'capacity_changed',
  METRICS_UPDATE: 'metrics_update'
};

// Simple event emitter
class EventEmitter {
  constructor() {
    this.events = {};
  }

  on(event, callback) {
    if (!this.events[event]) {
      this.events[event] = [];
    }
    this.events[event].push(callback);
    return this; // For chaining
  }

  emit(event, data) {
    if (!this.events[event]) return;
    this.events[event].forEach(callback => callback(data));
  }
}

// Global event emitter
const eventEmitter = new EventEmitter();

// ------ SETUP & INITIALIZATION ------
function setup() { // Usage by p5.js
  // Create canvas and add it to the container div
  let canvas = createCanvas(CANVAS_WIDTH, canvasHeightDynamic);
  canvas.parent('canvas-container');

  metrics.startTime = millis();
  metrics.lastUpdateTime = metrics.startTime;

  // Get references to HTML controls
  setupControlReferences();
  attachControlEventListeners();

  // Set up event handlers
  setupEventHandlers();

  initializeState();
}

function setupEventHandlers() {
  // Set up reactive event handlers for metrics
  eventEmitter.on(EVENTS.RECORD_PRODUCED, (data) => {
    // Update producer metrics reactively
    if (!metrics.producers[data.producerId]) {
      metrics.producers[data.producerId] = {
        recordsProduced: 0,
        bytesProduced: 0,
        produceRate: 0,
        recordsRate: 0,
        lastUpdateTime: millis()
      };
    }

    metrics.producers[data.producerId].recordsProduced++;
    metrics.producers[data.producerId].bytesProduced += data.value;
    metrics.global.totalRecordsProduced++;
    metrics.global.totalBytesProduced += data.value;

    // Calculate rate based on time since last update
    const now = millis();
    const elapsed = (now - metrics.producers[data.producerId].lastUpdateTime) / 1000;
    if (elapsed > 0.1) { // Only update rate if enough time has passed
      metrics.producers[data.producerId].produceRate = data.value / elapsed;
      metrics.producers[data.producerId].recordsRate = 1 / elapsed;
      metrics.producers[data.producerId].lastUpdateTime = now;
    }
  });

  eventEmitter.on(EVENTS.RECORD_PROCESSING_COMPLETED, (data) => {
    // Update consumer metrics reactively
    if (!metrics.consumers[data.consumerId]) {
      metrics.consumers[data.consumerId] = {
        recordsConsumed: 0,
        bytesConsumed: 0,
        consumeRate: 0,
        recordsRate: 0,
        lastUpdateTime: millis(),
        processingTimes: []
      };
    }

    metrics.consumers[data.consumerId].recordsConsumed++;
    metrics.consumers[data.consumerId].bytesConsumed += data.value;
    metrics.global.totalRecordsConsumed++;
    metrics.global.totalBytesConsumed += data.value;

    // Track processing time for this record
    metrics.consumers[data.consumerId].processingTimes.push(data.processingTimeMs);
    // Keep only the last 10 processing times
    if (metrics.consumers[data.consumerId].processingTimes.length > 10) {
      metrics.consumers[data.consumerId].processingTimes.shift();
    }

    // Update global average processing time
    metrics.global.avgProcessingTimeMs =
        (metrics.global.avgProcessingTimeMs * metrics.global.processingTimeSamples + data.processingTimeMs) /
        (metrics.global.processingTimeSamples + 1);
    metrics.global.processingTimeSamples++;

    // Calculate rate based on time since last update
    const now = millis();
    const elapsed = (now - metrics.consumers[data.consumerId].lastUpdateTime) / 1000;
    if (elapsed > 0.1) { // Only update rate if enough time has passed
      metrics.consumers[data.consumerId].consumeRate = data.value / elapsed;
      metrics.consumers[data.consumerId].recordsRate = 1 / elapsed;
      metrics.consumers[data.consumerId].lastUpdateTime = now;
    }
  });

  eventEmitter.on(EVENTS.CONSUMER_THROUGHPUT_UPDATED, (data) => {
    // When capacity changes, recalculate processing times for all active records
    for (const consumer of consumers) {
      if (!consumer.activePartitions) continue;

      for (const partitionId in consumer.activePartitions) {
        recalculateProcessingTime(consumer, partitionId, millis());
      }
    }
  });
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
  processingCapacitySlider = select('#processingCapacitySlider');

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
  processingCapacityInput = select('#processingCapacityInput');
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
  processingCapacitySlider.input(() => {
    handleSliderInput(processingCapacitySlider, processingCapacityInput, 'capacity');
    // Emit event for capacity change
    eventEmitter.emit(EVENTS.CONSUMER_THROUGHPUT_UPDATED, {
      value: parseInt(processingCapacitySlider.value())
    });
  });

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
  processingCapacityInput.input(() => {
    handleTextInput(processingCapacityInput, processingCapacitySlider, 'capacity');
    // Emit event for capacity change
    eventEmitter.emit(EVENTS.CONSUMER_THROUGHPUT_UPDATED, {
      value: parseInt(processingCapacitySlider.value())
    });
  });

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
  recordIDIncrementCounter = 0;
  producerEffects = [];

  // Reset metrics
  metrics = {
    startTime: millis(),
    lastUpdateTime: millis(),
    producers: {},
    consumers: {},
    global: {
      totalRecordsProduced: 0,
      totalRecordsConsumed: 0,
      totalBytesProduced: 0,
      totalBytesConsumed: 0,
      avgProcessingTimeMs: 0,
      processingTimeSamples: 0
    }
  };

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
      y: CANVAS_PARTITION_START_Y + i * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING),
      records: []
    });
  }
}

function initializeProducers() {
  producers = [];

  // Calculate the top and bottom Y coordinates of partitions for centering
  const topPartitionY = CANVAS_PARTITION_START_Y;
  const bottomPartitionY = CANVAS_PARTITION_START_Y + (partitionCount - 1) * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING);

  for (let i = 0; i < producerCount; i++) {
    // Generate a stable color based on index
    const hue = map(i, 0, producerCount, 0, 360);
    const color = colorFromHSB(hue, 70, 90);

    // Initially position producers evenly across the partition range
    const y = map(i, 0, Math.max(1, producerCount - 1),
        topPartitionY + CANVAS_PARTITION_HEIGHT / 2,
        bottomPartitionY + CANVAS_PARTITION_HEIGHT / 2);

    producers.push({
      id: i,
      y: y,
      color: color,
      nextProduceTime: frameCount + i * 10 // Stagger initial production
    });

    // Initialize producer metrics
    metrics.producers[i] = {
      recordsProduced: 0,
      bytesProduced: 0,
      produceRate: 0,
      recordsRate: 0,
      lastUpdateTime: millis()
    };
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
        avgY += partitions[partitionId].y + CANVAS_PARTITION_HEIGHT / 2;
      }
      avgY = avgY / assignedPartitions.length;
    } else {
      // Default position for unassigned consumers
      avgY = CANVAS_PARTITION_START_Y + partitionCount * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING) + 50 + i * 70;
    }

    // Generate a stable color based on index (distinct from producers)
    const hue = map(i, 0, Math.max(1, consumerCount - 1), 180, 360);
    const color = colorFromHSB(hue, 70, 80);

    consumers.push({
      id: i,
      y: avgY,
      color: color,
      assignedPartitions: assignedPartitions,
      // Structure for concurrent processing
      activePartitions: {}, // Map of partitionId -> record being processed
      processingTimes: {}, // Map of recordId -> {startTime, endTime}
      capacity: consumerThroughputMaxInBytes, // Bytes per second this consumer can process
      processingQueues: {}, // Map of partitionId -> queue of records waiting
      transitRecords: []
    });

    // Initialize consumer metrics
    metrics.consumers[i] = {
      recordsConsumed: 0,
      bytesConsumed: 0,
      consumeRate: 0,
      recordsRate: 0,
      lastUpdateTime: millis(),
      processingTimes: []
    };

    // Initialize processing queues for each assigned partition
    for (const partitionId of assignedPartitions) {
      consumers[i].processingQueues[partitionId] = [];
    }
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
    const bottomY = CANVAS_PARTITION_START_Y + partitionCount * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING) + 50;
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

function updateCanvasHeight() {
  const minHeight = 700; // Minimum canvas height

  // Find the lowest element (partition, consumer, or producer)
  let lowestY = CANVAS_PARTITION_START_Y + partitionCount * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING);

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
  producerDelayRandomFactor = parseFloat(produceRandomnessSlider.value());
  consumerRecordTransitSpeedAccelerator = parseFloat(processingSpeedSlider.value());

  if (processingCapacitySlider) {
    const newCapacity = parseInt(processingCapacitySlider.value());
    if (newCapacity !== consumerThroughputMaxInBytes) {
      consumerThroughputMaxInBytes = newCapacity;
      eventEmitter.emit(EVENTS.CONSUMER_THROUGHPUT_UPDATED, { value: consumerThroughputMaxInBytes });
    }
  }

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
  // Save old metrics
  const oldMetrics = {...metrics.producers};

  // Initialize new producers
  initializeProducers();

  // Restore metrics where applicable
  for (let i = 0; i < producerCount && i < Object.keys(oldMetrics).length; i++) {
    if (oldMetrics[i]) {
      metrics.producers[i] = oldMetrics[i];
    }
  }

  // Update canvas height in case producers extend beyond current canvas
  updateCanvasHeight();
}

function updateConsumers() {
  // Save old metrics and processing state
  const oldMetrics = {...metrics.consumers};
  const oldConsumers = [...consumers];

  // Initialize new consumers
  initializeConsumers();

  // Restore metrics where applicable
  for (let i = 0; i < consumerCount && i < Object.keys(oldMetrics).length; i++) {
    if (oldMetrics[i]) {
      metrics.consumers[i] = oldMetrics[i];
    }
  }

  // Restore processing state for assigned partitions
  for (let i = 0; i < consumerCount && i < oldConsumers.length; i++) {
    // Copy transit records for partitions still assigned
    consumers[i].transitRecords = oldConsumers[i].transitRecords?.filter(record => {
      return consumers[i].assignedPartitions.includes(parseInt(record.partitionId));
    }) || [];

    // Copy active partitions and processing queues
    if (oldConsumers[i].activePartitions) {
      for (const partitionId in oldConsumers[i].activePartitions) {
        if (consumers[i].assignedPartitions.includes(parseInt(partitionId))) {
          // Copy active record
          const record = oldConsumers[i].activePartitions[partitionId];
          if (record) {
            consumers[i].activePartitions[partitionId] = record;

            // Copy processing times
            if (oldConsumers[i].processingTimes && oldConsumers[i].processingTimes[record.id]) {
              consumers[i].processingTimes[record.id] = oldConsumers[i].processingTimes[record.id];
            }
          }
        }
      }
    }

    // Copy processing queues
    if (oldConsumers[i].processingQueues) {
      for (const partitionId in oldConsumers[i].processingQueues) {
        if (consumers[i].assignedPartitions.includes(parseInt(partitionId))) {
          consumers[i].processingQueues[partitionId] = oldConsumers[i].processingQueues[partitionId] || [];
        }
      }
    }
  }

  // Update canvas height
  updateCanvasHeight();
}

function updateSimulation() {
  // In p5.js, this is called every frame (60 times per second)
  produceRecords();
  updateRecordPositions();
  consumeRecords();
  updateTransitRecords();
}

// ------ BUSINESS LOGIC ------
function produceRecords() {
  // Process producer effects first
  updateProducerEffects();

  // Check for new records to produce
  for (const producer of producers) {
    if (frameCount >= producer.nextProduceTime) {
      // Create and add a new record
      createAndEmitRecord(producer);

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

function createAndEmitRecord(producer) {
  // Generate record characteristics
  const recordSize = random(recordValueSizeMin, recordValueSizeMax);
  const recordRadius = calculateRecordRadius(recordSize);
  const recordKey = int(random(1, recordKeyRange + 1));
  const partitionId = recordKey % partitionCount;
  const recordSpeed = calculateRecordSpeed(recordSize);

  // Create the record object
  const record = {
    id: recordIDIncrementCounter++,
    key: recordKey,
    value: recordSize,
    radius: recordRadius,
    producerId: producer.id,
    partitionId: partitionId,
    speed: recordSpeed,

    // UI
    x: CANVAS_PARTITION_START_X + recordRadius, // Start position based on Partition
    color: producer.color,

    // State flags for UI
    isBeingProcessed: false,
    isWaiting: false,
    isProcessed: false
  };

  // Add the record to the partition
  partitions[partitionId].records.push(record);

  // Emit record produced event
  eventEmitter.emit(EVENTS.RECORD_PRODUCED, record);

  // Add visual effect for production
  addProducerLineToPartitionEffect(producer, partitionId);

  // Log record production to console
  console.log(`Record produced: {"id": ${record.id}, "key": ${record.key}, "valueBytes": ${Math.round(recordSize)}, "partition": ${partitionId}, "producer": ${producer.id}}`);
}

function calculateRecordRadius(size) {
  // Handle edge case when min and max are too close
  if (recordValueSizeMin >= recordValueSizeMax || Math.abs(recordValueSizeMax - recordValueSizeMin) < 2) {
    return (CANVAS_RECORD_RADIUS_MIN + CANVAS_RECORD_RADIUS_MAX) / 2; // Return average radius
  }

  // Map size to radius logarithmically
  return map(
      log(size),
      log(recordValueSizeMin),
      log(recordValueSizeMax),
      CANVAS_RECORD_RADIUS_MIN,
      CANVAS_RECORD_RADIUS_MAX
  );
}

// Animation speed is now separated from processing speed
function calculateRecordSpeed(size) {
  // Now speed is just for animation
  const baseSpeed = ANIMATION_RECORD_SPEED;
  const radius = calculateRecordRadius(size);

  // Smaller records move slightly faster for visual variety
  const adjustedSpeed = baseSpeed * (1 - (radius - CANVAS_RECORD_RADIUS_MIN) / (CANVAS_RECORD_RADIUS_MAX - CANVAS_RECORD_RADIUS_MIN) * 0.3);
  return adjustedSpeed;
}

function scheduleNextProduction(producer) {
  // Base time between records in frames (60 frames per second)
  const framesPerRecord = 60 / producerRate;

  // Apply random delay in seconds
  let randomTimeFrames = 0;
  const randomDelaySec = random(0, producerDelayRandomFactor);
  randomTimeFrames = randomDelaySec * 60;

  // Set next production time
  producer.nextProduceTime = frameCount + int(framesPerRecord + randomTimeFrames);
}

function addProducerLineToPartitionEffect(producer, partitionId) {
  // Create a visual effect line from producer to partition
  const effect = {
    startX: CANVAS_PRODUCER_POSITION_X + 15,
    startY: producer.y,
    endX: CANVAS_PARTITION_START_X,
    endY: partitions[partitionId].y + CANVAS_PARTITION_HEIGHT / 2,
    color: producer.color,
    endTime: millis() + ANIMATION_PRODUCER_LINE_DURATION
  };

  producerEffects.push(effect);
}

function updateRecordPositions() {
  // Update record positions within each partition
  for (const partition of partitions) {
    // First pass: Process records in FIFO order (oldest first)
    // This makes sure older records move first
    for (let i = 0; i < partition.records.length; i++) {
      const record = partition.records[i];

      // Check if we're at the front of the line
      if (i === 0) {
        // First record can always move forward up to the end
        const maxX = CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH - record.radius;

        // Move record forward
        const newX = min(record.x + record.speed, maxX);

        // If record reaches the end of the partition, emit an event
        if (record.x < maxX && newX >= maxX) {
          eventEmitter.emit(EVENTS.RECORD_REACHED_PARTITION_END, {
            recordId: record.id,
            partitionId: partition.id
          });
        }

        record.x = newX;
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
    partition.records.sort((a, b) => b.x - a.x);

    // Third pass: If there are records at the end, adjust their positions to prevent overflow
    let endRecords = partition.records.filter(r =>
        r.x >= CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH - r.radius - 2);

    if (endRecords.length > 1) {
      // Calculate a nice stacking pattern
      for (let i = 0; i < endRecords.length; i++) {
        const record = endRecords[i];
        // Position in a staggered pattern at the end
        const offset = i * 5; // Slight offset for each record
        record.x = CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH - record.radius - 2 - offset;
      }
    }
  }
}

function consumeRecords() {
  const currentTime = millis();

  for (const consumer of consumers) {
    // Update consumer capacity from slider in real-time
    if (processingCapacitySlider) {
      const newCapacity = parseInt(processingCapacitySlider.value());
      if (consumer.capacity !== newCapacity) {
        consumer.capacity = newCapacity;
      }
    }

    // Ensure we have the necessary data structures
    if (!consumer.activePartitions) consumer.activePartitions = {};
    if (!consumer.processingTimes) consumer.processingTimes = {};
    if (!consumer.processingQueues) consumer.processingQueues = {};

    // Check all active partitions for completed records
    const activePartitionIds = Object.keys(consumer.activePartitions);

    for (const partitionId of activePartitionIds) {
      // Skip if the active partition entry is invalid
      const record = consumer.activePartitions[partitionId];
      if (!record) continue;

      // Get processing info for this record
      const processingInfo = consumer.processingTimes[record.id];
      if (!processingInfo) continue;

      // Check if the record has completed processing
      if (currentTime >= processingInfo.endTime) {
        // Record is finished, remove it from active partitions
        const finishedRecord = {...consumer.activePartitions[partitionId]};
        delete consumer.activePartitions[partitionId];

        // Calculate actual processing time
        const actualTime = currentTime - processingInfo.startTime;
        delete consumer.processingTimes[record.id];

        // Mark record as processed
        finishedRecord.isBeingProcessed = false;
        finishedRecord.isProcessed = true;

        // Emit completion event with processing metrics
        eventEmitter.emit(EVENTS.RECORD_PROCESSING_COMPLETED, {
          ...finishedRecord,
          consumerId: consumer.id,
          processingTimeMs: actualTime
        });

        // If there are more records in the queue for this partition, process the next one
        if (consumer.processingQueues[partitionId] && consumer.processingQueues[partitionId].length > 0) {
          const nextRecord = consumer.processingQueues[partitionId].shift();
          startProcessingRecord(consumer, nextRecord, partitionId, currentTime);
        }
      } else {
        // Record still processing, recalculate end time based on current capacity
        recalculateProcessingTime(consumer, partitionId, currentTime);
      }
    }

    // Check all assigned partitions for new records that have reached the end
    for (const partitionId of consumer.assignedPartitions) {
      // Skip this partition if it's already processing a record
      if (consumer.activePartitions[partitionId]) continue;

      // Check if there's a record at the end of this partition
      const partition = partitions[partitionId];
      if (partition && partition.records.length > 0) {
        const firstRecord = partition.records[0];

        // Check if record has reached the end of the partition
        if (firstRecord.x >= CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH - firstRecord.radius - 2) {
          // Remove record from partition
          const record = partition.records.shift();

          // Start processing this record
          startProcessingRecord(consumer, record, partitionId, currentTime);

          // Create visual transit path
          transferRecordToConsumer(consumer, record, partitionId);
        }
      }
    }
  }
}

// Fixed function for starting record processing
function startProcessingRecord(consumer, record, partitionId, currentTime) {
  // Ensure necessary data structures exist
  if (!consumer.activePartitions) consumer.activePartitions = {};
  if (!consumer.processingTimes) consumer.processingTimes = {};

  // Add record to active partitions
  consumer.activePartitions[partitionId] = record;

  // Get count of active partitions
  const activeCount = Object.keys(consumer.activePartitions).length;

  // Calculate effective capacity per partition
  const effectiveCapacity = consumer.capacity / activeCount;

  // Calculate processing time
  const processingTimeMs = (record.value / effectiveCapacity) * 1000;

  // Record processing start/end times
  consumer.processingTimes[record.id] = {
    startTime: currentTime,
    endTime: currentTime + processingTimeMs,
    partitionId: partitionId
  };

  // Update record state
  record.isBeingProcessed = true;
  record.isWaiting = false;
  record.processingTimeMs = processingTimeMs; // Expected time

  // Emit event for processing start
  eventEmitter.emit(EVENTS.RECORD_PROCESSING_STARTED, {
    ...record,
    consumerId: consumer.id,
    estimatedTimeMs: processingTimeMs
  });

  // Log processing start
  console.log(`Record processing started: {"id": ${record.id}, "key": ${record.key}, "valueBytes": ${Math.round(record.value)}, "partition": ${partitionId}, "consumer": ${consumer.id}, "estimatedTimeMs": ${Math.round(processingTimeMs)}}`);

  // Recalculate processing times for all other active records
  for (const pid of Object.keys(consumer.activePartitions)) {
    if (pid != partitionId && consumer.activePartitions[pid]) {
      recalculateProcessingTime(consumer, pid, currentTime);
    }
  }
}

// Fixed function to avoid negative processing times
function recalculateProcessingTime(consumer, partitionId, currentTime) {
  // Safety checks
  if (!consumer || !consumer.activePartitions) return;

  const record = consumer.activePartitions[partitionId];
  if (!record) return;

  const recordId = record.id;
  if (!consumer.processingTimes) consumer.processingTimes = {};

  const processingInfo = consumer.processingTimes[recordId];
  if (!processingInfo) return;

  // Calculate how many partitions are active for this consumer
  const activeCount = Object.keys(consumer.activePartitions).length;
  if (activeCount === 0) return;

  // Calculate effective capacity for this partition
  const effectiveCapacity = consumer.capacity / activeCount;

  // Calculate new total processing time based on current capacity
  const totalProcessingTime = (record.value / effectiveCapacity) * 1000;

  // How much time has already elapsed
  const elapsedTime = currentTime - processingInfo.startTime;

  // Calculate progress as a fraction (capped at 1.0)
  const progress = Math.min(elapsedTime / totalProcessingTime, 0.99);

  // Calculate remaining time based on current capacity
  const remainingTime = Math.max(10, totalProcessingTime * (1 - progress));

  // Update the end time
  processingInfo.endTime = currentTime + remainingTime;

  // Update processing progress for visualization
  if (record.isBeingProcessed) {
    record.processingProgress = progress;
  }
}

// Create a transit path for a record from partition to consumer
function transferRecordToConsumer(consumer, record, partitionId, isWaiting = false) {
  // Calculate start and end points for transit
  const startX = CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH;
  const startY = partitions[partitionId].y + CANVAS_PARTITION_HEIGHT / 2;
  const endX = CANVAS_CONSUMER_POSITION_X;
  const endY = consumer.y;

  // Make sure transitRecords exists
  if (!consumer.transitRecords) consumer.transitRecords = [];

  // Add to transit records with path information
  consumer.transitRecords.push({
    ...record,
    x: startX,
    y: startY,
    startX: startX,
    startY: startY,
    endX: endX,
    endY: endY,
    progress: 0,
    isWaiting: isWaiting, // Flag to indicate if record is waiting to be processed
    isBeingProcessed: !isWaiting // Will be true if not waiting
  });
}

// Handle record movement after reaching consumer
function updateTransitRecords() {
  const currentTime = millis();

  for (const consumer of consumers) {
    // Skip if transitRecords isn't initialized
    if (!consumer.transitRecords) continue;

    for (let i = consumer.transitRecords.length - 1; i >= 0; i--) {
      const record = consumer.transitRecords[i];

      // Skip processed records - they will be removed
      if (record.isProcessed) {
        consumer.transitRecords.splice(i, 1);
        continue;
      }

      // Check if this record is currently being processed
      const partitionId = record.partitionId;
      const isBeingProcessed = consumer.activePartitions &&
          consumer.activePartitions[partitionId] &&
          consumer.activePartitions[partitionId].id === record.id;

      // Update record state based on actual processing state
      record.isBeingProcessed = isBeingProcessed;

      // Get progress for records being processed
      if (isBeingProcessed && consumer.processingTimes && consumer.processingTimes[record.id]) {
        const info = consumer.processingTimes[record.id];
        const processingProgress = (currentTime - info.startTime) / (info.endTime - info.startTime);
        record.processingProgress = Math.min(processingProgress, 1.0);
      }

      // Update position for records in transit (not waiting or being processed)
      if (!record.isWaiting && !record.isBeingProcessed) {
        record.progress += 0.05 * consumerRecordTransitSpeedAccelerator;

        // Calculate new position along the path
        record.x = lerp(record.startX, record.endX, record.progress);
        record.y = lerp(record.startY, record.endY, record.progress);

        // If record has reached the consumer, mark it as waiting
        if (record.progress >= 1.0) {
          record.x = record.endX;
          record.y = record.endY;
          record.isWaiting = true;
        }
      }

      // Position waiting records in a queue near the consumer
      if (record.isWaiting) {
        // Find position in queue for this partition
        const queueIndex = consumer.processingQueues[partitionId] ?
            consumer.processingQueues[partitionId].findIndex(r => r && r.id === record.id) : -1;

        if (queueIndex !== -1) {
          // Position in a queue near the consumer, organized by partition
          const partitionOffset = consumer.assignedPartitions.indexOf(parseInt(partitionId)) * 25;
          record.x = CANVAS_CONSUMER_POSITION_X - 40;
          record.y = consumer.y + partitionOffset + (queueIndex + 1) * 15;
        }
      }
    }
  }
}

// ------ RENDERING ------
function renderSimulation() {
  push();

  // Draw simulation components
  drawPartitions();
  drawProducers();
  drawConsumers();
  drawProducerEffects();
  drawMetricsPanel();

  pop();
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

    // Draw partition rectangle
    rect(CANVAS_PARTITION_START_X, partition.y, CANVAS_PARTITION_WIDTH, CANVAS_PARTITION_HEIGHT);
    pop();

    // Draw partition label
    fill(0);
    noStroke();
    textAlign(RIGHT, CENTER);
    textSize(12);
    text(`P${i}`, CANVAS_PARTITION_START_X - 10, partition.y + CANVAS_PARTITION_HEIGHT / 2);

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
    ellipse(record.x, partition.y + CANVAS_PARTITION_HEIGHT / 2, record.radius * 2, record.radius * 2);

    // Draw record key inside if large enough
    if (record.radius > 8) {
      fill(255);
      noStroke();
      textAlign(CENTER, CENTER);
      textSize(10);
      text(record.key, record.x, partition.y + CANVAS_PARTITION_HEIGHT / 2);
    }
  }
}

function drawProducers() {
  for (let i = 0; i < producers.length; i++) {
    drawProducerComponent(producers[i], i);
  }
}

function drawProducerComponent(producer, index) {
  push(); // Start a new drawing context
  translate(CANVAS_PRODUCER_POSITION_X, producer.y); // Set the origin to the producer position

  // Get metrics for this producer
  const producerMetrics = metrics.producers[producer.id] || {
    recordsProduced: 0,
    bytesProduced: 0,
    produceRate: 0,
    recordsRate: 0
  };

  // Producer metrics data
  const metricsData = [
    `Records: ${producerMetrics.recordsProduced}`,
    `Sum B: ${formatBytes(producerMetrics.bytesProduced)}`,
    `${Math.round(producerMetrics.produceRate)} B/s`,
    `${Math.round(producerMetrics.recordsRate * 100) / 100} rec/s`
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

function drawConsumerComponent(consumer, index) {
  const consumerY = consumer.y;

  push(); // Start a new drawing context
  translate(CANVAS_CONSUMER_POSITION_X, consumerY); // Set the origin to the consumer position

  // Get metrics for this consumer
  const consumerMetrics = metrics.consumers[consumer.id] || {
    recordsConsumed: 0,
    bytesConsumed: 0,
    consumeRate: 0,
    recordsRate: 0
  };

  // Consumer metrics data
  const metricsData = [
    `Records: ${consumerMetrics.recordsConsumed}`,
    `Sum B: ${formatBytes(consumerMetrics.bytesConsumed)}`,
    `${Math.round(consumerMetrics.consumeRate)} B/s`,
    `${Math.round(consumerMetrics.recordsRate * 100) / 100} rec/s`
  ];

  // Calculate metrics box dimensions
  textSize(10);
  const textHeight = 15; // Height per line of text
  const textPadding = 2; // Padding between text and border
  const metricsWidth = max(
      ...metricsData.map(text => textWidth(text))
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

  // Draw consumer rectangle - always use regular color regardless of busy state
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

  // Draw the number of active partitions for debugging
  const activeCount = consumer.activePartitions ? Object.keys(consumer.activePartitions).length : 0;
  if (activeCount > 0) {
    fill(255, 0, 0);
    noStroke();
    textAlign(CENTER, CENTER);
    textSize(9);
    text(`${activeCount}`, 5, -10);
  }

  pop(); // Restore the drawing context
}

function drawConsumerPartitionConnections(consumer) {
  stroke(consumer.color);
  strokeWeight(1.8);
  drawingContext.setLineDash([5, 5]);

  for (const partitionId of consumer.assignedPartitions) {
    const partitionY = partitions[partitionId].y + CANVAS_PARTITION_HEIGHT / 2;
    line(CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH, partitionY, CANVAS_CONSUMER_POSITION_X, consumer.y);
  }

  drawingContext.setLineDash([]);
}

function drawTransitRecords(consumer) {
  const currentTime = millis();

  // Skip if transitRecords isn't initialized
  if (!consumer.transitRecords) return;

  for (const record of consumer.transitRecords) {
    // Skip processed records
    if (record.isProcessed) continue;

    // Change color based on record state
    if (record.isBeingProcessed) {
      // Use a pulsing effect for active processing
      const pulseFreq = 0.1;
      const pulseAmount = (sin(frameCount * pulseFreq) * 0.3) + 0.7;
      const c = color(record.color);
      c.setAlpha(pulseAmount * 255);
      fill(c);
    } else if (record.isWaiting) {
      // Waiting records are more transparent
      const c = color(record.color);
      c.setAlpha(180);
      fill(c);
    } else {
      // Normal records
      fill(record.color);
    }

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

    // For records being processed, show a progress indicator
    if (record.isBeingProcessed && record.processingProgress !== undefined) {
      noFill();
      stroke(0, 255, 0);
      strokeWeight(2);
      arc(record.x, record.y, record.radius * 2.5, record.radius * 2.5,
          -HALF_PI, -HALF_PI + TWO_PI * record.processingProgress);
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

// New function to draw global metrics panel
function drawMetricsPanel() {
  const panelX = 20;
  const panelY = 20;
  const panelWidth = 160;
  const panelHeight = 80;

  // Draw panel background
  fill(240);
  stroke(100);
  strokeWeight(1);
  rect(panelX, panelY, panelWidth, panelHeight);

  // Draw metrics text
  fill(0);
  noStroke();
  textAlign(LEFT, TOP);
  textSize(12);
  text("Global Metrics:", panelX + 5, panelY + 5);

  textSize(10);
  text(`Records: ${metrics.global.totalRecordsProduced} → ${metrics.global.totalRecordsConsumed}`,
      panelX + 5, panelY + 25);
  text(`Bytes: ${formatBytes(metrics.global.totalBytesProduced)} → ${formatBytes(metrics.global.totalBytesConsumed)}`,
      panelX + 5, panelY + 40);
  text(`Avg Processing: ${Math.round(metrics.global.avgProcessingTimeMs)}ms`,
      panelX + 5, panelY + 55);
  text(`Consumer Capacity: ${formatBytes(consumerThroughputMaxInBytes)}/s`,
      panelX + 5, panelY + 70);
}

// ------ UTILITIES ------
function formatBytes(bytes) {
  if (bytes < 1000) {
    return Math.round(bytes) + ' B';
  } else if (bytes < 1000 * 1000) {
    return (bytes / 1000).toFixed(2) + ' KB';
  } else {
    return (bytes / (1000 * 1000)).toFixed(2) + ' MB';
  }
}

function colorFromHSB(h, s, b) {
  colorMode(HSB, 360, 100, 100);
  const col = color(h, s, b);
  colorMode(RGB, 255, 255, 255);
  return col;
}

// Consumer partition assignment
function rebalanceConsumerGroup(partitions, consumerCount, strategy = 'round-robin') {
  // Array to store partition assignments (which consumer owns which partition)
  let assignments = new Array(partitions).fill(-1);

  if (consumerCount <= 0) return assignments;

  switch (strategy) {
    case 'range':
      // Range strategy: divide partitions into ranges and assign each range to a consumer
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
      const partitionsPerConsumerSticky = Math.ceil(partitions / consumerCount);

      for (let i = 0; i < partitions; i++) {
        const consumerId = Math.floor(i / partitionsPerConsumerSticky);
        assignments[i] = consumerId < consumerCount ? consumerId : consumerCount - 1;
      }
      break;

    case 'cooperative-sticky':
      // First, do round-robin assignment
      for (let i = 0; i < partitions; i++) {
        assignments[i] = i % consumerCount;
      }

      // Then, adjust to create some locality clustering
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
      for (let i = 0; i < partitions; i++) {
        assignments[i] = i % consumerCount;
      }
      break;
  }

  return assignments;
}

// Main draw function - p5.js animation loop
function draw() {
  background(240);

  // Handle events
  handleControlChanges();

  // Update simulation
  updateSimulation();

  // Render simulation
  renderSimulation();
}