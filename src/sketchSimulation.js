import {createConsumerRenderer} from './canvas/consumers.js';
import createProducerEffectsManager, {createProducerRenderer} from "./canvas/producers";
import {createPartitionRenderer} from './canvas/partitions.js';
import createConfigManager, {Config} from './config.js';
import sharedState from './sharedState.js';
import {generateConsistentColor} from './utils.js';

const sketchSimulation = (p) => {
    // ------ Canvas, UI and Animations ------
    const CANVAS_WIDTH = 1100;
    const CANVAS_HEIGHT = 700;
    const CANVAS_PARTITION_WIDTH = 400;
    const CANVAS_PARTITION_HEIGHT = 30;
    const CANVAS_PARTITION_HEIGHT_SPACING = 20;
    const CANVAS_PARTITION_START_X = 200;
    const CANVAS_PARTITION_START_Y = 30;
    const CANVAS_PRODUCER_POSITION_X = 120;
    const CANVAS_CONSUMER_POSITION_X = CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH + 50;
    const CANVAS_RECORD_RADIUS_MAX = 15;
    const CANVAS_RECORD_RADIUS_MIN = 6;
    const ANIMATION_PRODUCER_LINE_DURATION = 100;

    const ConfigManager = createConfigManager();

    // Dynamic canvas height based on content
    let canvasHeightDynamic = CANVAS_HEIGHT;

    // Record ID counter
    let recordIDIncrementCounter = 0;

    // Runtime Data structures
    let partitions = [];
    let producers = [];
    let consumers = [];

    // Canvas Components
    let producerEffectsManager;
    let producerRenderer;
    let consumerRenderer;
    let partitionRenderer;

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

    // ------ EVENT SYSTEM ------
    // Event types for the reactive system
    const EVENTS = {
        RECORD_PRODUCED: 'record_produced',
        RECORD_REACHED_PARTITION_END: 'record_reached_partition_end',
        RECORD_PROCESSING_STARTED: 'record_processing_started',
        RECORD_PROCESSING_COMPLETED: 'record_processing_completed',
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

    p.setup = () => { // called once during boot
        // Create canvas and add it to the container div
        let canvas = p.createCanvas(CANVAS_WIDTH, canvasHeightDynamic);
        canvas.parent('canvas-simulation');

        // Initialize Canvas Components
        producerEffectsManager = createProducerEffectsManager(p);
        producerRenderer = createProducerRenderer(p, CANVAS_PRODUCER_POSITION_X);
        consumerRenderer = createConsumerRenderer(p, CANVAS_CONSUMER_POSITION_X);
        partitionRenderer = createPartitionRenderer(
            p,
            CANVAS_PARTITION_START_X,
            CANVAS_PARTITION_START_Y,
            CANVAS_PARTITION_WIDTH,
            CANVAS_PARTITION_HEIGHT,
            CANVAS_PARTITION_HEIGHT_SPACING
        );

        metrics.startTime = p.millis();
        metrics.lastUpdateTime = metrics.startTime;

        ConfigManager.init(p);

        // Set up config observers
        setupConfigObservers();

        // Set up event handlers
        setupEventHandlers();

        // Reset counters and state
        recordIDIncrementCounter = 0;

        // Reset metrics
        metrics = {
            startTime: p.millis(),
            lastUpdateTime: p.millis(),
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

        // Initialize data structures AFTER config is initialized
        initializePartitions();
        initializeProducers();
        initializeConsumers();

        setupOrderingIssuesDetection();

        // Update canvas height to accommodate partitions
        updateCanvasHeight();
    };

    p.draw = () => { // called 60 times/second
        p.background(240);

        // No need for handleControlChanges anymore - changes happen through observers
        producerEffectsManager.update();
        produceRecords();
        partitionRenderer.drawPartitionRecordsMovement(partitions, eventEmitter);
        consumeRecords();

        // Draw simulation components
        partitionRenderer.drawPartitions(partitions);
        producerRenderer.drawProducers(producers, metrics);
        consumerRenderer.drawConsumersWithConnections(
            consumers,
            partitions,
            metrics,
            CANVAS_PARTITION_START_X,
            CANVAS_PARTITION_WIDTH,
            CANVAS_PARTITION_HEIGHT
        );
        producerEffectsManager.draw();

        // Update shared state with current metrics and consumers for the metrics panel
        sharedState.updateMetrics(metrics);
        sharedState.updateConsumers(consumers);
    };

    function setupConfigObservers() {
        // Register observers for configuration changes that require data structure updates
        ConfigManager.onChange('partitionsAmount', () => updatePartitions());
        ConfigManager.onChange('producersAmount', () => updateProducers());
        ConfigManager.onChange('consumersAmount', () => updateConsumers());

        ConfigManager.onChange('consumerAssignmentStrategy', () => {
            if (Config.consumersAmount > 0) {
                updateConsumers();
            }
        });

        ConfigManager.onChange('partitionBandwidth', () => updateAllRecordSpeeds());

        ConfigManager.onChange('consumerThroughputMaxInBytes', (value) => {
            // Update throughput for all existing consumers
            for (const consumer of consumers) {
                consumer.throughputMax = value;
            }
        });
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
                    lastUpdateTime: p.millis()
                };
            }

            metrics.producers[data.producerId].recordsProduced++;
            metrics.producers[data.producerId].bytesProduced += data.value;
            metrics.global.totalRecordsProduced++;
            metrics.global.totalBytesProduced += data.value;

            // Calculate rate based on time since last update
            const now = p.millis();
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
                    lastUpdateTime: p.millis(),
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
            const now = p.millis();
            const elapsed = (now - metrics.consumers[data.consumerId].lastUpdateTime) / 1000;
            if (elapsed > 0.1) { // Only update rate if enough time has passed
                metrics.consumers[data.consumerId].consumeRate = data.value / elapsed;
                metrics.consumers[data.consumerId].recordsRate = 1 / elapsed;
                metrics.consumers[data.consumerId].lastUpdateTime = now;
            }
        });
    }

    function initializePartitions() {
        partitions = [];

        for (let i = 0; i < Config.partitionsAmount; i++) {
            partitions.push({
                id: i,
                y: CANVAS_PARTITION_START_Y + i * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING),
                records: [],
                currentOffset: 0 // Initialize offset counter for each partition
            });
        }
    }

    function initializeProducers() {
        producers = [];

        // Calculate the top and bottom Y coordinates of partitions for centering
        const topPartitionY = CANVAS_PARTITION_START_Y;
        const bottomPartitionY = CANVAS_PARTITION_START_Y + (Config.partitionsAmount - 1) * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING);

        for (let i = 0; i < Config.producersAmount; i++) {

            const color = generateConsistentColor(p, i);

            // Initially position producers evenly across the partition range
            const y = p.map(i, 0, Math.max(1, Config.producersAmount - 1),
                topPartitionY + CANVAS_PARTITION_HEIGHT / 2,
                bottomPartitionY + CANVAS_PARTITION_HEIGHT / 2);

            producers.push({
                id: i,
                y: y,
                color: color
            });

            // Initialize producer metrics
            metrics.producers[i] = {
                recordsProduced: 0,
                bytesProduced: 0,
                produceRate: 0,
                recordsRate: 0,
                lastUpdateTime: p.millis()
            };
        }

        // Adjust producer positions to prevent overlap
        adjustProducerPositions();
    }

    function initializeConsumers() {
        // Create an empty array for consumers
        consumers = [];

        // If no consumers requested, just return
        if (Config.consumersAmount <= 0) return;

        // Get partition assignments using the rebalance algorithm
        let partitionAssignments = rebalanceConsumerGroup(
            Config.partitionsAmount,
            Config.consumersAmount,
            Config.consumerAssignmentStrategy
        );

        for (let i = 0; i < Config.consumersAmount; i++) {
            // Find partitions assigned to this consumer
            const assignedPartitions = [];
            for (let j = 0; j < Config.partitionsAmount; j++) {
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
                avgY = CANVAS_PARTITION_START_Y + Config.partitionsAmount * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING) + 50 + i * 70;
            }

            const color = generateConsistentColor(p, i, 0.5, 70, 90);

            consumers.push({
                id: i,
                y: avgY,
                color: color,
                assignedPartitions: assignedPartitions,
                // Structure for concurrent processing
                activePartitions: {}, // Map of partitionId -> record being processed
                processingTimes: {}, // Map of recordId -> {startTime, endTime}
                throughputMax: Config.consumerThroughputMaxInBytes, // Bytes per second this consumer can process
                processingQueues: {}, // Map of partitionId -> queue of records waiting
                transitRecords: []
            });

            // Initialize consumer metrics
            metrics.consumers[i] = {
                recordsConsumed: 0,
                bytesConsumed: 0,
                consumeRate: 0,
                recordsRate: 0,
                lastUpdateTime: p.millis(),
                processingTimes: []
            };

            // Initialize processing queues for each assigned partition
            for (const partitionId of assignedPartitions) {
                consumers[i].processingQueues[partitionId] = [];
            }
        }

        // Only adjust positions if we have consumers
        if (Config.consumersAmount > 0) {
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
            const bottomY = CANVAS_PARTITION_START_Y + Config.partitionsAmount * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING) + 50;

            for (let i = 0; i < unassignedConsumers.length; i++) {
                unassignedConsumers[i].y = bottomY + i * MIN_CONSUMER_SPACING;
            }

            // Resort consumers by position after adjusting unassigned consumers
            consumers.sort((a, b) => a.y - b.y);
        }

        // Now fix overlaps while trying to keep each consumer as close as possible to its ideal position
        for (let i = 1; i < consumers.length; i++) {
            const prevConsumer = consumers[i - 1];
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
                        const minY = consumers[i - 1].y + MIN_CONSUMER_SPACING; // Can't go above this
                        const maxY = consumers[i + 1].y - MIN_CONSUMER_SPACING; // Can't go below this

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
                        const maxY = consumers.length > 1 ? consumers[i + 1].y - MIN_CONSUMER_SPACING : Infinity;
                        if (originalY <= maxY && consumer.y !== originalY) {
                            consumer.y = originalY;
                            improved = true;
                        }
                    } else if (i === consumers.length - 1) {
                        // Last consumer, can only be constrained from above
                        const minY = consumers[i - 1].y + MIN_CONSUMER_SPACING;
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
            const prevProducer = producers[i - 1];
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
        let lowestY = CANVAS_PARTITION_START_Y + Config.partitionsAmount * (CANVAS_PARTITION_HEIGHT + CANVAS_PARTITION_HEIGHT_SPACING);

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

        canvasHeightDynamic = p.max(minHeight, requiredHeight);
        p.resizeCanvas(CANVAS_WIDTH, canvasHeightDynamic);
    }

    function setupOrderingIssuesDetection() {
        // Track the last processed event time per key
        const keyProcessingHistory = {};

        // Add detection to the RECORD_PROCESSING_COMPLETED event
        eventEmitter.on(EVENTS.RECORD_PROCESSING_COMPLETED, (data) => {
            const key = data.key;

            // Initialize history for this key if needed
            if (!keyProcessingHistory[key]) {
                keyProcessingHistory[key] = [];
            }

            // Add this record to the processing history
            keyProcessingHistory[key].push({
                recordId: data.id,
                eventTime: data.eventTime,
                processingTime: p.millis(),
                offset: data.offset,
                partition: data.partitionId,
                consumerId: data.consumerId
            });

            // Get the history for this key and sort by processing time
            const history = keyProcessingHistory[key];

            // Only check for out-of-order if we have more than one record
            if (history.length > 1) {
                // Sort by processing time to see the order they were actually processed
                const processedOrder = [...history].sort((a, b) => a.processingTime - b.processingTime);

                // Find the most recently processed record (last in the sorted array)
                const newestProcessed = processedOrder[processedOrder.length - 1];

                // Look through previously processed records to find any out-of-order conditions
                for (let i = processedOrder.length - 2; i >= 0; i--) {
                    // If this record has a later event time but was processed earlier
                    if (processedOrder[i].eventTime > newestProcessed.eventTime) {
                        const olderRecord = newestProcessed;
                        const newerRecord = processedOrder[i];
                        const timeDifference = newerRecord.eventTime - olderRecord.eventTime;

                        console.warn("Out of order processing detected: {" +
                            "\"key\": " + key + ", " +
                            "\"olderRecord\": {" +
                            "\"id\": " + olderRecord.recordId + ", " +
                            "\"eventTime\": " + olderRecord.eventTime.toFixed(1) + ", " +
                            "\"partition\": " + olderRecord.partition + ", " +
                            "\"consumerId\": " + olderRecord.consumerId +
                            "}, " +
                            "\"newerRecord\": {" +
                            "\"id\": " + newerRecord.recordId + ", " +
                            "\"eventTime\": " + newerRecord.eventTime.toFixed(1) + ", " +
                            "\"partition\": " + newerRecord.partition + ", " +
                            "\"consumerId\": " + newerRecord.consumerId +
                            "}, " +
                            "\"timeDifference\": " + timeDifference.toFixed(1) + " ms" +
                            "}");

                        // Log only once per event
                        break;
                    }
                }
            }

            // Limit history size to prevent memory issues
            if (history.length > 100) {
                keyProcessingHistory[key] = history.slice(-100);
            }
        });

        return {keyProcessingHistory};
    }

    function updatePartitions() {
        // Save existing records and offsets
        const oldRecords = {};
        const oldOffsets = {};
        for (let i = 0; i < partitions.length; i++) {
            if (i < Config.partitionsAmount) {
                oldRecords[i] = partitions[i].records;
                oldOffsets[i] = partitions[i].currentOffset || 0;
            }
        }

        // Reinitialize partitions
        initializePartitions();

        // Restore records and offsets where possible
        for (let i = 0; i < Config.partitionsAmount; i++) {
            if (oldRecords[i]) {
                partitions[i].records = oldRecords[i];
            }
            if (oldOffsets[i] !== undefined) {
                partitions[i].currentOffset = oldOffsets[i];
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
        for (let i = 0; i < Config.producersAmount && i < Object.keys(oldMetrics).length; i++) {
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
        for (let i = 0; i < Config.consumersAmount && i < Object.keys(oldMetrics).length; i++) {
            if (oldMetrics[i]) {
                metrics.consumers[i] = oldMetrics[i];
            }
        }

        // Restore processing state for assigned partitions
        for (let i = 0; i < Config.consumersAmount && i < oldConsumers.length; i++) {
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

    function produceRecords() {
        const currentTime = p.millis();

        // Check for new records to produce
        for (const producer of producers) {
            // Calculate base time between records in milliseconds
            // Apply random delay factor if configured
            let actualDelay = 1000 / Config.producerRate;
            if (Config.producerDelayRandomFactor > 0) {
                // Calculate a random factor between 1.0 and (1.0 + producerDelayRandomFactor)
                const randomFactor = 1.0 + p.random(0, Config.producerDelayRandomFactor);
                actualDelay *= randomFactor;
            }

            // If this is the first record or enough time has elapsed since last production
            if (!producer.lastProduceTime || currentTime - producer.lastProduceTime >= actualDelay) {
                // Create and add a new record
                createAndEmitRecord(producer);

                // Update last produce time to current time
                producer.lastProduceTime = currentTime;
            }
        }
    }

    function createAndEmitRecord(producer) {
        // Generate record characteristics
        const recordSize = p.random(Config.recordValueSizeMin, Config.recordValueSizeMax);
        const recordRadius = calculateRecordRadius(recordSize);
        const recordKey = p.int(p.random(1, Config.recordKeyRange + 1));
        const partitionId = recordKey % Config.partitionsAmount;
        const recordSpeed = calculateRecordSpeedMS(recordSize);
        const eventTime = p.millis(); // Add creation timestamp

        // Get the current offset and increment it for this partition
        const offset = partitions[partitionId].currentOffset++;

        // Create the record object
        const record = {
            id: recordIDIncrementCounter++,
            key: recordKey,
            value: recordSize,
            radius: recordRadius,
            producerId: producer.id,
            partitionId: partitionId,
            speed: recordSpeed,
            offset: offset,
            eventTime: eventTime, // Store creation time with the record

            // UI
            x: CANVAS_PARTITION_START_X + recordRadius,
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
        producerEffectsManager.addEffect(
            CANVAS_PRODUCER_POSITION_X + 15,
            producer.y,
            CANVAS_PARTITION_START_X,
            partitions[partitionId].y + CANVAS_PARTITION_HEIGHT / 2,
            producer.color,
            ANIMATION_PRODUCER_LINE_DURATION
        );

        console.log("Record produced: {" +
            "\"id\": " + record.id + ", " +
            "\"key\": " + record.key + ", " +
            "\"valueBytes\": " + Math.round(recordSize) + ", " +
            "\"partition\": " + partitionId + ", " +
            "\"offset\": " + offset + ", " +
            "\"producer\": " + producer.id + ", " +
            "\"producedAt\": " + p.millis().toFixed(1) + ", " +
            "\"eventTime\": " + eventTime.toFixed(1) + "}"
        );
    }

    function calculateRecordRadius(size) {
        // Handle edge case when min and max are equal
        if (Config.recordValueSizeMin === Config.recordValueSizeMax) {
            return CANVAS_RECORD_RADIUS_MAX;
        }

        // Handle edge case when min and max are invalid
        if (Config.recordValueSizeMin > Config.recordValueSizeMax) {
            return (CANVAS_RECORD_RADIUS_MIN + CANVAS_RECORD_RADIUS_MAX) / 2;
        }

        // Linear mapping from size to radius
        return p.map(
            size,
            Config.recordValueSizeMin,
            Config.recordValueSizeMax,
            CANVAS_RECORD_RADIUS_MIN,
            CANVAS_RECORD_RADIUS_MAX
        );
    }

    // Calculate record speed based on partition bandwidth
    function calculateRecordSpeedMS(recordSize) {
        // Calculate transfer time in milliseconds
        // Formula: time (ms) = size (bytes) / bandwidth (bytes/s) * 1000
        return (recordSize / Config.partitionBandwidth) * 1000;
    }

    // Function to update all existing record speeds
    function updateAllRecordSpeeds() {
        // Update partition records
        for (const partition of partitions) {
            for (const record of partition.records) {
                record.speed = calculateRecordSpeedMS(record.value);
            }
        }

        // Update consumer transit records
        for (const consumer of consumers) {
            if (consumer.transitRecords) {
                for (const record of consumer.transitRecords) {
                    record.speed = calculateRecordSpeedMS(record.value);
                }
            }
        }
    }

    function consumeRecords() {
        const currentTime = p.millis();

        for (const consumer of consumers) {
            // Ensure we have the necessary data structures
            if (!consumer.activePartitions) consumer.activePartitions = {};
            if (!consumer.recordProcessingState) consumer.recordProcessingState = {}; // New tracking object
            if (!consumer.processingQueues) consumer.processingQueues = {};
            if (!consumer.lastUpdateTime) consumer.lastUpdateTime = currentTime;

            // Calculate time elapsed since last update
            const elapsedTimeMs = currentTime - consumer.lastUpdateTime;
            consumer.lastUpdateTime = currentTime;

            // Skip processing if no time has passed (prevents division by zero)
            if (elapsedTimeMs <= 0) continue;

            // Count active records (not just partitions)
            const activeRecords = [];
            for (const partitionId in consumer.activePartitions) {
                const record = consumer.activePartitions[partitionId];
                if (record) {
                    activeRecords.push({
                        id: record.id,
                        partitionId: partitionId,
                        record: record
                    });
                }
            }

            const activeRecordCount = activeRecords.length;

            if (activeRecordCount > 0) {
                // Distribute throughput evenly across active records
                const throughputPerRecord = consumer.throughputMax / activeRecordCount;

                // Calculate bytes processed during this time slice for each active record
                const bytesProcessedPerRecord = (throughputPerRecord * elapsedTimeMs) / 1000;

                // Process each active record
                for (const activeRecord of activeRecords) {
                    const record = activeRecord.record;
                    const partitionId = activeRecord.partitionId;

                    // Initialize processing state if needed
                    if (!consumer.recordProcessingState[record.id]) {
                        consumer.recordProcessingState[record.id] = {
                            startTime: currentTime,
                            bytesProcessed: 0,
                            bytesTotal: record.value,
                            partitionId: partitionId,
                            lastProgressUpdate: currentTime
                        };
                    }

                    const state = consumer.recordProcessingState[record.id];

                    // Update bytes processed
                    state.bytesProcessed += bytesProcessedPerRecord;
                    state.lastProgressUpdate = currentTime;

                    // Update visual progress indicator
                    record.processingProgress = Math.min(state.bytesProcessed / state.bytesTotal, 0.99);

                    // Check if record is complete
                    if (state.bytesProcessed >= state.bytesTotal) {
                        const finishedTime = p.millis();
                        // Record is finished, remove it from active partitions
                        const finishedRecord = {...record};
                        delete consumer.activePartitions[partitionId];

                        // Calculate actual processing time and any lost bytes
                        const actualTime = currentTime - state.startTime;
                        const lostBytes = Math.max(0, state.bytesProcessed - state.bytesTotal);

                        // Clean up state
                        delete consumer.recordProcessingState[record.id];

                        // Mark record as processed
                        finishedRecord.isBeingProcessed = false;
                        finishedRecord.isProcessed = true;

                        // Emit completion event with processing metrics
                        eventEmitter.emit(EVENTS.RECORD_PROCESSING_COMPLETED, {
                            ...finishedRecord,
                            consumerId: consumer.id,
                            processingTimeMs: actualTime,
                            lostBytes: lostBytes
                        });

                        // Now remove the record from the partition since processing is complete
                        const partition = partitions[partitionId];
                        if (partition) {
                            // Find and remove this record from the partition
                            const recordIndex = partition.records.findIndex(r => r.id === record.id);
                            if (recordIndex >= 0) {
                                partition.records.splice(recordIndex, 1);
                            }
                        }

                        // Calculate end-to-end latency in milliseconds
                        const e2eLatencyMs = finishedTime - record.eventTime;

                        console.log("Record processing completed: {" +
                            "\"id\": " + record.id + ", " +
                            "\"key\": " + record.key + ", " +
                            "\"valueBytes\": " + Math.round(record.value) + ", " +
                            "\"partition\": " + partitionId + ", " +
                            "\"offset\": " + record.offset + ", " +
                            "\"consumer\": " + consumer.id + ", " +
                            "\"actualTimeMs\": " + Math.round(actualTime) + ", " +
                            "\"lostBytes\": " + Math.round(lostBytes) + ", " +
                            "\"committedAt\": " + finishedTime.toFixed(1) + ", " +
                            "\"e2eLatencyMs\": " + Math.round(e2eLatencyMs) + "}"
                        );

                        // If there are more records in the queue for this partition, process the next one
                        if (consumer.processingQueues[partitionId] && consumer.processingQueues[partitionId].length > 0) {
                            const nextRecord = consumer.processingQueues[partitionId].shift();
                            startProcessingRecord(consumer, nextRecord, partitionId, currentTime);
                        }
                    }
                }

                // After processing current active records, update expected completion times for UI
                recalculateProcessingTimes(consumer, currentTime);
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
                    if (firstRecord.x >= CANVAS_PARTITION_START_X + CANVAS_PARTITION_WIDTH - firstRecord.radius - 5) {
                        // Don't remove the record from the partition - keep it there during processing
                        // Just reference it in the consumer's active records

                        // Start processing this record
                        startProcessingRecord(consumer, firstRecord, partitionId, currentTime);

                        // Mark record as being processed by this consumer (but it stays in the partition)
                        transferRecordToConsumer(consumer, firstRecord, partitionId);
                    }
                }
            }
        }
    }

    function startProcessingRecord(consumer, record, partitionId, currentTime) {
        // Ensure necessary data structures exist
        if (!consumer.activePartitions) consumer.activePartitions = {};
        if (!consumer.recordProcessingState) consumer.recordProcessingState = {};
        if (!consumer.lastUpdateTime) consumer.lastUpdateTime = currentTime;

        // Add record to active partitions
        consumer.activePartitions[partitionId] = record;

        // Initialize processing state with byte tracking
        consumer.recordProcessingState[record.id] = {
            startTime: currentTime,
            bytesProcessed: 0,
            bytesTotal: record.value,
            partitionId: partitionId,
            lastProgressUpdate: currentTime
        };

        // Calculate estimated processing time for UI/metrics
        const activeRecordCount = Object.keys(consumer.activePartitions).length;
        const throughputPerRecord = consumer.throughputMax / activeRecordCount;
        const estimatedProcessingTimeMs = (record.value / throughputPerRecord) * 1000;

        // Update record state
        record.isBeingProcessed = true;
        record.isWaiting = false;
        record.processingTimeMs = estimatedProcessingTimeMs;
        record.processingProgress = 0;

        // If there's a transit record for this record, update its properties for synchronized animation
        if (consumer.transitRecords) {
            const transitRecord = consumer.transitRecords.find(tr => tr.id === record.id);
            if (transitRecord) {
                transitRecord.processingStartTime = currentTime;
                transitRecord.estimatedProcessingTimeMs = estimatedProcessingTimeMs;
            }
        }

        // Emit event for processing start
        eventEmitter.emit(EVENTS.RECORD_PROCESSING_STARTED, {
            ...record,
            consumerId: consumer.id,
            estimatedTimeMs: estimatedProcessingTimeMs
        });

        console.log("Record processing started: {" +
            "\"id\": " + record.id + ", " +
            "\"key\": " + record.key + ", " +
            "\"valueBytes\": " + Math.round(record.value) + ", " +
            "\"partition\": " + partitionId + ", " +
            "\"offset\": " + record.offset + ", " +
            "\"consumer\": " + consumer.id + ", " +
            "\"estimatedTimeMs\": " + Math.round(estimatedProcessingTimeMs) + "}"
        );

        // After adding a new record, recalculate processing times for all records
        recalculateProcessingTimes(consumer, currentTime);
    }

    // New function to update all processing times based on current capacity allocation
    function recalculateProcessingTimes(consumer, currentTime) {
        // Count active records
        const activeRecords = [];
        for (const partitionId in consumer.activePartitions) {
            const record = consumer.activePartitions[partitionId];
            if (record) {
                activeRecords.push({
                    id: record.id,
                    partitionId: partitionId,
                    record: record
                });
            }
        }

        const activeRecordCount = activeRecords.length;

        if (activeRecordCount === 0) return;

        // Throughput per record given equal distribution
        const throughputPerRecord = consumer.throughputMax / activeRecordCount;

        // Update each active record's expected completion time
        for (const activeRecord of activeRecords) {
            const record = activeRecord.record;
            const partitionId = activeRecord.partitionId;

            const state = consumer.recordProcessingState[record.id];
            if (!state) continue;

            // Calculate remaining bytes
            const bytesRemaining = Math.max(0, state.bytesTotal - state.bytesProcessed);

            // Calculate time needed to process remaining bytes at current throughput
            const timeRemainingMs = (bytesRemaining / throughputPerRecord) * 1000;

            // Set expected completion time for UI purposes
            const expectedEndTime = currentTime + timeRemainingMs;

            // Store end time for visualization
            if (!consumer.processingTimes) consumer.processingTimes = {};
            consumer.processingTimes[record.id] = {
                startTime: state.startTime,
                endTime: expectedEndTime,
                partitionId: partitionId
            };

            // Update the processing progress for visualization
            record.processingProgress = Math.min(state.bytesProcessed / state.bytesTotal, 0.99);
        }
    }

    // Mark a record as being processed by a consumer (no longer creates a transit path)
    function transferRecordToConsumer(consumer, record, partitionId, isWaiting = false) {
        // We no longer need to create a transit animation
        // Just mark the record as being processed so it stays in place in the partition
        record.isBeingProcessed = !isWaiting;
        record.isWaiting = isWaiting;
        record.processingConsumerId = consumer.id; // Mark which consumer is processing this record
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

    // ------ UTILITIES ------
    function colorFromHSB(h, s, b) {
        p.colorMode(p.HSB, 360, 100, 100);
        const col = p.color(h, s, b);
        p.colorMode(p.RGB, 255, 255, 255);
        return col;
    }
};

export default sketchSimulation;
