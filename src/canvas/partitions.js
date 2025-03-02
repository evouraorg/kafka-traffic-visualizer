import { formatBytes } from '../utils.js';

export function createPartitionRenderer(p, startX, startY, partitionWidth, partitionHeight, partitionSpacing) {
    // Draw a single partition with its records
    function drawPartition(partition, index) {
        // Set consistent styling for all partitions
        p.push();
        p.fill(255);
        p.stroke(0);  // Changed from 100 to 0 to match original styling
        p.strokeWeight(1);

        // Draw partition rectangle
        p.rect(startX, partition.y, partitionWidth, partitionHeight);
        p.pop();

        // Draw partition label with current offset (matching original format)
        p.fill(0);
        p.noStroke();
        p.textAlign(p.RIGHT, p.CENTER);
        p.textSize(12);
        p.text(`P${index} (${partition.currentOffset})`, startX - 10, partition.y + partitionHeight / 2);
    }

    // Draw all records within a partition
    function drawPartitionRecords(partition) {
        for (const record of partition.records) {
            // Draw Record circle
            p.fill(record.color);
            p.stroke(0);
            p.strokeWeight(1);
            p.ellipse(record.x, partition.y + partitionHeight / 2, record.radius * 2, record.radius * 2);

            p.fill(255);
            p.noStroke();
            p.textAlign(p.CENTER, p.CENTER);
            p.textSize(10);
            p.text(record.key, record.x, partition.y + partitionHeight / 2);

            // Processing Records circular progress bar
            if (record.isBeingProcessed && record.processingProgress !== undefined) {
                p.noFill();
                p.stroke(0, 179, 0);
                p.strokeWeight(2);
                p.arc(
                    record.x,
                    partition.y + partitionHeight / 2,
                    (record.radius + 1) * 2,
                    (record.radius + 1) * 2,
                    -p.HALF_PI,
                    -p.HALF_PI + p.TWO_PI * record.processingProgress
                );
            }
        }
    }

    function drawPartitionRecordsMovement(partitions, eventEmitter) {
        // Process each partition
        for (const partition of partitions) {
            // Skip empty partitions
            if (partition.records.length === 0) continue;

            // First pass: Move records from oldest to newest (FIFO order)
            for (let i = 0; i < partition.records.length; i++) {
                const record = partition.records[i];

                // Skip records being processed or waiting
                if (record.isBeingProcessed || record.isWaiting) continue;

                // Define the maximum position inside the partition
                const maxX = startX + partitionWidth - record.radius - 5;

                // Convert milliseconds to pixels per frame
                const framesForTransfer = record.speed / (1000 / 60); // at 60fps
                const pixelsPerFrame = framesForTransfer > 0 ? partitionWidth / framesForTransfer : 0;

                // First record can move freely
                if (i === 0) {
                    const newX = Math.min(record.x + pixelsPerFrame, maxX);

                    // Emit event when record reaches the end of partition
                    if (record.x < maxX && newX >= maxX) {
                        eventEmitter.emit('RECORD_REACHED_PARTITION_END', {
                            recordId: record.id,
                            partitionId: partition.id
                        });
                    }

                    record.x = newX;
                    continue;
                }

                // Other records: check collision with the record ahead
                const recordAhead = partition.records[i - 1];
                const minDistance = recordAhead.radius + record.radius;
                const maxPossibleX = recordAhead.x - minDistance;

                // Move without collision
                if (record.x < maxPossibleX) {
                    record.x = Math.min(record.x + pixelsPerFrame, maxPossibleX);
                }
            }

            // Sort records by x position for proper drawing order
            partition.records.sort((a, b) => b.x - a.x);

            // Handle records being processed
            const processingRecords = partition.records.filter(r => r.isBeingProcessed);
            if (processingRecords.length === 0) continue;

            // Position processing records at the end of the partition
            const maxX = startX + partitionWidth - 5;
            for (const record of processingRecords) {
                record.x = maxX - record.radius;
            }

            // Ensure non-processing records don't overlap with processing ones
            const minNonProcessingX = maxX - (processingRecords[0].radius * 2) - 5;
            for (const record of partition.records) {
                if (!record.isBeingProcessed && record.x > minNonProcessingX) {
                    record.x = minNonProcessingX;
                }
            }
        }
    }

    // Return public API
    return {
        drawPartition,
        drawPartitionRecords,
        drawPartitions(partitions) {
            for (let i = 0; i < partitions.length; i++) {
                drawPartition(partitions[i], i);
                drawPartitionRecords(partitions[i]);
            }
        },
        drawPartitionRecordsMovement
    };
}
