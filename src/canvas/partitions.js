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

    // Return public API
    return {
        drawPartition,
        drawPartitionRecords,
        drawPartitions(partitions) {
            for (let i = 0; i < partitions.length; i++) {
                drawPartition(partitions[i], i);
                drawPartitionRecords(partitions[i]);
            }
        }
    };
}
