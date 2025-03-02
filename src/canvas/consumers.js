import { formatBytes } from '../utils.js';

export function createConsumerRenderer(p, positionX) {
  // Function to draw a single consumer
  function drawConsumerComponent(consumer, index, metrics) {
    p.push(); // Start a new drawing context
    p.translate(positionX, consumer.y); // Set the origin to the consumer position

    // Get metrics for this consumer
    const consumerMetrics = metrics?.consumers?.[consumer.id] || {
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
    p.textSize(10);
    const textHeight = 15; // Height per line of text
    const textPadding = 2; // Padding between text and border
    const metricsWidth = p.max(
        ...metricsData.map(text => p.textWidth(text))
    ) + textPadding * 2;
    const metricsHeight = textHeight * metricsData.length + textPadding * 2;

    // Use gray color for unassigned consumers
    const borderColor = consumer.assignedPartitions.length === 0 ? p.color(200) : consumer.color;

    // Draw metrics box - vertically centered with the consumer square
    p.noFill();
    p.stroke(borderColor);
    p.strokeWeight(1);
    p.rect(30, -metricsHeight / 2, metricsWidth, metricsHeight);

    // Draw metrics text
    p.fill(0);
    p.noStroke();
    p.textAlign(p.LEFT, p.TOP);
    for (let i = 0; i < metricsData.length; i++) {
        p.text(metricsData[i], 30 + textPadding, -metricsHeight / 2 + textPadding + i * textHeight);
    }

    // Draw consumer rectangle - always use regular color regardless of busy state
    p.fill(consumer.color);
    p.stroke(0);
    p.strokeWeight(1);
    p.rect(0, -15, 30, 30);

    // Draw consumer ID inside rectangle
    p.fill(255);
    p.noStroke();
    p.textAlign(p.CENTER, p.CENTER);
    p.textSize(10);
    p.textStyle(p.BOLD);
    p.text(index, 15, 0);
    p.textStyle(p.NORMAL);

    p.pop(); // Restore the drawing context
  }

  // Function to draw connections between consumers and their partitions
  function drawConsumerPartitionConnections(consumer, partitions, partitionStartX, partitionWidth, partitionHeight) {
    p.stroke(consumer.color);
    p.strokeWeight(1.8);
    p.drawingContext.setLineDash([5, 5]);

    for (const partitionId of consumer.assignedPartitions) {
      const partitionY = partitions[partitionId].y + partitionHeight / 2;
      p.line(partitionStartX + partitionWidth, partitionY, positionX, consumer.y);
    }

    p.drawingContext.setLineDash([]);
  }

  return {
    drawConsumer: drawConsumerComponent,
    drawConsumerPartitionConnections: drawConsumerPartitionConnections,
    drawConsumers(consumers, metrics) {
      for (let i = 0; i < consumers.length; i++) {
        drawConsumerComponent(consumers[i], i, metrics);
      }
    },
    drawConsumersWithConnections(consumers, partitions, metrics, partitionStartX, partitionWidth, partitionHeight) {
      for (let i = 0; i < consumers.length; i++) {
        drawConsumerComponent(consumers[i], i, metrics);
        drawConsumerPartitionConnections(
          consumers[i],
          partitions,
          partitionStartX,
          partitionWidth,
          partitionHeight
        );
      }
    }
  };
}