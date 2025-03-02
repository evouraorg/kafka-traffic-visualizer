import { formatBytes } from '../utils.js';

export default function createProducerEffectsManager(p) {
  const effects = [];

  function addEffectToManager(x1, y1, x2, y2, color, duration) {
    const effect = {
      startTime: p.millis(),
      endTime: p.millis() + duration,
      x1, y1, x2, y2, color
    };
    effects.push(effect);
  }

  function updateEffects() {
    // Remove expired effects
    for (let i = effects.length - 1; i >= 0; i--) {
      if (p.millis() >= effects[i].endTime) {
        effects.splice(i, 1);
      }
    }
  }

  function drawEffects() {
    for (const effect of effects) {
      // Draw the full line immediately (no animation)
      p.stroke(effect.color);
      p.strokeWeight(2);
      p.line(effect.x1, effect.y1, effect.x2, effect.y2);
    }
  }

  function getEffectsCount() {
    return effects.length;
  }

  return {
    addEffect: addEffectToManager,
    update: updateEffects,
    draw: drawEffects,
    getCount: getEffectsCount
  };
}

// src/canvas/producers.js
export function createProducerRenderer(p, positionX) {
  // Original function to draw a single producer
  function drawProducerComponent(producer, index, metrics) {
    p.push(); // Start a new drawing context
    p.translate(positionX, producer.y); // Set the origin to the producer position

    // Get metrics for this producer
    const producerMetrics = metrics?.producers?.[producer.id] || {
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
    p.textSize(10);
    const textHeight = 15; // Height per line of text
    const textPadding = 2; // Padding between text and border
    const metricsWidth = p.max(
      p.textWidth(metricsData[0]),
      p.textWidth(metricsData[1]),
      p.textWidth(metricsData[2]),
      p.textWidth(metricsData[3])
    ) + textPadding * 2;
    const metricsHeight = textHeight * metricsData.length + textPadding * 2;

    // Draw metrics box - positioned to touch the producer triangle
    p.noFill();
    p.stroke(producer.color);
    p.strokeWeight(1);
    p.rect(-metricsWidth - 15, -metricsHeight / 2, metricsWidth, metricsHeight);

    // Draw metrics text
    p.fill(0);
    p.noStroke();
    p.textAlign(p.LEFT, p.TOP);
    for (let i = 0; i < metricsData.length; i++) {
      p.text(
        metricsData[i],
        -metricsWidth - 15 + textPadding,
        -metricsHeight / 2 + i * textHeight + textPadding
      );
    }

    // Draw producer symbol (triangle)
    p.fill(producer.color);
    p.stroke(0);
    p.strokeWeight(1);
    p.triangle(-15, -15, 15, 0, -15, 15);

    // Draw producer ID inside the triangle
    p.fill(255);
    p.noStroke();
    p.textAlign(p.CENTER, p.CENTER);
    p.textSize(10);
    p.textStyle(p.BOLD);
    p.text(index, -10, 0);
    p.textStyle(p.NORMAL);

    p.pop(); // Restore the drawing context
  }

  // Return an object with both individual and batch rendering functions
  return {
    drawProducer: drawProducerComponent,
    drawProducers(producers, metrics) {
      for (let i = 0; i < producers.length; i++) {
        drawProducerComponent(producers[i], i, metrics);
      }
    }
  };
}
