import {formatBytes} from '../utils.js';

export function createMetricsPanelRenderer(p, panelX = 20, panelY = 20) {
    function drawMetricsPanel(metrics, consumers = []) {
        const panelWidth = 160;
        const panelHeight = 80;

        // Calculate active consumer count (consumers with assigned partitions)
        let consumerThroughput = 0;

        consumers.filter(consumer =>
            consumer.assignedPartitions && consumer.assignedPartitions.length > 0
        ).forEach((consumer) => {
            consumerThroughput += consumer.throughputMax
        });

        // Draw panel background
        p.fill(240);
        p.stroke(100);
        p.strokeWeight(1);
        p.rect(panelX, panelY, panelWidth, panelHeight);

        // Draw metrics text
        p.fill(0);
        p.noStroke();
        p.textAlign(p.LEFT, p.TOP);
        p.textSize(12);
        p.text("Global Metrics:", panelX + 5, panelY + 5);

        p.textSize(10);
        p.text(`Records: ${metrics.global.totalRecordsProduced} → ${metrics.global.totalRecordsConsumed}`,
            panelX + 5, panelY + 25);
        p.text(`Bytes: ${formatBytes(metrics.global.totalBytesProduced)} → ${formatBytes(metrics.global.totalBytesConsumed)}`,
            panelX + 5, panelY + 40);
        p.text(`Avg Processing: ${Math.round(metrics.global.avgProcessingTimeMs)}ms`,
            panelX + 5, panelY + 55);
        p.text(`Consumers Throughput: ${formatBytes(consumerThroughput)}`,
            panelX + 5, panelY + 70);
    }

    return {
        drawMetricsPanel
    };
}