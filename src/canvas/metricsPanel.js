import {formatBytes} from '../utils.js';

export function createMetricsPanelRenderer(p, panelX = 20, panelY = 20) {
    function formatElapsedTime(ms) {
        if (ms < 1000) {
            return `${ms.toFixed(0)}ms`;
        } else if (ms < 60000) {
            const seconds = Math.floor(ms / 1000);
            const remainingMs = ms % 1000;
            return `${seconds}s ${remainingMs.toFixed(0)}ms`;
        } else {
            const minutes = Math.floor(ms / 60000);
            const seconds = Math.floor((ms % 60000) / 1000);
            const remainingMs = ms % 1000;
            return `${minutes}m ${seconds}s ${remainingMs.toFixed(0)}ms`;
        }
    }

    function drawMetricsPanel(metrics, consumers = []) {
        const panelWidth = 160;
        const panelHeight = 110; // Increased to fit new metrics

        // Calculate active consumer count (consumers with assigned partitions)
        let consumerThroughput = 0;

        consumers.filter(consumer =>
            consumer.assignedPartitions && consumer.assignedPartitions.length > 0
        ).forEach((consumer) => {
            consumerThroughput += consumer.throughputMax
        });

        p.push();
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

        const elapsedMs = p.millis();

        p.textSize(10);
        p.text(`Records: ${metrics.global.totalRecordsProduced} → ${metrics.global.totalRecordsConsumed}`,
            panelX + 5, panelY + 25);
        p.text(`Bytes: ${formatBytes(metrics.global.totalBytesProduced)} → ${formatBytes(metrics.global.totalBytesConsumed)}`,
            panelX + 5, panelY + 40);
        p.text(`Avg Processing: ${Math.round(metrics.global.avgProcessingTimeMs)}ms`,
            panelX + 5, panelY + 55);
        p.text(`Consumers Throughput: ${formatBytes(consumerThroughput)}`,
            panelX + 5, panelY + 70);
        p.text(`Elapsed ms: ${elapsedMs.toFixed(0)}`,
            panelX + 5, panelY + 85);
        p.text(`Elapsed time: ${formatElapsedTime(elapsedMs)}`,
            panelX + 5, panelY + 100);
        p.pop();
    }

    return {
        drawMetricsPanel
    };
}
