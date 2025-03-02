import sharedState from './sharedState.js';
import {createMetricsPanelRenderer} from './canvas/metricsPanel.js';

const metricsSketch = (p) => {
    // Constants for metrics panel
    const CANVAS_WIDTH = 1100;
    const CANVAS_HEIGHT = 300;

    // Position for multiple metrics panels
    const PANEL_SPACING = 180;
    const PANEL_START_X = 20;
    const PANEL_START_Y = 20;

    // Canvas Component
    let metricsPanelRenderer;

    p.setup = () => {
        // Create canvas for metrics panel
        let metricsCanvas = p.createCanvas(CANVAS_WIDTH, CANVAS_HEIGHT);
        metricsCanvas.parent('canvas-metrics');

        // Initialize metrics panel renderer
        metricsPanelRenderer = createMetricsPanelRenderer(p);
    };

    p.draw = () => {
        // Clear the canvas
        p.background(200);

        // Get current metrics from shared state
        const metrics = sharedState.getMetrics();
        const consumers = sharedState.consumers;

        // Calculate required panels
        const numConsumers = Object.keys(metrics.consumers).length;
        const numProducers = Object.keys(metrics.producers).length;

        // Draw global metrics panel at the top
        metricsPanelRenderer.drawMetricsPanel(metrics, consumers);

        // Resize canvas height dynamically if needed
        let requiredHeight = PANEL_START_Y + 120; // Base height for global metrics

        // Add space for producer and consumer details if we add them in the future
        const additionalPanels = Math.ceil((numProducers + numConsumers) / 2);
        requiredHeight += additionalPanels * PANEL_SPACING;

        // Ensure minimum height
        requiredHeight = Math.max(CANVAS_HEIGHT, requiredHeight);

        // Resize if needed
        if (p.height !== requiredHeight) {
            p.resizeCanvas(CANVAS_WIDTH, requiredHeight);
        }
    };
};

export default metricsSketch;