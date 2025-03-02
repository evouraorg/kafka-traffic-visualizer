import sharedState from './sharedState.js';
import {createMetricsPanelRenderer} from './canvas/metricsPanel.js';

const metricsSketch = (p) => {
    const CANVAS_WIDTH = 1100;
    const CANVAS_HEIGHT = 150;

    let metricsPanelRenderer;

    p.setup = () => {
        let metricsCanvas = p.createCanvas(CANVAS_WIDTH, CANVAS_HEIGHT);
        metricsCanvas.parent('canvas-metrics');

        metricsPanelRenderer = createMetricsPanelRenderer(p);
    };

    p.draw = () => {
        p.background(200);

        metricsPanelRenderer.drawMetricsPanel(
            sharedState.getMetrics(),
            sharedState.consumers
        );
    };
};

export default metricsSketch;
