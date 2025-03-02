const sharedState = {
    metrics: {
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
    },

    // Method to update metrics from simulation sketch
    updateMetrics(newMetrics) {
        this.metrics = newMetrics;
    },

    // Method to get current metrics for metrics panel sketch
    getMetrics() {
        return this.metrics;
    },

    // Reference to consumers array for the metrics panel to use
    consumers: [],

    // Method to update consumers reference
    updateConsumers(newConsumers) {
        this.consumers = newConsumers;
    }
};

export default sharedState;
