export const Config = {};

export const ConfigMetadata = {
    partitionsAmount: {
        min: 1,
        max: 32,
        default: 1,
        type: 'number'
    },

    producersAmount: {
        min: 0,
        max: 32,
        default: 1,
        type: 'number'
    },

    consumersAmount: {
        min: 0,
        max: 32,
        default: 1,
        type: 'number'
    },

    producerRate: {
        min: 1,
        max: 16,
        default: 1,
        type: 'number'
    },

    producerDelayRandomFactor: {
        min: 0,
        max: 1,
        default: 0,
        step: 0.1,
        type: 'number',
        format: (value) => value.toFixed(1)
    },

    partitionBandwidth: {
        min: 1000,
        max: 32000,
        step: 1000,
        default: 1000,
        type: 'number'
    },

    consumerAssignmentStrategy: {
        choices: ['round-robin', 'range', 'sticky', 'cooperative-sticky'],
        default: 'sticky',
        type: 'string'
    },

    consumerThroughputMaxInBytes: {
        min: 1000,
        max: 2000000,
        step: 1000,
        default: 1000,
        type: 'number'
    },

    recordValueSizeMin: {
        min: 1,
        max: 10000,
        default: 800,
        type: 'number'
    },

    recordValueSizeMax: {
        min: 1,
        max: 10000,
        default: 1200,
        type: 'number'
    },

    recordKeyRange: {
        min: 1,
        max: 128,
        default: 16,
        type: 'number'
    }
};

export default function createConfigManager() {
    return {
        _p: null,
        _uiElements: {},
        _observers: {},
        _initialized: false,

        // Initialize the configuration system
        init(p) {
            this._p = p;

            // First initialize all values from defaults
            this._initFromDefaults();

            // Find and store UI elements
            this._findUIElements();

            // UPDATE UI ELEMENTS FIRST to set them to default values
            this._updateAllUI();

            // Set up dependencies between properties
            this._setupDependencies();

            // Attach event listeners to UI elements
            this._attachEventListeners();

            this._initialized = true;
            console.debug(JSON.stringify({type: "config_initialized", config: Config}));
            return this;
        },

        // Initialize configuration from defaults in ConfigMetadata
        _initFromDefaults() {
            for (const [key, metadata] of Object.entries(ConfigMetadata)) {
                Config[key] = metadata.default;
            }
        },

        // Find and store references to UI elements
        _findUIElements() {
            this._uiElements = {};

            for (const key in ConfigMetadata) {
                const elements = {};

                // Try to find UI elements for this config property
                if (key === 'consumerAssignmentStrategy') {
                    const select = this._p.select(`#${key}Select`);
                    if (select && select.elt) {
                        elements.select = select;
                    }
                } else {
                    const slider = this._p.select(`#${key}Slider`);
                    const input = this._p.select(`#${key}Input`);

                    if (slider && slider.elt) {
                        elements.slider = slider;
                    }

                    if (input && input.elt) {
                        elements.input = input;
                    }
                }

                // Only store if we found any elements
                if (Object.keys(elements).length > 0) {
                    this._uiElements[key] = elements;
                }
            }
        },

        // Attach event listeners to UI elements
        _attachEventListeners() {
            // For each UI element set, attach appropriate event handlers
            for (const key in this._uiElements) {
                const elements = this._uiElements[key];

                if (elements.select) {
                    elements.select.changed(() => {
                        this._handleUIChange(key, 'select');
                    });
                }

                if (elements.slider) {
                    // Use both input and changed events for sliders to catch all changes
                    elements.slider.input(() => {
                        this._handleUIChange(key, 'slider');
                    });
                    elements.slider.changed(() => {
                        this._handleUIChange(key, 'slider');
                    });
                }

                if (elements.input) {
                    // Use both input and changed events for inputs to catch all changes
                    elements.input.input(() => {
                        this._handleUIChange(key, 'input');
                    });
                    elements.input.changed(() => {
                        this._handleUIChange(key, 'input');
                    });
                }
            }
        },

        // Update all UI elements with current configuration values
        _updateAllUI() {
            for (const key in this._uiElements) {
                this._updateUI(key);
            }
        },

        // Handle UI element change
        _handleUIChange(key, sourceType) {
            const elements = this._uiElements[key];
            if (!elements) return;

            let value = undefined;
            let sourceElement = null;

            // Get value from the appropriate UI element
            if (sourceType === 'select' && elements.select) {
                value = elements.select.value();
                sourceElement = elements.select;
            } else if (sourceType === 'slider' && elements.slider) {
                value = this._parseValue(elements.slider.value(), key);
                sourceElement = elements.slider;
            } else if (sourceType === 'input' && elements.input) {
                value = this._parseValue(elements.input.value(), key);
                sourceElement = elements.input;
            }

            // Only proceed if we got a valid value
            if (value === undefined || value === null) return;

            // Update the configuration value (this will also update other UI elements)
            const oldValue = Config[key];

            // Update the value
            value = this._validateValue(key, value);
            Config[key] = value;

            // If value hasn't changed, don't proceed
            if (oldValue === value) return;

            if (console.debug) console.debug(JSON.stringify({
                type: "config_changed",
                key,
                newValue: value,
                oldValue,
                source: sourceType
            }));

            // Update UI elements (except source)
            this._updateUI(key, sourceElement);

            // Notify observers
            this._notifyObservers(key, value, oldValue);
        },

        // Set up dependencies between configuration properties
        _setupDependencies() {
            // Min/Max value pairs
            this.onChange('recordValueSizeMin', (newValue) => {
                if (Config.recordValueSizeMax < newValue) {
                    this.setValue('recordValueSizeMax', newValue);
                }
            });

            this.onChange('recordValueSizeMax', (newValue) => {
                if (Config.recordValueSizeMin > newValue) {
                    this.setValue('recordValueSizeMin', newValue);
                }
            });
        },

        // Update UI elements for a specific configuration property
        _updateUI(key, excludeElement = null) {
            const elements = this._uiElements[key];
            if (!elements) return;

            const value = Config[key];
            const metadata = ConfigMetadata[key];

            // Format display value if needed
            let displayValue = value;
            if (metadata.format) {
                displayValue = metadata.format(value);
            } else if (typeof value === 'number' && !Number.isInteger(value)) {
                displayValue = value.toFixed(1);
            }

            // Update select if it exists and is not the source element
            if (elements.select && elements.select !== excludeElement) {
                elements.select.elt.value = value;
            }

            // Update slider if it exists and is not the source element
            if (elements.slider && elements.slider !== excludeElement) {
                elements.slider.elt.value = value;
            }

            // Update input if it exists and is not the source element
            if (elements.input && elements.input !== excludeElement) {
                elements.input.elt.value = displayValue;
            }
        },

        // Parse value according to expected type
        _parseValue(value, key) {
            const metadata = ConfigMetadata[key];

            if (metadata.type === 'number') {
                const numValue = Number(value);
                if (isNaN(numValue)) return undefined;
                return numValue;
            }

            return value;
        },

        // Validate and normalize a value according to its metadata
        _validateValue(key, value) {
            const metadata = ConfigMetadata[key];
            if (!metadata) return value;

            // Validate type
            if (metadata.type === 'number' && typeof value !== 'number') {
                value = Number(value);
                if (isNaN(value)) return Config[key]; // Return current value if invalid
            }

            // Validate range
            if (metadata.min !== undefined && value < metadata.min) {
                value = metadata.min;
            }
            if (metadata.max !== undefined && value > metadata.max) {
                value = metadata.max;
            }

            // Validate choices
            if (metadata.choices && !metadata.choices.includes(value)) {
                return Config[key]; // Return current value if invalid
            }

            // Handle step
            if (metadata.type === 'number' && metadata.step) {
                value = Math.round(value / metadata.step) * metadata.step;
            }

            return value;
        },

        // PUBLIC API METHODS

        // Register a change observer for a configuration property
        onChange(key, callback) {
            if (!this._observers[key]) {
                this._observers[key] = [];
            }

            this._observers[key].push(callback);
            return this;
        },

        // Set a configuration value
        setValue(key, value, options = {}) {
            // Make sure the key exists in our configuration
            if (!(key in ConfigMetadata)) return false;

            // Get current value for comparison
            const oldValue = Config[key];

            // Validate and normalize the value
            value = this._validateValue(key, value);

            // Don't proceed if value hasn't changed
            if (oldValue === value) return true;

            // Update the configuration
            Config[key] = value;

            // Update UI elements
            if (!options.silent) {
                this._updateUI(key);
            }

            // Notify observers
            if (!options.silent) {
                this._notifyObservers(key, value, oldValue);
            }

            return true;
        },

        _notifyObservers(key, newValue, oldValue) {
            if (this._observers[key]) {
                this._observers[key].forEach(callback => {
                    callback(newValue, oldValue, key);
                });
            }

            // Global observers
            if (this._observers['*']) {
                this._observers['*'].forEach(callback => {
                    callback(newValue, oldValue, key);
                });
            }
        },

        // Reset a configuration property to its default value
        resetToDefault(key) {
            const metadata = ConfigMetadata[key];
            if (metadata && metadata.default !== undefined) {
                this.setValue(key, metadata.default);
            }
        },

        // Reset all configuration properties to their default values
        resetAllToDefaults() {
            for (const key in ConfigMetadata) {
                this.resetToDefault(key);
            }
        }
    };
}
