
export function formatBytes(bytes) {
  if (bytes < 1000) {
    return Math.round(bytes) + ' B';
  } else if (bytes < 1000 * 1000) {
    return (bytes / 1000).toFixed(2) + ' KB';
  } else {
    return (bytes / (1000 * 1000)).toFixed(2) + ' MB';
  }
}

/**
 * Generates a consistent color based on an index using the golden ratio conjugate
 *
 * @param {Object} p - p5.js instance
 * @param {number} index - The index to generate color for
 * @param {number} [offset=0] - Offset in the color wheel (0.5 = opposite side)
 * @param {number} [saturation=70] - Color saturation (0-100)
 * @param {number} [brightness=80] - Color brightness (0-100)
 * @returns {Object} p5.js color object
 */
export function generateConsistentColor(p, index, offset = 0, saturation = 70, brightness = 80) {
  const goldenRatioConjugate = 0.618033988749895;
  const hue = (((index * goldenRatioConjugate) + offset) % 1) * 360;

  p.colorMode(p.HSB, 360, 100, 100);
  const color = p.color(hue, saturation, brightness);
  p.colorMode(p.RGB, 255, 255, 255);

  return color;
}
