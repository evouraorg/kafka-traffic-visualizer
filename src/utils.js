
export function formatBytes(bytes) {
  if (bytes < 1000) {
    return Math.round(bytes) + ' B';
  } else if (bytes < 1000 * 1000) {
    return (bytes / 1000).toFixed(2) + ' KB';
  } else {
    return (bytes / (1000 * 1000)).toFixed(2) + ' MB';
  }
}
