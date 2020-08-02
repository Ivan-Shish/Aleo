use tracing::debug;

pub fn report_progress_starting() {
    debug!(report = "progress", phase = "starting");
}
pub fn report_progress_processing(start: usize, end: usize, total: usize) {
    debug!(report = "progress", phase = "processing", start, end, total);
}
pub fn report_progress_ending() {
    debug!(report = "progress", phase = "ending");
}
