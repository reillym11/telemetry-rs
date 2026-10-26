fn main() -> std::process::ExitCode {
    telemetry_core::run(ota_status_tracker::OtaStatusTracker::default())
}
