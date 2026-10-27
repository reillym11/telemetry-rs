fn main() -> std::process::ExitCode {
    telemetry_core::run(battery_health_aggregator::BatteryHealthAggregator::default())
}
