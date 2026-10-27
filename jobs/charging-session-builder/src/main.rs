fn main() -> std::process::ExitCode {
    telemetry_core::run(charging_session_builder::ChargingSessionBuilder::default())
}
