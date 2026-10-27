fn main() -> std::process::ExitCode {
    telemetry_core::run(diagnostics_dtc_router::DiagnosticsDtcRouter::default())
}
