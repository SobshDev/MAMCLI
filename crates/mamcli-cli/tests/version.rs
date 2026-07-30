use std::process::Command;

fn assert_version_flag(flag: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_mamcli"))
        .arg(flag)
        .output()
        .expect("mamcli should run");

    assert!(
        output.status.success(),
        "mamcli {flag} failed with status {} and stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("version output should be valid UTF-8"),
        format!("mamcli {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(
        output.stderr.is_empty(),
        "version output should not write to stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn long_version_flag_prints_package_version() {
    assert_version_flag("--version");
}

#[test]
fn short_version_flag_prints_package_version() {
    assert_version_flag("-V");
}
