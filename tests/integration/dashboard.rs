//! Dashboard listener, read-only API, and file browser

use super::{
    Harness, dashboard_addr, http_get, http_get_host, http_post_json, http_request, wait_until,
};
use serde_json::{Value, json};
use std::fs;
use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn dashboard_off_when_web_listen_unset() {
    let hb = assert_cmd::cargo::cargo_bin("homebased");
    let dir = TempDir::new().unwrap();
    let user_home = dir.path().join("user-home");
    let home = dir.path().join("state");
    fs::create_dir_all(&user_home).unwrap();
    fs::create_dir_all(&home).unwrap();
    let mut child = Command::new(&hb)
        .env_remove("HOMEBASED_WEB_LISTEN")
        .env("HOME", &user_home)
        .args(["daemon", "serve", "--home"])
        .arg(&home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let sock = home.join("homebased.sock");
    let appeared = wait_until(Duration::from_secs(5), || sock.exists());
    let out = if appeared {
        Some(
            Command::new(&hb)
                .env_remove("HOMEBASED_WEB_LISTEN")
                .env("HOME", &user_home)
                .args(["--json", "daemon", "status", "--home"])
                .arg(&home)
                .output()
                .unwrap(),
        )
    } else {
        None
    };
    let _ = child.kill();
    let _ = child.wait();
    assert!(appeared, "socket did not appear");
    let out = out.expect("status ran");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let status: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(status["socket"], "up", "{status}");
    assert!(status["web"].is_null(), "{status}");
}

#[test]
fn web_listener_serves_read_only_api() {
    let h = Harness::with_dashboard();
    h.set_control("stdout", "line one\nline two\nline three\n");
    let id = h.submit(&Harness::spec("claude", "dashboard"));
    h.wait_status(&id, "succeeded");

    let list = h.cmd().args(["--json", "task", "list"]).output().unwrap();
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let socket_list: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert!(
        socket_list["tasks"][0]["created_at"].is_string(),
        "{socket_list}"
    );

    let addr = dashboard_addr(&h);

    let tasks = http_get(&addr, "/v1/tasks");
    assert_eq!(tasks.status, 200, "{tasks:?}");
    assert!(tasks.content_type_contains("application/json"), "{tasks:?}");
    let body: Value = serde_json::from_str(&tasks.body).unwrap();
    let first = &body["tasks"][0];
    assert_eq!(first["id"], id, "{body}");
    assert!(first["created_at"].is_string(), "{body}");
    assert_eq!(first["timeout_secs"], 2 * 3600, "{body}");
    assert_eq!(
        first["workload"],
        json!({"type": "agent", "agent": "claude", "model": "fable"}),
        "{body}"
    );

    let log = http_get(&addr, &format!("/v1/tasks/{id}/log?tail=1"));
    assert_eq!(log.status, 200, "{log:?}");
    let body: Value = serde_json::from_str(&log.body).unwrap();
    assert_eq!(body["log"], "line three", "{body}");
    assert_eq!(body["truncated"], true, "{body}");

    // mutations stay on the 0600 socket; the TCP router rejects write methods here
    let submit = http_request(&addr, "POST", "/v1/tasks", None, None);
    assert_eq!(submit.status, 405, "{submit:?}");

    let index = http_get(&addr, "/");
    assert!(
        index.status == 200 || index.status == 503,
        "unexpected index status: {index:?}"
    );
    assert!(index.content_type_contains("text/html"), "{index:?}");

    let missing = http_get(&addr, "/v1/nope");
    assert_eq!(missing.status, 404, "{missing:?}");
    let body: Value = serde_json::from_str(&missing.body).unwrap();
    assert_eq!(body["error"]["code"], "not_found", "{body}");
}

#[test]
fn dashboard_file_browser_and_content_origin() {
    let h = Harness::with_dashboard();
    let addr = dashboard_addr(&h);
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("nested");
    fs::create_dir(&nested).unwrap();
    let text = nested.join("note.txt");
    fs::write(&text, b"hello files").unwrap();
    let html = nested.join("page.html");
    fs::write(
        &html,
        b"<html><body><a href=\"note.txt\">n</a></body></html>",
    )
    .unwrap();
    let bin = nested.join("blob.bin");
    fs::write(&bin, b"\0\x01\x02\x03binary").unwrap();

    let resolve = http_post_json(
        &addr,
        "/v1/files/resolve",
        &json!({ "path": nested.to_string_lossy() }),
    );
    assert_eq!(resolve.status, 200, "{resolve:?}");
    let resolved: Value = serde_json::from_str(&resolve.body).unwrap();
    assert_eq!(resolved["kind"], "directory");
    let token = resolved["token"].as_str().unwrap();

    let listing = http_get(&addr, &format!("/v1/files/{token}"));
    assert_eq!(listing.status, 200, "{listing:?}");
    let listing_body: Value = serde_json::from_str(&listing.body).unwrap();
    assert!(listing_body["entries"].as_array().unwrap().len() >= 3);

    let origin = http_get(&addr, "/v1/files/origin");
    assert_eq!(origin.status, 200, "{origin:?}");
    let origin_body: Value = serde_json::from_str(&origin.body).unwrap();
    let content_port = origin_body["port"].as_u64().unwrap() as u16;
    let content_host = addr.split(':').next().unwrap();
    let content_addr = format!("{content_host}:{content_port}");

    let mirrored = format!("/raw{}", text.to_string_lossy());
    let text_resp = http_get(&content_addr, &mirrored);
    assert_eq!(text_resp.status, 200, "{text_resp:?}");
    assert!(
        text_resp.content_type_contains("text/plain"),
        "{text_resp:?}"
    );
    assert!(
        text_resp.head.contains("content-disposition: inline"),
        "{text_resp:?}"
    );
    assert!(
        text_resp.head.contains("x-content-type-options: nosniff"),
        "{text_resp:?}"
    );
    assert!(
        text_resp
            .head
            .contains("cross-origin-resource-policy: same-origin"),
        "{text_resp:?}"
    );
    assert!(
        text_resp.head.contains("referrer-policy: no-referrer"),
        "{text_resp:?}"
    );
    assert_eq!(text_resp.body, "hello files");
    assert!(
        !text_resp.head.contains("access-control-allow-origin"),
        "{text_resp:?}"
    );

    let html_resp = http_get(&content_addr, &format!("/raw{}", html.to_string_lossy()));
    assert_eq!(html_resp.status, 200, "{html_resp:?}");
    assert!(
        html_resp.content_type_contains("text/html"),
        "{html_resp:?}"
    );
    assert!(
        html_resp.head.contains("content-disposition: inline"),
        "{html_resp:?}"
    );

    let bin_resp = http_get(&content_addr, &format!("/raw{}", bin.to_string_lossy()));
    assert_eq!(bin_resp.status, 200, "{bin_resp:?}");
    assert!(
        bin_resp.content_type_contains("application/octet-stream"),
        "{bin_resp:?}"
    );
    assert!(
        bin_resp.head.contains("content-disposition: attachment"),
        "{bin_resp:?}"
    );

    // content origin has no dashboard API
    let leaked = http_get(&content_addr, "/v1/tasks");
    assert_eq!(leaked.status, 404, "{leaked:?}");

    // LAN short names and mDNS reach both origins; public DNS names do not
    let lan_host = http_get_host(&addr, "/v1/status", "code.local");
    assert_eq!(lan_host.status, 200, "{lan_host:?}");
    let short_host = http_get_host(&addr, "/v1/status", "main");
    assert_eq!(short_host.status, 200, "{short_host:?}");
    let bad_host = http_get_host(&addr, "/v1/status", "evil.example");
    assert_eq!(bad_host.status, 400, "{bad_host:?}");
    let bad_content = http_get_host(&content_addr, &mirrored, "evil.example");
    assert_eq!(bad_content.status, 400, "{bad_content:?}");

    assert!(
        !origin.head.contains("access-control-allow-origin"),
        "{origin:?}"
    );
}
