use crate::netns::Netns;
use anyhow::anyhow;
use anyhow::Result;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::str;
use std::thread::sleep;
use std::time;

pub fn exec(mut cmd: Command) -> Result<String> {
    println!("executing {:?}", cmd);
    let out = cmd.output()?;
    let stdout: String = str::from_utf8(&out.stdout)?.into();
    let stderr = str::from_utf8(&out.stderr)?;
    if !out.status.success() {
        println!("stdout: {}", stdout);
        println!("stderr: {}", stderr);
        return Err(anyhow!(
            "failed to execute with error {}: \nstderr: {}\nstdout: {}",
            out.status,
            stderr,
            stdout
        ));
    }
    Ok(stdout)
}

pub fn exec_with_timeout(mut cmd: Command, timeout: time::Duration) -> Result<String> {
    println!("executing with timeout {:?}", cmd);
    let t = time::Instant::now();
    let mut child = cmd.stdout(Stdio::piped()).spawn()?;
    loop {
        if time::Instant::now().duration_since(t).as_millis() > timeout.as_millis() {
            return Err(anyhow!("timeout"));
        }
        let w = child.try_wait();
        if let Ok(Some(v)) = w {
            if time::Instant::now().duration_since(t).as_millis() > timeout.as_millis() {
                // have to do this since zdbfs will probably block for read commands
                // and try wait might block (didn't try it)
                return Err(anyhow!("timeout"));
            } else if v.success() {
                let stdout = child.stdout;
                let mut buf = String::new();
                stdout.unwrap().read_to_string(&mut buf)?;
                return Ok(buf);
            } else {
                return Err(anyhow!("exec with timeout failed"));
            }
        } else if let Ok(None) = w {
            sleep(time::Duration::from_secs(1));
        } else if let Err(e) = w {
            println!("failed to wait {}", e);
            sleep(time::Duration::from_secs(1));
        }
    }
}

pub fn wait_server(
    endpoint: String,
    port: u16,
    ns: Option<Netns>,
    timeout: time::Duration,
) -> Result<()> {
    println!("waiting for the server {}:{}", endpoint, port);
    let t = time::Instant::now();
    loop {
        if time::Instant::now().duration_since(t).as_millis() > timeout.as_millis() {
            return Err(anyhow!("timeout"));
        }

        let mut cmd = Command::new("nc");
        cmd.arg("-z").arg(&endpoint).arg(format!("{}", port));
        if let Some(ref v) = ns {
            cmd = v.wrap(cmd);
        }
        match exec(cmd) {
            Err(e) => println!("error waiting server: {}", e),
            Ok(_) => {
                let duration = time::Duration::from_millis(1000);
                sleep(duration);
                return Ok(());
            }
        }
        let duration = time::Duration::from_millis(2000);
        sleep(duration);
    }
}

pub fn wait_zstor(endpoint: String, ns: Option<Netns>, timeout: time::Duration) -> Result<()> {
    println!("waiting for the server {}", endpoint);
    let t = time::Instant::now();
    loop {
        if time::Instant::now().duration_since(t).as_millis() > timeout.as_millis() {
            return Err(anyhow!("timeout"));
        }
        let mut cmd = Command::new("stat");
        cmd.arg(&endpoint);
        if let Some(ref v) = ns {
            cmd = v.wrap(cmd);
        }
        match exec(cmd) {
            Err(e) => println!("error waiting zstor: {}", e),
            Ok(_) => {
                let duration = time::Duration::from_millis(1000);
                sleep(duration);
                return Ok(());
            }
        }
        let duration = time::Duration::from_millis(2000);
        sleep(duration);
    }
}

pub fn wait_mountpoint(mountpoint: &Path) {
    println!("waiting for the server {}", mountpoint.to_str().unwrap());
    loop {
        let mut cmd = Command::new("mountpoint");
        cmd.arg(mountpoint.to_str().unwrap());
        match exec(cmd) {
            Err(e) => println!("error waiting mountpoint: {}", e),
            Ok(_) => {
                let duration = time::Duration::from_millis(1000);
                sleep(duration);
                return;
            }
        }
        let duration = time::Duration::from_millis(2000);
        sleep(duration);
    }
}

pub fn wait_mountpoint_released(path: &Path, timeout: time::Duration) -> Result<()> {
    let t = time::Instant::now();
    loop {
        if time::Instant::now().duration_since(t).as_millis() > timeout.as_millis() {
            return Err(anyhow!("mountpoint didn't stop in time"));
        }

        let mut cmd = Command::new("mountpoint");
        cmd.arg(path.to_str().unwrap());
        if let Err(e) = exec(cmd) {
            println!("probing mountpoint errored: {}", e);
            if e.to_string().contains("is not a mountpoint") {
                return Ok(());
            } else {
                println!("wait mountpoint released: {}", e);
            }
        }
        let duration = time::Duration::from_millis(2000);
        sleep(duration);
    }
}

pub fn tempfile() -> String {
    let tempcmd = Command::new("mktemp");
    let temp = exec(tempcmd).expect("failed to create temp file for the image");
    temp.trim().into()
}

pub fn tempdir() -> String {
    let mut tempcmd = Command::new("mktemp");
    tempcmd.arg("-d");
    let temp = exec(tempcmd).expect("failed to create temp file for the image");
    temp.trim().into()
}

pub fn sed(path: &Path, src: &str, dst: &str) {
    let mut cmd = Command::new("sed");
    cmd.arg("-i")
        .arg(format!("s/{}/{}/g", src, dst))
        .arg(path.to_str().unwrap());
    exec(cmd).expect("replacing in file");
}

pub fn graceful_shutdown(proc: &mut Child, timeout: time::Duration) -> Result<()> {
    let t = time::Instant::now();
    loop {
        let w = proc.try_wait();
        if let Ok(Some(_)) = w {
            return Ok(());
        }
        if time::Instant::now().duration_since(t).as_millis() > timeout.as_millis() {
            let _ = proc.kill();
            if proc.wait().is_ok() {
                return Ok(());
            } else {
                return Err(anyhow!("failed to stop process"));
            }
        }

        // it's still running
        let mut cmd = Command::new("kill");
        cmd.arg(format!("{}", proc.id()));
        if let Err(e) = exec(cmd) {
            if e.to_string().contains("no such process") {
                return Ok(());
            } else {
                return Err(e);
            }
        }
        let duration = time::Duration::from_millis(2000);
        sleep(duration);
    }
}

pub fn write_random_file(path: &Path, size_in_mb: u32) -> Result<()> {
    let mut cmd = Command::new("dd");
    cmd.arg("if=/dev/urandom")
        .arg(format!("of={}", path.to_str().unwrap()))
        .arg("bs=1M")
        .arg(format!("count={}", size_in_mb));
    exec(cmd)?;
    Ok(())
}

pub fn checksum_file(path: &Path) -> Result<String> {
    let mut cmd = Command::new("md5sum");
    cmd.arg(path.to_str().unwrap());
    exec(cmd)
}

pub fn checksum_file_with_timeout(path: &Path, timeout: time::Duration) -> Result<String> {
    let mut cmd = Command::new("md5sum");
    cmd.arg(path.to_str().unwrap());
    exec_with_timeout(cmd, timeout)
}

pub fn remove_dir_contents(path: &PathBuf) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if entry.file_type()?.is_dir() {
            remove_dir_contents(&path)?;
            fs::remove_dir(path)?;
        } else {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}
pub fn count_procs(name: &str) -> u32 {
    let mut cmd = Command::new("ps");
    cmd.arg("aux");
    let output = exec(cmd);
    if let Err(e) = output {
        println!("checking number of {} procs: {}", name, e);
        return 0;
    }
    let count = output.as_ref().unwrap().matches(name).count() as u32;
    println!("output: {}, count: {}", output.unwrap(), count);
    count
}
