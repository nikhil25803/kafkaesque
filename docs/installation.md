# Installation

Kafkaesque publishes checksummed binaries for Linux, macOS, and Windows on
AMD64 and ARM64. Container images are published for Linux AMD64 and ARM64.

## Linux and macOS

The installer detects the operating system and architecture, verifies the
release checksum, and installs to `~/.local/bin` without `sudo`:

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
kafkaesque --version
```

Set `INSTALL_DIR` to choose another destination:

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | \
  INSTALL_DIR="$HOME/bin" sh
```

## Windows

Download and run the PowerShell installer:

```powershell
curl.exe -fsSLo install.ps1 https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.ps1
powershell -ExecutionPolicy Bypass -File .\install.ps1
kafkaesque --version
```

The installer verifies the archive checksum and adds Kafkaesque to the current
user's `PATH`. Open a new terminal if the command is not immediately available.

## Pin a release

Installers use the latest release unless a version is supplied:

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil25803/kafkaesque/main/install.sh | \
  VERSION=<release-tag> sh
```

```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1 -Version <release-tag>
```

Release tags use the `vMAJOR.MINOR.PATCH` form. Choose a tag from
[GitHub Releases](https://github.com/nikhil25803/kafkaesque/releases).

## Verify a manual download

Download the archive and `checksums.txt` from the same GitHub Release, then
compare its SHA-256 checksum before extracting it:

```sh
sha256sum -c checksums.txt --ignore-missing
```

On macOS, use `shasum -a 256 <archive>` and compare it with the matching entry
in `checksums.txt`. On Windows, use `Get-FileHash -Algorithm SHA256 <archive>`.

## Container image

Pull an exact release for deployment:

```sh
docker pull ghcr.io/nikhil25803/kafkaesque:<release-tag>
```

Use `latest` only for local evaluation. The image runs as a non-root user,
contains the embedded UI, and starts `kafkaesque serve`. Server mode requires
Kafka, PostgreSQL, and authentication configuration; see
[Deployment](deployment.md).

## Build from source

Install the Go version declared in [`go.mod`](../go.mod), then run:

```sh
git clone https://github.com/nikhil25803/kafkaesque.git
cd kafkaesque
make check
./bin/kafkaesque --help
```

`make check` validates formatting, runs `go vet`, runs unit tests, and builds
`bin/kafkaesque`.

## Upgrade or remove

Rerun the installer with a newer pinned version to replace the binary. To
remove a script installation, delete the `kafkaesque` binary from its install
directory. Before upgrading a server deployment, follow the database and image
guidance in [Deployment](deployment.md#upgrades).
