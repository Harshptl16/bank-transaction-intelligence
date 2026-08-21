# VS Code Setup Guide

Getting the Bank Transaction Intelligence Platform running locally.

Covers Windows, macOS, and Linux. Windows has the most friction with PySpark — the Hadoop binaries section is not optional there.

---

## 1. Prerequisites

### Java (required — PySpark runs on the JVM)

**Install Java 17.** Spark 3.5 officially supports Java 8, 11, and 17. Java 21 often works but isn't officially supported until Spark 4.0, and when it breaks the error messages are unhelpful. Save yourself the debugging.

| OS | Command |
|---|---|
| Windows | Download [Eclipse Temurin 17](https://adoptium.net/temurin/releases/?version=17) (MSI installer) |
| macOS | `brew install --cask temurin@17` |
| Linux | `sudo apt install openjdk-17-jdk` |

Verify:
```bash
java -version
# should print: openjdk version "17.x.x"
```

### Set JAVA_HOME

Spark reads this directly. If it's unset or points at the wrong JDK, you get a `JAVA_HOME is not set` error or a cryptic gateway failure.

**Windows (PowerShell, permanent):**
```powershell
[System.Environment]::SetEnvironmentVariable(
    "JAVA_HOME",
    "C:\Program Files\Eclipse Adoptium\jdk-17.0.13.11-hotspot",
    "User"
)
```
Adjust the path to match your actual install. Restart VS Code after setting it.

**macOS / Linux (add to `~/.zshrc` or `~/.bashrc`):**
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # macOS
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 # Linux
```

### Python 3.10–3.12

Python 3.13 has had PySpark compatibility issues. 3.11 or 3.12 is the safe choice.

```bash
python --version
```

---

## 2. Windows only — Hadoop binaries

**Skip this section on macOS/Linux.**

PySpark on Windows needs `winutils.exe` and `hadoop.dll` even though you aren't running Hadoop. Without them you get:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
```
or a `NullPointerException` deep in `Shell.getWinUtilsPath` when writing Parquet.

**Setup:**

1. Create `C:\hadoop\bin`
2. Download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x from [cdarlint/winutils](https://github.com/cdarlint/winutils) (grab from the `hadoop-3.3.x/bin/` folder)
3. Place both files in `C:\hadoop\bin`
4. Set the environment variable:

```powershell
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
```

5. Add `C:\hadoop\bin` to your `PATH`
6. Restart VS Code

**Also enable long paths** — Spark's Parquet output creates deeply nested directories that blow past Windows' 260-character limit:

```powershell
# Run as Administrator
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" `
    -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```

---

## 3. Project setup

```bash
# Clone or create the project directory
cd bank-transaction-intelligence

# Create a virtual environment
python -m venv .venv

# Activate it
.venv\Scripts\Activate.ps1      # Windows PowerShell
source .venv/bin/activate        # macOS / Linux

# Install dependencies
pip install -r requirements.txt
```

If PowerShell blocks the activation script:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## 4. VS Code extensions

Install these from the Extensions panel (`Ctrl+Shift+X`):

| Extension | Publisher | Why |
|---|---|---|
| **Python** | Microsoft | Interpreter selection, debugging, IntelliSense |
| **Pylance** | Microsoft | Fast type checking and autocomplete |
| **Jupyter** | Microsoft | Interactive Spark exploration in notebooks |
| **PostgreSQL** | Chris Kolkman | Browse the warehouse, run queries without leaving the editor |
| **Even Better TOML** | tamasfe | If you add `pyproject.toml` later |
| **Rainbow CSV** | mechatroner | Makes raw PaySim CSVs readable |
| **Azure Account** + **Azure Storage** | Microsoft | Phase 2 — browse Blob Storage containers inline |

---

## 5. VS Code configuration

Create `.vscode/settings.json`:

```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.terminal.activateEnvironment": true,
  "python.analysis.typeCheckingMode": "basic",
  "python.analysis.extraPaths": ["${workspaceFolder}"],
  "files.exclude": {
    "**/__pycache__": true,
    "**/spark-warehouse": true,
    "**/.pytest_cache": true
  },
  "files.watcherExclude": {
    "**/data/**": true,
    "**/spark-warehouse/**": true
  },
  "terminal.integrated.env.windows": {
    "PYSPARK_PYTHON": "${workspaceFolder}\\.venv\\Scripts\\python.exe",
    "PYSPARK_DRIVER_PYTHON": "${workspaceFolder}\\.venv\\Scripts\\python.exe"
  },
  "terminal.integrated.env.linux": {
    "PYSPARK_PYTHON": "${workspaceFolder}/.venv/bin/python",
    "PYSPARK_DRIVER_PYTHON": "${workspaceFolder}/.venv/bin/python"
  },
  "terminal.integrated.env.osx": {
    "PYSPARK_PYTHON": "${workspaceFolder}/.venv/bin/python",
    "PYSPARK_DRIVER_PYTHON": "${workspaceFolder}/.venv/bin/python"
  }
}
```

On Windows, change `defaultInterpreterPath` to `${workspaceFolder}\\.venv\\Scripts\\python.exe`.

> **Why `PYSPARK_PYTHON` matters:** Spark launches Python worker processes separately from the driver. If these aren't set, workers may launch with your *system* Python instead of the venv one, and you get `ModuleNotFoundError` for packages that are clearly installed. It's a confusing failure — set these up front.

The `files.watcherExclude` on `data/**` matters too. Without it, VS Code tries to watch every Parquet file Spark writes and the editor stalls.

---

## 6. Debug configuration

Create `.vscode/launch.json` so you can set breakpoints in the PySpark code:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Generate sample data",
      "type": "debugpy",
      "request": "launch",
      "program": "${workspaceFolder}/etl/generate_sample.py",
      "args": ["--rows", "20000", "--out", "data/raw/paysim_tiny.csv"],
      "console": "integratedTerminal",
      "justMyCode": true
    },
    {
      "name": "Run transform (tiny)",
      "type": "debugpy",
      "request": "launch",
      "program": "${workspaceFolder}/etl/transform.py",
      "args": [
        "--input", "data/raw/paysim_tiny.csv",
        "--output", "data/processed",
        "--shuffle-partitions", "8"
      ],
      "console": "integratedTerminal",
      "envFile": "${workspaceFolder}/.env",
      "justMyCode": true
    },
    {
      "name": "Run transform (full PaySim)",
      "type": "debugpy",
      "request": "launch",
      "program": "${workspaceFolder}/etl/transform.py",
      "args": [
        "--input", "data/raw/PS_20174392719_1491204439457_log.csv",
        "--output", "data/processed",
        "--shuffle-partitions", "200"
      ],
      "console": "integratedTerminal",
      "envFile": "${workspaceFolder}/.env",
      "justMyCode": true
    }
  ]
}
```

`envFile` loads `.env` automatically, so `MASKING_SALT` is picked up without exporting it each session.

> **Breakpoint caveat:** breakpoints work in driver code (anything outside a Spark transformation). They will *not* hit inside UDFs or lambdas passed to `.map()` — those execute in separate worker JVMs. Debug those by calling `.show()` / `.printSchema()` on intermediate DataFrames instead.

---

## 7. Environment file

```bash
cp .env.example .env
```

Edit `.env` and set a real salt:

```
MASKING_SALT=some-long-random-string-you-generate-once
```

Generate one:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

**Keep this value stable.** Change it and every account hashes differently, meaning your next pipeline run creates an entirely new set of dimension rows that don't match the existing ones. `.env` is already in `.gitignore` — leave it there.

---

## 8. Verify the setup

```bash
# Should print your Spark version without errors
python -c "import pyspark; print(pyspark.__version__)"

# Full smoke test
python etl/generate_sample.py --rows 20000 --out data/raw/paysim_tiny.csv
python etl/transform.py --input data/raw/paysim_tiny.csv --output data/processed
```

Expected output:
```
[clean] 20,000 rows in -> 20,000 rows out (0 dropped)
[build] dim_account:  2,500 rows
[build] dim_merchant:   500 rows
[build] dim_date:       744 rows
[build] fact:        20,000 rows
[validate] all checks passed
[write] output written to data/processed/
```

First run takes 1–3 minutes — the JVM startup and Spark session initialization dominate on a dataset this small. That's expected, not a problem with your setup.

---

## 9. Connecting to the warehouse (Phase 3+)

Once Azure Postgres is provisioned, add the connection in the PostgreSQL extension:

- **Host:** `your-server.postgres.database.azure.com`
- **Port:** `5432`
- **Database:** `bank_intelligence`
- **Username:** your admin user
- **SSL:** **required** — Azure Database for PostgreSQL enforces SSL by default. Connections without it fail with a non-obvious error.

You can then run `warehouse/schema.sql` directly from the editor.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `JAVA_HOME is not set` | Java missing or env var unset | Section 1 |
| `Unsupported class file major version 65` | Java 21 with Spark 3.5 | Install Java 17 |
| `HADOOP_HOME and hadoop.home.dir are unset` | Windows, missing winutils | Section 2 |
| `NullPointerException` in `Shell.getWinUtilsPath` | Missing `hadoop.dll` | Section 2 — both files needed, not just `winutils.exe` |
| `ModuleNotFoundError` for an installed package | Workers using system Python | Set `PYSPARK_PYTHON` — Section 5 |
| `MASKING_SALT environment variable is not set` | `.env` not loaded | Use the launch config, or `export` manually |
| Editor stalls after running the transform | VS Code watching Parquet output | `files.watcherExclude` — Section 5 |
| Path-too-long errors on write | Windows 260-char limit | Enable long paths — Section 2 |
| Transform takes 10+ minutes on small data | Default 200 shuffle partitions | `--shuffle-partitions 8` |

---

## Optional: Jupyter for exploration

The `.ipynb` workflow is genuinely useful for Phase 5's SQL analysis, where you want to iterate on queries and see results inline.

```bash
pip install jupyter ipykernel
python -m ipykernel install --user --name bank-intel --display-name "Bank Intelligence"
```

Then create notebooks in an `notebooks/` directory and select that kernel. Add `notebooks/*.ipynb` outputs to `.gitignore` — committed notebook output makes diffs unreadable.
