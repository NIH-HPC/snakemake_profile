# Snakemake profile for Biowulf — Snakemake ≥ 9

Minimal [Snakemake profile](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles)
for the [NIH Biowulf](https://hpc.nih.gov) cluster using the
[snakemake-executor-plugin-slurm](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html).

> **Requires Snakemake ≥ 9 and `snakemake-executor-plugin-slurm`.**
> For older Snakemake, use the `main` branch of this repository.

---

## Installation

```bash
# 1. Install Snakemake ≥ 9 and the SLURM executor plugin
module load snakemake  # or: pip/conda install snakemake snakemake-executor-plugin-slurm

# 2. Install the profile
mkdir -p ~/.config/snakemake
git clone --branch snakemake9 \
    https://github.com/NIH-HPC/snakemake_profile.git \
    ~/.config/snakemake/biowulf
```

Then run any workflow with:

```bash
snakemake --profile biowulf [other options]
```

---

## How it works

This profile sets `executor: slurm` and sensible Biowulf defaults in `config.yaml`.
Job submission, status polling, and cancellation are all handled by the executor plugin —
no custom Python scripts are needed.

All resource information is taken from rule `threads:` and `resources:` directives.

---

## Resources

### Standard resources

| Resource | Description | Default |
|---|---|---|
| `threads` | CPUs per task (`--cpus-per-task`) | 1 |
| `mem_mb` | Memory in MB | 4096 |
| `runtime` | Wall time in **minutes** | 120 |
| `slurm_partition` | Override partition | `norm` |

### lscratch (local scratch disk)

Biowulf's local scratch (`/lscratch/$SLURM_JOB_ID`) must be requested explicitly via
`--gres=lscratch:N` (N in GB). The executor plugin does **not** translate `disk_mb`
automatically. Use `slurm_extra` in the rule:

```python
rule with_scratch:
    output: "results/big_output"
    threads: 8
    resources:
        mem_mb=16384,
        runtime=240,
        disk_mb=102400,           # informational for Snakemake scheduling
        slurm_extra="'--gres=lscratch:100'"   # 100 GB; sets $TMPDIR automatically
    shell:
        "echo $TMPDIR; my_tool --tmp $TMPDIR ..."
```

To avoid repeating `slurm_extra` in every rule, define a helper in your Snakefile:

```python
def lscratch(gb):
    return f"'--gres=lscratch:{gb}'"
```

Then use `slurm_extra=lscratch(100)` in each rule.

### GPUs

The executor plugin maps `gpu` + `gpu_model` resources to `--gres=gpu:MODEL:N`.

```python
rule gpu_job:
    output: "results/gpu_output"
    threads: 8
    resources:
        mem_mb=32768,
        runtime=480,
        slurm_partition="gpu",
        gpu=1,
        gpu_model="a100"        # → --gres=gpu:a100:1
    shell:
        "nvidia-smi; my_gpu_tool ..."
```

For GPU jobs where you want to select by constraint (feature flag) rather than model name,
use `slurm_extra`:

```python
resources:
    slurm_partition="gpu",
    gpu=1,
    slurm_extra="'--constraint=gpua100&gpunvidia'"
```

### Multiple tasks (MPI)

```python
rule mpi_job:
    output: "results/mpi_output"
    threads: 1              # threads per task
    resources:
        mem_mb=8192,
        runtime=600,
        slurm_extra="'--ntasks=16 --nodes=2'"
    shell:
        "mpirun -np 16 my_mpi_tool ..."
```

---

## Full example Snakefile

```python
rule all:
    input:
        "tests/norm",
        "tests/quick",
        "tests/force_norm",
        "tests/gpu",
        "tests/scratch",

rule norm:
    output: "tests/norm"
    threads: 10
    resources:
        runtime=600,
        mem_mb=1024
    shell: "touch {output}"

rule quick:
    output: "tests/quick"
    threads: 4
    resources:
        runtime=10,
        mem_mb=1024
    shell: "touch {output}"

rule force_norm:
    output: "tests/force_norm"
    threads: 10
    resources:
        runtime=10,
        mem_mb=1024,
        slurm_partition="norm"
    shell: "touch {output}"

rule gpu:
    output: "tests/gpu"
    threads: 8
    resources:
        runtime=60,
        mem_mb=16384,
        slurm_partition="gpu",
        gpu=1,
        gpu_model="a100"
    shell: "touch {output}"

rule scratch:
    output: "tests/scratch"
    threads: 4
    resources:
        runtime=30,
        mem_mb=4096,
        disk_mb=51200,
        slurm_extra="'--gres=lscratch:50'"
    shell: "echo $TMPDIR; touch {output}"
```

Run with:

```bash
snakemake --profile biowulf
```

---

## Profile hierarchy

You can layer a workflow-level profile on top of the system profile for per-project
defaults. Create a `config.yaml` next to your `Snakefile`:

```yaml
# workflow-level profile — overrides ~/.config/snakemake/biowulf/config.yaml
default-resources:
  mem_mb: 8192
  runtime: 60
set-resources:
  big_rule:
    mem_mb: 128000
    runtime: 1440
    slurm_partition: "largemem"
```

Then run:

```bash
snakemake --profile biowulf --workflow-profile .
```

---

## What changed from the old profile (Snakemake < 8)

| Old | New |
|---|---|
| `slurm-submit.py` | Removed — handled by `snakemake-executor-plugin-slurm` |
| `slurm-status.py` | Removed — plugin polls via `sacct`/`squeue` |
| `slurm-jobscript.sh` | Removed |
| `cluster:` key in `config.yaml` | Replaced by `executor: slurm` |
| `disk_mb` auto-mapped to lscratch | Must use `slurm_extra="'--gres=lscratch:N'"` |
| `gpu` + `gpu_model` custom logic | Native plugin support (`gpu` + `gpu_model` resources) |
| `ntasks` resource | Use `slurm_extra="'--ntasks=N'"` |
