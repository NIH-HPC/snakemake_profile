
Snakemake profile for biowulf
================================================================================

> [!IMPORTANT]
>
> This is a profile for snakemake<8
> 
> For an experimental snakemake>=8 profile please use the snakemake8 branch



### Overview
- obtains job status from dashboard data to minimize slurm queries
- obtains all information required to submit rules as slurm jobs from rule
  threads and resources. The partition to submit to is inferred from the
  required resources.

### Usage

The `threads` keyword is translated to `--cpus-per-task`

**Required resources:**
- `mem_mb=#` required memory in megabytes. Translates to `--mem`
 
**Optional resources:** 
- `disk_mb=#` translates to `--gres=lscratch`
- `gpu=#` Number of gpus needed. If no `gpu_model` is given translates to `--gres=gpu:#`
- `gpu_model="MODEL"` Which gpu to use. Translates to `--gres=gpu:MODEL:#`. If the string contains
  a "|" it is interpreted as a constraint and the gpu allocation is translated to 
  `--gres=gpu:# --constraint=MODEL`. Note that for the latter use the feature names for the
  gpus have to be used. On biowulf these start with 'gpu'. See the example below
- `runtime=#` Runtime of the rule. Translates to `--time=#`
- `ntasks` and `nodes`


**TODO:**
- add a config file to supply additional sbatch options (e.g. constraints).


### Example

```console
$ cat <<'__EOF__' > Snakefile
rule all:
    input: "tests/norm", "tests/force_norm", "tests/quick", "tests/gpu", "tests/gpu2", "tests/tasks", "tests/ntasks"

rule clean:
    shell: "rm -f tests/* logs/*"

rule norm:
    output: "tests/norm"
    threads: 10
    resources: runtime=600, mem_mb=1024
    shell: "echo $TMPDIR ; touch {output}"

rule quick:
    output: "tests/quick"
    threads: 10
    resources: runtime=10, mem_mb=1024, disk_mb=10240
    shell: "touch {output}"

rule force_norm:
    output: "tests/force_norm"
    threads: 10
    resources: runtime=10, mem_mb=1024, disk_mb=10240, slurm_partition="norm"
    shell: "touch {output}"

rule gpu:
    output: "tests/gpu"
    threads: 10
    resources: runtime=10, mem_mb=1024, disk_mb=10240, gpu=1, gpu_model="k80"
    shell: "touch {output}"

rule gpu2:
    output: "tests/gpu2"
    threads: 10
    resources: runtime=10, mem_mb=1024, disk_mb=10240, gpu=1, gpu_model="[gpuk80|gpup100]"
    shell: "set -x ; touch {output}"

rule tasks:
    output: "tests/tasks"
    resources: tasks=2
    shell: "touch {output}"

rule ntasks:
    output: "tests/ntasks"
    resources: ntasks=2
    shell: "touch {output}"


__EOF__

$ module load snakemake
$ git clone https://github.com/NIH-HPC/snakemake_profile.git
$ snakemake --profile snakemake_profile
```
