1. Create a new directory in flink called **template-slurm**
2. Copy the contents of flink/conf into flink/template-slurm
3. Replace the template-slurm/flink-conf.yaml with this flink-conf.yaml
4. export FLINK_HOME environment variable pointing to flink directory
5. Run **sbatch script.slurm**