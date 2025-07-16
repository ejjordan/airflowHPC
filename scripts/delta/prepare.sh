
echo "==== prepare.sh"
hostname

echo "load modules"
module load anaconda3_cpu openmpi
module list

echo "load conda"
. ~/.bashrc.conda

export SCALEMS="$HOME/scalems"

echo "load gmx"
. $SCALEMS/gromacs-2024.4/install/bin/GMXRC.bash

echo "activate conda"
conda activate scalems
which python3

# no idea why this is sometimes needed
export PYTHONPATH=$SCALEMS/airflowHPC

echo "load spack"
export PATH="$PATH:$SCALEMS/spack/bin"
. $SCALEMS/spack/share/spack/setup-env.sh
spack load postgresql

# echo "test mpirun"
# /sw/spack/deltas11-2023-03/apps/linux-rhel8-zen3/gcc-11.4.0/openmpi-4.1.6-lranp74/bin/mpirun -np 1 hostname

unset SLURM_CPUS_PER_TASK
# export SLURM_CPUS_PER_TASK=1
# env | sort | grep = > t.env

