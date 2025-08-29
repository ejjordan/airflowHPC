
module load cray-python/3.10.10
module list

export SCALEMS="/pfs/lustrep3/scratch/project_465001998/scalems"

# no idea why this is sometimes needed
export PYTHONPATH=$SCALEMS/airflowHPC

export PATH="$PATH:$HOME/j/spack/bin"
. $HOME/j/spack/share/spack/setup-env.sh
spack load postgresql
spack load gromacs

