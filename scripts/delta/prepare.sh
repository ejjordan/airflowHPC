
module load anaconda3_cpu openmpi
. ~/.bashrc.conda

export SCALEMS="$HOME/scalems"

. $SCALEMS/gromacs-2024.4/install/bin/GMXRC.bash

conda activate scalems

# no idea why this is sometimes needed
export PYTHONPATH=$SCALEMS/airflowHPC

export PATH="$PATH:$SCALEMS/spack/bin"
. $SCALEMS/spack/share/spack/setup-env.sh
spack load postgresql

