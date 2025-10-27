#!/bin/bash -l
#SBATCH --job-name=singletest
#SBATCH --partition=debug
#SBATCH --time=00:30:00
#SBATCH --ntasks-per-node=64
#SBATCH --cpus-per-task=1
#SBATCH -A project_465001666
#

# ./server_only.sh gmx_multi_weak gmx_multi 1     128   512   default
#./server_only.sh gmx_multi_weak gmx_multi 1     128   512   rct

#               name           dag   nodes   slots tasks   mode

source prepare.sh
cd airflowHPC/scripts/lumi
ssh -fN -R 8081:localhost:8081 uan04 &
./server_only.sh test anthracene_runner   1      32    32   default

# ./server_only.sh gmx_multi_weak gmx_multi 1      32   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 1      32   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 1      32   512   default
#                                                         
# ./server_only.sh gmx_multi_weak gmx_multi 1      64   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 1      64   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 1      64   512   default
                                                        
##./server_only.sh gmx_multi_weak gmx_multi 1     128   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 1     128   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 1     128   512   default
#                                                         
# ./server_only.sh gmx_multi_weak gmx_multi 2     256   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 2     256   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 2     256   512   default
#                                                         
# ./server_only.sh gmx_multi_weak gmx_multi 4     512   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 4     512   512   default
# ./server_only.sh gmx_multi_weak gmx_multi 4     512   512   default


# ./server_only.sh gmx_multi_weak gmx_multi 1      32   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 1      32   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 1      32   512   rct
#                                                         
# ./server_only.sh gmx_multi_weak gmx_multi 1      64   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 1      64   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 1      64   512   rct
                                                       
##./server_only.sh gmx_multi_weak gmx_multi 1     128   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 1     128   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 1     128   512   rct
#                                                       
# ./server_only.sh gmx_multi_weak gmx_multi 2     256   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 2     256   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 2     256   512   rct
#                                                      
# ./server_only.sh gmx_multi_weak gmx_multi 4     512   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 4     512   512   rct
# ./server_only.sh gmx_multi_weak gmx_multi 4     512   512   rct
sleep 1800
