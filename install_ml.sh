 #!/bin/bash
 
 #   lda  stage
source /etc/oni.conf


#  copy solution files to all nodes 
if [$1 = '-o' or $1 = '--overwrite']; then
   EXCLUDES = "--exclude '*lda-c/'"
else
   EXCLUDES = ""
fi

for d in "${NODES[@]}" 
do 
     rsync -r --verbose $EXCLUDES  ${LUSER}/ml $d:${LUSER}/.    
done

