 #!/bin/bash
 
 #   lda  stage
source /etc/oni.conf


#  copy solution files to all nodes 
if [$1 = '-x' or $1 = '--excludelda']; then
   EXCLUDES = "--exclude '*lda-c/'"
else
   EXCLUDES = ""
fi

for d in "${NODES[@]}" 
do 
     rsync -r --copy-links --verbose $EXCLUDES  ${LUSER}/ml $d:${LUSER}/.    
done

