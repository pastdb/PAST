 #!/bin/bash         
 
 s=`cat a.txt`
 size=${#s}
 
 let "c=($size/10)"
 
for (( i=0; i < size-$c; i+=$c )); do 
 printf "%s\n"  ${s:$i:$c} > 'input'$((i+$c))'.txt'
done