function [start finish] = linearSeq(A,seq)

start=1;
finish=1;
length=size(seq)
size(A)
min=Inf;
for i=1:1:size(A,2)-length(2);
    
    result=sum(A(1,i:i+length)-seq(2));
    
    if(result<min)
        min=result;
        start=i;
        finish=i+length;
    end;
    
end