% Code retrieved from http://www.cs.ucr.edu/~eamonn/iSAX/iSAX.html
% Property of Jin Shieh and Eamonn Keogh
% Used in the academic project PAST (Big Data 2014)
 
 function time_series = translator(filename)

 time_series(1)=1;
DNA_STRING = importdata(filename);
DNA_STRING = DNA_STRING';
DNA_STRING=strjoin(DNA_STRING,'');
for i = 1 : length(DNA_STRING)

        if     DNA_STRING(i)=='a' | DNA_STRING(i)=='A'
            index = 2;
        elseif DNA_STRING(i)=='g' | DNA_STRING(i)=='G'
            index = 1;
        elseif DNA_STRING(i)=='c' | DNA_STRING(i)=='C'
            index = -1;
        elseif DNA_STRING(i)=='t' | DNA_STRING(i)=='T'
            index = -2;
        end
              
        time_series(i+1) =  time_series(i) + index;
        
   end 