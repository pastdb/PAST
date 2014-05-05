 % Code retrieved from http://www.cs.ucr.edu/~eamonn/iSAX/iSAX.html
 % Property of Jin Shieh and Eamonn Keogh
 % Used in the academic project PAST (Big Data 2014)
 
 function time_series = DNA2TimeSeries

time_series = 0;

fileName = input ( 'File name?  ','s');

fid = fopen(fileName,'rt');
DNA_STRING = fgets(fid);
fclose(fid);

   for i = 1 : length(DNA_STRING)

        if  DNA_STRING(i) =='a'   | DNA_STRING(i)== 'A'
            index = 2;
        elseif DNA_STRING(i)=='g' | DNA_STRING(i)=='G'
            index = 1;
        elseif DNA_STRING(i)=='c' | DNA_STRING(i)=='C'
            index = -1;
        elseif DNA_STRING(i)=='t' | DNA_STRING(i)=='T'
            index = -2;
        else
            disp('Found a non A,T,C,G!!!')
            index = 0;
        end
              
        time_series(i+1) =  time_series(i) + index;
        
   end  
    
    figure;
    hold on
    title(fileName)
    plot(time_series)
