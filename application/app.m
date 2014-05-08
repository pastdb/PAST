

chunkTS=translator('Test.txt');
humanTS=translator('human.txt');
chimpTS=translator('chimp.txt');

polarTS=translator('polar.txt');
hippoTS=translator('hippo.txt');

[a b]=linearSeq(chunkTS,chimpTS);

figure();

hold on;

title('Polar Bear and Hippopotamus mitochondria DNA');
plot(polarTS,'r');
plot(hippoTS);

hold off;

figure();

hold on;
title('Human and chimpanzee chromosome 2 DNA');
plot(humanTS,'r');
plot(chimpTS);

hold off;