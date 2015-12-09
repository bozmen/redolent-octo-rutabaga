#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
int main(int argc , char** argv){
	int i,j,k;
	i = atoi(argv[1]);
	j = atoi(argv[2]);
	k = atoi(argv[3]);
	char filename1[200];
	char filename2[200];
	srand((unsigned)time(NULL));
	/*
	*	In here, we will generate matrix A
	*/
	FILE *fp1, *fp2;
	sprintf(filename1,"%dx%d_1.txt", i, j);
	sprintf(filename2,"%dx%d_2.txt", j, k);

	fp1 = fopen(filename1,"w");
	int a,b,c;
	for(a = 0 ; a < i ; a++)
		for(b = 0 ; b < j ; b++){
			int value = rand() % 1000;
			fprintf(fp1,"%s %d %d %d\n","A", a, b, value);
		}
	fclose(fp1);


	fp2 = fopen(filename2, "w");
	for(b = 0 ; b < j ; b++)
		for(c = 0 ; c < k ; c++){
			int value = rand() % 1000;
			fprintf(fp2,"%s %d %d %d\n","B", b, c, value );
		}
	fclose(fp2);
}
