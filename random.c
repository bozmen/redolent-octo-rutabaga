#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
int main(int argc , char** argv){
	int i,j,k;
	i = atoi(argv[1]);
	j = atoi(argv[2]);
	k = atoi(argv[3]);
	char str[200];
	srand((unsigned)time(NULL));
	/*
	*	In here, we will generate matrix A
	*/
	FILE *fp;
	sprintf(str,"%dx%d_%dx%d.txt", i, j, j, k);
	fp = fopen(str,"w");
	int a,b,c;
	for(a = 0 ; a < i ; a++)
		for(b = 0 ; b < j ; b++){
			int value = rand() % 1000;
			fprintf(fp,"%s %d %d %d\n","A", a, b, value);
		}


	for(b = 0 ; b < j ; b++)
		for(c = 0 ; c < k ; c++){
			int value = rand() % 1000;
			fprintf(fp,"%s %d %d %d\n","B", b, c, value );
		}
	fclose(fp);
}
