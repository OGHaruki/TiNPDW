#include <stdio.h>
//#include <stdlib.h>
#include <omp.h>

int suma(int start, int end){
  unsigned long int wynik=0;
  #pragma omp parallel for reduction(+:wynik)
  for (int i = 0; i < start; i++) {
    for (int j = 0; j < end; j++) {
     wynik += i + j; }
  }
  return wynik;
}

int main () {
  int l;
  static int x[25000][25000];
  
  for (l=0;l<20;l++){
    #pragma omp parallel for
    for (int i = 0; i < 25000; i++) {
      for (int j = 0; j < 25000; j++) {
        x[j][i] = i*l + j; }
    }
  }
  int max_i = 25000 / 20;
  int max_j = 25000 / 20;
  printf("Suma= %d\n",suma(max_i,max_j));

  return 0;
}

