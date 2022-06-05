#include<stdio.h>
#include<ctime>
int main()
{   
    clock_t st,ed;
    st=clock();
    int i=100000000;
    while(i--);
    ed=clock();
    printf("helo = %d\n",(int)ed-st);
}