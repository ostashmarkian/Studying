#include<iostream>
#include<ctime>
#include<time.h>
using namespace std;


int main(){
    srand((unsigned)time(NULL));
    int row;
    int col;
    cout << "row"; cin >> row;
    cout << "col"; cin >> col;
        int** Q = new int*[row];
    for (int i = 0; i < row; i++)
        Q[i] = new int[col];
    
}
void Create()
