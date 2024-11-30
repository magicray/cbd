#include <stdio.h>
#include <sys/ioctl.h>

void main() {
    for(int i=0; i<=10; i++) {
        printf("%d %d\n", i, _IO(0xab, i));
    }
}
