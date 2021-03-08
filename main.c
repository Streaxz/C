#include <stdio.h>
#include <sys/mman.h>
#include <pthread.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>
#include <semaphore.h>

#define A 214
#define B 0x8B1598B0
#define D 134
#define E 188
#define G 62
#define I 55
#define SUCCESS 0
#define ERROR_CREATE_THREAD -11
sem_t files_semaphore;
#define ERROR_JOIN_THREAD -12
//C=mmap; !F=block; H=seq; J=min; !K=sem

typedef struct{
    char* address;
    FILE* file;
    size_t size;
    int number;
} write_args;

typedef struct{
    char* filename;
    int number;

} aggregate_args;

char* allocate_memory(){
    return mmap((void*)B, A*1024*1024, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
}

void* generate_numbers(void* arg){
    write_args* args = (write_args*) arg;
    int count = 0;
    count = fread(args->address, args->size, 1, args->file);
    if(count == 1) return SUCCESS;
    else return (void*)1;
}

void fill_memory(char* addr){
    int i;
    int status;
    pthread_t threads[D];
    write_args args[D];
    int extra_size = A*1024*1024%D;
    int size = (A*1024*1024 - extra_size)/D;
    FILE* f = fopen("/dev/urandom", "rb");
    for(i=0; i<D; i++){
        args[i].address = addr + i*size;
        args[i].file = f;
        if(i == D-1) args[i].size = (size_t) (size + extra_size);
        else args[i].size = (size_t) size;
        status = pthread_create(&threads[i], NULL, generate_numbers, (void*)&args[i]);
        if(status != 0){
            printf("error: can't create thread number:%d\n", i+1);
            exit(ERROR_CREATE_THREAD);
        }
    }
    printf("filling the memory...\n");
    for(i=0; i<D; i++){
        pthread_join(threads[i], NULL);

    }
    printf("completed\n");
    fclose(f);

}

char* generate_filename(int number){
    char* str_number = malloc(sizeof(number) + 1);
    char* filename = malloc(sizeof("File") + sizeof(number) + 1);
    strcpy(filename, "File");
    snprintf(str_number, 10, "%d", number+1);
    strcat(filename, str_number);
    return filename;
}

void* write_thread(void* arg){
    write_args* args = (write_args*) arg;
    sem_init(&files_semaphore, 0, 0);
    int flags = O_WRONLY | O_CREAT | O_SYNC;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    char* filename = generate_filename(args->number);
    int fd = open(filename, flags, mode);
    write(fd, args->address, args->size);
    close(fd);
}

void write_in_files(char* address){
    int i;
    int last_size = A%E*1024*1024;
    int count = (A*1024*1024-last_size)/E/1024/1024;
    pthread_t* threads;
    write_args* args;
    threads = (pthread_t*) malloc((count+1) * sizeof(pthread_t));
    args = (write_args*) malloc((count+1) * sizeof(write_args));
    printf("start writing...\n");
    for(i=0; i<count; i++){
        args[i].address = address;
        args[i].size = E*1024*1024;
        args[i].number = i;
        pthread_create(&threads[i], NULL, write_thread, (void*)&args[i]);
        address += E*1024*1024;
    }
    args[i].address = address;
    args[i].size = last_size;
    args[i].number = i;
    pthread_create(&threads[i], NULL, write_thread, (void*)&args[i]);
    printf("completed\n");

}

void* aggregate_data(void* arg){
    while(1){
        sem_wait(&files_semaphore);
        aggregate_args* args = (aggregate_args*) arg;
        int fd = open(args->filename, O_RDONLY);
        int min = INT_MAX;
        char buffer[G];
        int i;
        int j;

        while(1){
            int count = read(fd, buffer, G);
            if(count==0) break;
            for(i=0; i<G/sizeof(int); i+=sizeof(int)){
                int num = 0;
                for(j=0; j<sizeof(int);j++){
                    num = (num << 8) + buffer[i+j];
                }
                if(min>num) min = num;
            }
        }


        close(fd);
    printf("thread%d min value from file: %s= %d\n", args->number, args->filename, min);
    }

}

void start_aggregate_threads(const int files_count){
    int i;
    aggregate_args args[I];
    pthread_t threads[I];
    printf("starting aggregating threads...\n");
    for(i=0; i<I; i++){// I
        int file_number = i%files_count;
        char* filename =

                generate_filename(file_number);
        args[i].number = i + 1;
        args[i].filename = filename;
        pthread_create(&threads[i], NULL, aggregate_data, (void*)&args[i]);

    }
/*for(i=0; i<1; i++){//I
pthread_join(threads[i], NULL);

}*/

}

int main(void){
    printf("before allocation\n");
    getchar();
    char* address = allocate_memory();
    printf("after allocation\n");
    getchar();
    int is_aggregate_start = 0;
    const int last_size = A%E*1024*1024;
    const int count_of_files = (A*1024*1024-last_size)/E/1024/1024 + 1;
    while(1){
        fill_memory(address);
        printf("after filling memory\n");
        getchar();
        write_in_files(address);
        if(!is_aggregate_start) start_aggregate_threads(count_of_files);
        is_aggregate_start = 1;
        printf("%p", address);
        sem_post(&files_semaphore);
        munmap(address, A*1024*1024);
        printf("after deallocation\n");
        getchar();
    }



}