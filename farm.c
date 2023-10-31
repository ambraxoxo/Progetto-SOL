#include "boundedqueue.h"
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

BQueue_t* taskqueue;
//socket per comunicazione con Collector, passo il socket ai thread? oppure struttura con riferimento alla coda e alla socket 

/*typedef struct threadArgs{
    BQueue_t* q;
    //socket?    
}threadArgs_t;*/

typedef struct fileType{
    char* name;
    size_t size;
}fileType_t;

typedef struct resType{
    char* filename;
    long result;
} resType_t;

static void *worker(void* args){
    fflush(stdout);
    long res;
    int n_long;
    //BQueue_t *q = ((threadArgs_t*)args)->q;
    
    fileType_t* el = pop(taskqueue);
    if(el == NULL){
        printf("pop\n");
        exit(EXIT_FAILURE);
    }
    FILE* file; 
    n_long = (el->size)/8;
    long buff[n_long];
    if((file = fopen(el->name, "r")) == NULL){
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    if(fread(buff, sizeof(long), n_long, file) == 0){
        perror("fread");
        fclose(file);
        exit(EXIT_FAILURE);
    }

    for(int i = 0; i < n_long; i++){
        res += i*buff[i];
    }
    resType_t r;
    r.filename = el->name;
    r.result = res;
    printf("%ld\n", res);
}

int isRegular(const char filename[], size_t* size){
    struct stat statbuf;
    size_t fsize;

    if(stat(filename, &statbuf) == 0){
        if(S_ISREG(statbuf.st_mode)){    
            if(size != NULL){ 
                *size = statbuf.st_size;
                return 1;
            }
        }
    }
    return 0;
}

int isDir(const char filename[]){
    struct stat statbuf;
    
    if(S_ISDIR(statbuf.st_mode)) return 1;
    else return 0;
}

int main(int argc, char *argv[]){
    if( argc < 2 ){
        printf("Uso: %s [file list]", argv[0]);
        return -1;
    }
    int nThreads = 4;
    int qLen = 8;
    int delay = 0;
    int opt;

    while((opt = getopt(argc, argv, "n:q:t:")) != -1){
        switch (opt){
        case 'n':
            nThreads = atoi(optarg);
            break;
        case 'q':
            qLen = atoi(optarg);
            break;
        case 't':
            delay = atoi(optarg)*1000;
            break;        
        default:
            break;
        }
    }
    int pid;
    pid = fork();

    if(pid != 0){
        //MasterWorker
        
        fflush(stdout);
        taskqueue = initBQueue(qLen);
          
        pthread_t* workers = malloc(nThreads*sizeof(pthread_t)); 
        //threadArgs_t args;      
        if(!workers){
            perror("malloc fallita");
            return -1;
        }
        for(int i = 0; i < nThreads; i++){
            fflush(stdout);
            if(pthread_create(&workers[i], NULL, &worker, NULL) == -1){
                perror("thread create");
                exit(EXIT_FAILURE);
            }
            
        }

        for(int i = optind; i < argc; i++){
            if(argv[i][1] == 'd'){
                if(isDir(&argv[++i])){
                    DIR * d;
                    struct dirent* ent;
                    if((d = opendir(argv[i]) == NULL)){
                        perror("apertura directory");
                        exit(EXIT_FAILURE);
                    }
                    while((errno = 0, ent = readdir(d)) != NULL){
                        
                    }
                }
            }
            fileType_t* file = malloc(sizeof(fileType_t));  
            if(isRegular(argv[i], &file->size)){
                file->name = argv[i];
                if(push(taskqueue, file) == -1){
                    perror("push");
                    exit(EXIT_FAILURE);
                }
            }else{
                perror("file non regolare");
                //gestire la terminazione delle cose tipo
            }
        }
        for (int i = 0; i < nThreads; i++) {
            pthread_join(workers[i], NULL);
        }
    }

    
    return 0;
}
