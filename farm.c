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
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include "conn.h"
#include "util.h"

#define SOCKNAME "farm.sck"
#define UNIX_PATH_MAX 108

BQueue_t* taskqueue;
struct sockaddr_un sa;

//socket per comunicazione con Collector, passo il socket ai thread? oppure struttura con riferimento alla coda e alla socket 

/*typedef struct threadArgs{
    BQueue_t* q;
    //socket?    
}threadArgs_t;*/

/*typedef struct fileType{
    char* name;
    size_t size;
}fileType_t;*/

typedef struct resType{
    char* filename;
    long result;
} resType_t;

void cleanup() {
    unlink(SOCKNAME);
}

static void *worker(void* args){
    fflush(stdout);
    struct stat statbuf;
    size_t size;
    long res;
    int n_long;
    //BQueue_t *q = ((threadArgs_t*)args)->q;
    
    char* el = pop(taskqueue);
    if(stat(el, &statbuf) == 0){
        size = statbuf.st_size;
    }
    if(el == NULL){
        printf("pop\n");
        exit(EXIT_FAILURE);
    }
    FILE* file; 
    n_long = size/8;
    long buff[n_long];
    if((file = fopen(el, "r")) == NULL){
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
    r.filename = el;
    r.result = res;
    printf("%ld\n", res);
}

void processdir(char* dirpath){
    DIR* d;
    struct dirent* file;
}

int isRegular(const char filename[]){
    struct stat statbuf;

    if(stat(filename, &statbuf) == 0){
        if(S_ISREG(statbuf.st_mode)){    
            return 1;
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
    strncpy(sa.sun_path, SOCKNAME, UNIX_PATH_MAX);
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
            /*if(argv[i][1] == 'd'){
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
            }*/
            if(isRegular(argv[i])){
                if(push(taskqueue, argv[i]) == -1){
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
    }else{
        /*----------COLLECTOR----------*/
        int fd_skt, fd_c, notused;
        if((fd_skt = socket(AF_UNIX, SOCK_STREAM, 0)) == -1 ){
            perror("creazione socket");
            exit(EXIT_FAILURE);
        }

        if((notused = bind(fd_skt, (struct sockaddr *)&sa, sizeof(sa))) == -1){
            perror("bind");
            exit(EXIT_FAILURE);
        }

        if((notused = listen(fd_skt, SOMAXCONN)) == -1){
            perror("listen");
            exit(EXIT_FAILURE);
        }

        if((fd_c = accept(fd_skt, NULL, 0)) == -1){
            perror("accept");
            exit(EXIT_FAILURE);
        }
    }

    
    return 0;
}
