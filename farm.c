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

//#define SOCKNAME "farm.sck"
#define UNIX_PATH_MAX 108
#define EF(s, m)                                                               \
  if ((s) == -1) {                                                             \
    perror(m);                                                                 \
    exit(EXIT_FAILURE);                                                        \
  }

BQueue_t* taskqueue;
struct sockaddr_un sa;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
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

typedef struct Queue{
    resType_t res;
    struct Queue* next;
}Queue_t;

void cleanup() {
    unlink(SOCKNAME);
}

/**
 * @brief inserimento ordinato nella coda di risultati del collector
 * 
 * @param t puntatore alla testa della coda
 * @param val risultato da inserire
 */

void insert(Queue_t** t, resType_t val){
    Queue_t* newres = (Queue_t*)malloc(sizeof(Queue_t));
    newres->res = val;
    newres->next = NULL;

    Queue_t* curr = *t;
    Queue_t* prev = NULL;

    while(curr != NULL && curr->res.result > val.result){
        prev = curr;
        curr = curr->next;
    }

    if(prev == NULL){
        newres->next = *t;
        *t = newres;
    }
    else{
        prev->next = newres;
        newres->next = curr;
    }
    
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
    
    /* mando il risultato al collector tramite la socket*/
    int* fd_skt = (int*) args;
    size_t namelen = strlen(el);
    LOCK(&lock);
    if(writen((long)*fd_skt, &res, sizeof(long)) == -1){
        UNLOCK(&lock);
        perror("write result");
        exit(EXIT_FAILURE);
    }
    if(writen((long)*fd_skt, &namelen, sizeof(size_t)) == -1){
        UNLOCK(&lock);
        perror("write result");
        exit(EXIT_FAILURE);
    }
    if(writen((long)*fd_skt, &el, namelen) == -1){
        UNLOCK(&lock);
        perror("write result");
        exit(EXIT_FAILURE);
    }
    UNLOCK(&lock);

    printf("%ld\n", res);
    exit(EXIT_SUCCESS);
}

void processdir(char* dirpath){
    DIR* d;
    struct dirent* file;
}

int isRegular(const char filename[]){
    struct stat statbuf;

    if((stat(filename, &statbuf)) == 0){
        if(S_ISREG(statbuf.st_mode)){    
            return 1;
        }
    }
    return 0;
}

int isDir(const char filename[]){
    struct stat statbuf;

    if((stat(filename, &statbuf)) == 0){    
        if(S_ISDIR(statbuf.st_mode)) return 1;
    }
    else return 0;
}

int main(int argc, char *argv[]){
    if( argc < 2 ){
        printf("Uso: %s [file list]", argv[0]);
        return -1;
    }
    strncpy(sa.sun_path, SOCKNAME, UNIX_PATH_MAX);
    int fd_skt,
        nThreads = 4,
        qLen = 8,
        delay = 0,
        opt;

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
        /*--------MASTERWORKER--------*/
        
        fflush(stdout);
        taskqueue = initBQueue(qLen);
        EF((fd_skt = socket(AF_UNIX, SOCK_STREAM, 0)), "socket client");
        while(connect(fd_skt, (struct sockaddr*) &sa, sizeof(sa)) == -1){
            if(errno == ENOENT)
                sleep(1);
            else 
                exit(EXIT_FAILURE);
        }
        pthread_t* workers = malloc(nThreads*sizeof(pthread_t)); 
        //threadArgs_t args;      
        if(!workers){
            perror("malloc fallita");
            return -1;
        }
        for(int i = 0; i < nThreads; i++){
            fflush(stdout);
            if(pthread_create(&workers[i], NULL, &worker, &fd_skt) == -1){
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
                EF(push(taskqueue, argv[i]), "push");
                //gestire la terminazione delle cose?
            }else{
                perror("file non regolare");
                //gestire la terminazione delle cose tipo
            }
        }
        for (int i = 0; i < nThreads; i++) {
            pthread_join(workers[i], NULL);
        }
    }
    else{
        /*----------COLLECTOR----------*/
        int fd_c, notused, fd_skt;
        long res;
        char* filename;
        size_t namelen;
        EF(fd_skt = socket(AF_UNIX, SOCK_STREAM, 0), "socket");
        EF(notused = bind(fd_skt, (struct sockaddr *)&sa, sizeof(sa)), "bind");
        EF((notused = listen(fd_skt, SOMAXCONN)), "listen");
        EF((fd_c = accept(fd_skt, NULL, 0)), "accept");
        /*lettura dalla socket*/
        EF(readn(fd_c, &res, sizeof(long)), "read result");
        EF(readn(fd_c, &namelen, sizeof(size_t)), "read name lenght");
        filename = malloc(namelen + 1);
        EF(readn(fd_c, &filename, namelen+1), "read file name");

        printf("%s: %ld", filename, res);
        fflush(stdout);        

    }

    
    return 0;
}
