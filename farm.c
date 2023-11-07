#include "boundedqueue.h"
#include <bits/getopt_core.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
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

    while(curr != NULL && curr->res.result < val.result){
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

void printQueue(Queue_t** t){
    Queue_t* curr = *t;
    while(curr != NULL){
        printf("%ld %s\n", curr->res.result, curr->res.filename);
        curr = curr->next;
    }
    fflush(stdout);
}

void freeQueue(Queue_t** t){
    if(*t == NULL) return;
    while(*t != NULL){
        Queue_t *tmp = *t;
        *t = (*t)->next;
        free(tmp->res.filename);
        free(tmp); 
    }
}



static void *worker(void* args){
    fflush(stdout);
    struct stat statbuf;
    size_t size;
    long res, end = -1;
    int n_long;
    int* fd_skt = (int*) args;
    
    //BQueue_t *q = ((threadArgs_t*)args)->q;
    while(1){
        char* el = pop(taskqueue);
        //printf("%s\n", el);
        //fflush(stdout);
        if(strcmp(el, "fine") == 0){
            return EXIT_SUCCESS;
        }

        
        if(stat(el, &statbuf) == 0){
            size = statbuf.st_size;
            if(!S_ISREG(statbuf.st_mode)){
                break;
            }
        }
        if(el == NULL){
            printf("pop\n");
            return (void*)EXIT_FAILURE;
        }

        FILE* file; 
        n_long = size/8;
        long buff[n_long];
        if((file = fopen(el, "r")) == NULL){
            perror("fopen");
            return (void*)EXIT_FAILURE;
        }
        if(fread(buff, sizeof(long), n_long, file) == 0){
            printf("%s\n", el);
            perror("fread");
            fclose(file);
            return (void*)EXIT_FAILURE;
        }

        for(int i = 0; i < n_long; i++){
            res += i*buff[i];
        }
        
        /* mando il risultato al collector tramite la socket*/
        int namelen = strlen(el);
        int a;
        LOCK(&lock);
        if(writen((long)*fd_skt, &res, sizeof(long)) == -1){
            UNLOCK(&lock);
            perror("write result");
            return (void*)EXIT_FAILURE;
        }

        if(writen((long)*fd_skt, &namelen, sizeof(int)) == -1){
            UNLOCK(&lock);
            perror("write result");
            return (void*)EXIT_FAILURE;
        }

        if(writen((long)*fd_skt, (void*)el, sizeof(char)*namelen) == -1){
            UNLOCK(&lock);
            perror("write result");
            return (void*)EXIT_FAILURE;
        }
        UNLOCK(&lock);
        //free(el);
        fclose(file);
        res = 0;
    }
    

    //printf("%ld\n", res);
    return EXIT_SUCCESS;
}

void processdir(const char* dirpath){
    DIR* d = opendir(dirpath);
    struct dirent* file;
    if(d == NULL){
        perror("apertura dir");
        return;
    }

    while((file = readdir(d)) != NULL){
        if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
            continue; // Skip "." and ".." entries
        }

        char path[255];
        if((strlen(file->d_name) + strlen(dirpath) + 1) < 255){
            strncpy(path, dirpath, 255);
            strncat(path, "/", 2);
            strncat(path, file->d_name, 255-(strlen(dirpath)+1));
        }else{
            printf("stringa path troppo lunga");
            return;
        }
        //snprintf(path, 255, "%s/%s", dirpath, file->d_name);
        //printf("%s: ", path);
        if (file->d_type == DT_DIR) {
            // If it's a directory, recurse into it
            processdir(path);
        } else {
            // If it's a file, print its name
            //printf("%s\n ", path);
            char* pushFile = malloc((strlen(path)+1) * sizeof(char));
            strncpy(pushFile, path, strlen(path)+1);

            push(taskqueue, (void*)pushFile);
        }
    }
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
    return 0;
}



int main(int argc, char *argv[]){
    if( argc < 2 ){
        printf("Uso: %s [file list]", argv[0]);
        return -1;
    }
    strncpy(sa.sun_path, SOCKNAME, UNIX_PATH_MAX);
    sa.sun_family = AF_UNIX;

    int fd_skt,
        nThreads = 4,
        qLen = 8,
        delay = 0,
        opt;
    
    char* dir = NULL;
    
    while((opt = getopt(argc, argv, "n:q:t:d:")) != -1){
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
        case 'd':
            dir = optarg;
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
        //pthread_t* workers = malloc(nThreads*sizeof(pthread_t)); 
        pthread_t workers[nThreads];
        //threadArgs_t args;      
        /*if(!workers){
            perror("malloc fallita");
            return -1;
        }*/
        for(int i = 0; i < nThreads; i++){
            fflush(stdout);
            if(pthread_create(&workers[i], NULL, &worker, &fd_skt) == -1){
                perror("thread create");
                exit(EXIT_FAILURE);
            }
            
        }
        if(dir != NULL){
            if(isDir(dir)){
                processdir(dir);
            }
            else{
                perror("directory non valida");
                exit(EXIT_FAILURE);
            }
        }
        if(optind != 0){
            for(int i = optind; i < argc; i++){
                if(isRegular(argv[i])){
                    EF(push(taskqueue, (void*)argv[i]), "push");
                    //gestire la terminazione delle cose?
                }else{
                    perror("file non regolare");
                    //gestire la terminazione delle cose tipo
                }
            }
        }
        char end[] = "fine";
        for(int i = 0; i < nThreads; i++){
            EF(push(taskqueue, (void*)end), "push end");
        }

        for (int i = 0; i < nThreads; i++) {
            pthread_join(workers[i], NULL);
        }
        long res = -1;
        if(writen((long)fd_skt, (void*)&res, sizeof(long)) == -1){
            perror("write exit");
            exit(EXIT_FAILURE);
        }
        close(fd_skt);
        deleteBQueue(taskqueue, NULL);
        free(taskqueue);
        unlink(SOCKNAME);
        exit(EXIT_SUCCESS);
    }
    else{
        /*----------COLLECTOR----------*/
        Queue_t* codaRes = malloc(sizeof(Queue_t));
        codaRes = NULL;
        resType_t result;
        int fd_c, notused;
        long res;
        char* filename;
        int namelen;
        int a;
        
        EF(fd_skt = socket(AF_UNIX, SOCK_STREAM, 0), "socket");
        EF(notused = bind(fd_skt, (struct sockaddr *)&sa, sizeof(sa)), "bind server");
        EF((notused = listen(fd_skt, SOMAXCONN)), "listen");
        EF((fd_c = accept(fd_skt, NULL, 0)), "accept");
        /*lettura dalla socket*/
        while(1){
            EF(readn(fd_c, &res, sizeof(long)), "read result");
            //printf("%ld\n", res);
            if(res == -1) break;
            EF(readn(fd_c, &namelen, sizeof(int)), "read name lenght");

            filename = malloc(sizeof(char)*(namelen + 1));
            EF((a = readn(fd_c, (void*)filename, namelen*sizeof(char))), "read file name");
            filename[namelen] = '\0';
            result.filename = filename;
            result.result = res;

            insert(&codaRes, result);
            //free(filename);
        }
        printQueue(&codaRes);    
        fflush(stdout);       
        close(fd_c);
        close(fd_skt);
        unlink(SOCKNAME);
        freeQueue(&codaRes);
        free(codaRes);

    }
    unlink(SOCKNAME);

    
    return 0;
}
