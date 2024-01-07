#include "boundedqueue.h"
#include <bits/getopt_core.h>
#include <bits/types/sigset_t.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <signal.h>
#include "conn.h"
#include "util.h"

//#define SOCKNAME "farm.sck"
#define UNIX_PATH_MAX 108
#define EF(s, m)                                                               \
  if ((s) == -1) {                                                             \
    perror(m);                                                                 \
    exit(EXIT_FAILURE);                                                        \
  }
static volatile sig_atomic_t usr1 = 0;
static volatile sig_atomic_t sign = 0;
static int nThreads = 4;

BQueue_t* taskqueue = NULL;
struct sockaddr_un addr;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
int delay = 0;
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
    pthread_mutex_destroy(&lock);
    unlink(SOCKNAME);
}

static void sighandler(int sig){
    switch(sig){
        case SIGUSR1: {
            usr1 = 1;
            break;
        }
        default: sign = 1;
    }
}

/**
 * @brief inserimento ordinato nella coda di risultati del collector
 * 
 * @param t puntatore alla testa della coda
 * @param val risultato da inserire
 */
static void master_exit(pthread_t workers[], int* fd_skt){
    char end[] = "fine";
    for(int i = 0; i < nThreads; i++){
        EF(push(taskqueue, (void*)end), "push end");
    }

    for (int i = 0; i < nThreads; i++) {
            pthread_join(workers[i], NULL);
    }
    long res = -1;
    if(writen((long)*fd_skt, (void*)&res, sizeof(long)) == -1){
        perror("write exit");
        exit(EXIT_FAILURE);
    }
    shutdown(*fd_skt, SHUT_RDWR);
    EF(close(*fd_skt), "socket close");
    deleteBQueue(taskqueue, NULL);
        //unlink(SOCKNAME);
    int status;
    wait(&status);
    exit(EXIT_SUCCESS);

}
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
    long res = 0;
    int n_long;
    int* fd_skt = (int*) args;
    
    //BQueue_t *q = ((threadArgs_t*)args)->q;
    while(1){
        char* el = pop(taskqueue);
        
        //printf("%s\n", el);
        //fflush(stdout);
        if(el == NULL){
            printf("pop\n");
            return (void*)EXIT_FAILURE;
        }

        if(strcmp(el, "fine") == 0){
            return EXIT_SUCCESS;
        }        
        
        if(stat(el, &statbuf) == 0){
            size = statbuf.st_size;
            if(!S_ISREG(statbuf.st_mode)){
                break;
            }
        }


        FILE* file; 
        n_long = size/8;
        long buff[n_long];
        if((file = fopen(el, "r")) == NULL){
            printf("%s", el);
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
        if(el!= NULL){
            free(el);
        } 
        
    }
    
    //printf("%ld\n", res);
    return (void*)EXIT_SUCCESS;
}


void processdir(const char* dirpath){
    DIR* d = opendir(dirpath);
    struct dirent* file;
    if(d == NULL){
        perror("apertura dir");
        return;
    }

    while((errno = 0, file = readdir(d)) != NULL){
        if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
            continue; // Skip "." and ".." entries
        }

        char path[255];
        if((strlen(file->d_name) + strlen(dirpath) + 1) < 255){
            strncpy(path, dirpath, 255);
            if (path[strlen(path) - 1] != '/') {
                    strncat(path, "/", 255 - 1);
                }
            //strncat(path, "/", 2);
            //printf("file-> d_name: %ld\n", strlen(file->d_name));
            //printf("dirpath: %ld\n", strlen(dirpath));
            strncat(path, file->d_name, 255-(strlen(dirpath)+1));
            strncat(path, "\0", 1);
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
            //printf("calloc: %ld\n", strlen(path));
            char* pushFile = (char*)malloc((strlen(path)+1));

            strncpy(pushFile, path, strlen(path)+1);
            push(taskqueue, (void*)pushFile);
            sleep(delay);
        }
    }
    closedir(d);
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

void freeChar(void* el){
    char* tmp = (char*)el;
    free(tmp);
}


int main(int argc, char *argv[]){
    if( argc < 2 ){
        printf("Uso: %s [file list]", argv[0]);
        return -1;
    }
    strncpy(addr.sun_path, SOCKNAME, UNIX_PATH_MAX);
    addr.sun_family = AF_UNIX;
    atexit(cleanup);

    int fd_skt,
        qLen = 8,
        //delay = 0,
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
            delay = atoi(optarg)/1000;
            break;   
        case 'd':
            dir = optarg;
            break;      
        default:
            break;
        }
    }
    sigset_t mask, oldmask;
    sigemptyset(&mask);        // resetto tutti i bits
    sigaddset(&mask, SIGINT); 
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGQUIT);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGUSR1);

    EF(pthread_sigmask(SIG_BLOCK, &mask, &oldmask), "pthread sigmask"); //maschero i segnali fino all'istallazione dell'handler
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sighandler;
    sa.sa_mask = mask;
    EF(sigaction(SIGINT, &sa, NULL), "handler install");
    EF(sigaction(SIGHUP, &sa, NULL), "handler install");
    EF(sigaction(SIGQUIT, &sa, NULL), "handler install");
    EF(sigaction(SIGTERM, &sa, NULL), "handler install");
    EF(sigaction(SIGUSR1, &sa, NULL), "handler install");
    EF(pthread_sigmask(SIG_SETMASK, &oldmask, NULL), "ripristinare maschera");
    int pid;
    EF(pid = fork(), "fork");
    if(pid != 0){
        /*--------MASTERWORKER--------*/
        sigset_t mw_mask;
        sigemptyset(&mw_mask);
        sigaddset(&mw_mask, SIGUSR1);
        EF(pthread_sigmask(SIG_BLOCK, &mw_mask, NULL), "maschera mw");
        fflush(stdout);
        taskqueue = initBQueue(qLen);
        if(!taskqueue){
            perror("initBqueue");
            exit(EXIT_FAILURE);
        }
        EF((fd_skt = socket(AF_UNIX, SOCK_STREAM, 0)), "socket client");
        while(connect(fd_skt, (struct sockaddr*) &addr, sizeof(addr)) == -1){
            if(errno == ENOENT)
                sleep(1);
            else{ 
                perror("socket connect");
                exit(EXIT_FAILURE);
            }
        }
        
        pthread_t workers[nThreads];
        for(int i = 0; i < nThreads; i++){
            fflush(stdout);
            if(pthread_create(&workers[i], NULL, &worker, &fd_skt) == -1){
                perror("thread create");
                exit(EXIT_FAILURE);
            }
            
        }
        if(dir != NULL){
            if(isDir(dir)){
                if(sign) master_exit(workers, &fd_skt);
                processdir(dir);
            }
            else{
                perror("directory non valida");
                exit(EXIT_FAILURE);
            }
        }
        if(optind != 0){
            for(int i = optind; i < argc; i++){
                if(sign) master_exit(workers, &fd_skt);
                if(isRegular(argv[i])){
                    char* file = (char*)malloc(strlen(argv[i])+1);
                    strncpy(file, argv[i], strlen(argv[i])+1);

                    //EF(push(taskqueue, (void*)argv[i]), "push");
                    EF(push(taskqueue, (void*)file), "push");
                    sleep(delay);
                    //gestire la terminazione delle cose?
                }else{
                    perror("file non regolare");
                    //gestire la terminazione delle cose tipo
                }
            }
        }
       /* char end[] = "fine";
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
        shutdown(fd_skt, SHUT_RDWR);
        EF(close(fd_skt), "socket close");
        deleteBQueue(taskqueue, NULL);
        //unlink(SOCKNAME);
        exit(EXIT_SUCCESS);*/
        master_exit(workers, &fd_skt);
    }
    else{
        /*----------COLLECTOR----------*/
        Queue_t* codaRes = NULL;//malloc(sizeof(Queue_t));
        //codaRes = NULL;
        resType_t result;
        int fd_c, notused;
        long res;
        char* filename;
        int namelen;
        int a;
        int serv_sock;
        
        EF(serv_sock = socket(AF_UNIX, SOCK_STREAM, 0), "socket");
        EF(notused = bind(serv_sock, (struct sockaddr *)&addr, sizeof(addr)), "bind server");
        EF((notused = listen(serv_sock, SOMAXCONN)), "listen");
        EF((fd_c = accept(serv_sock, NULL, 0)), "accept");
        /*lettura dalla socket*/
        while(1){
            if(usr1){
                printQueue(&codaRes);
                usr1 = 0;
            }
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
        //fflush(stdout);     
       // printf("ao");  
        fflush(stdout);
        shutdown(fd_c, SHUT_RDWR);
        EF(close(fd_c), "server close fd_c");
       // EF(close(fd_skt), "server close fd_skt");
        //unlink(SOCKNAME);
        freeQueue(&codaRes);
        //free(codaRes);
        exit(EXIT_SUCCESS);

    }
    //unlink(SOCKNAME);

    
    exit(EXIT_SUCCESS);
}
