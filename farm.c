#include "util.h"
#include <stdio.h>
#include <stdlib.h>

//varaibili di stato modificate dal signal handler
static volatile sig_atomic_t usr1 = 0;
static volatile sig_atomic_t sign = 0;
//delay e numero di thread generati default
static int nThreads = 4;
static int delay = 0;
//coda condivisa dai thread in cui il master mette i file da elaborare
BQueue_t* taskqueue = NULL;
//indirizzo della socket
struct sockaddr_un addr;
//lock usata dai thread per la comunicazione con il server
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

//tipo risultato che usa il collector nella sua coda di risultati
typedef struct resType{
    char* filename;
    long result;
} resType_t;

//tipo coda che utilizza il Collector
typedef struct Queue{
    resType_t res;
    struct Queue* next;
}Queue_t;

//cleanup impostata atexit
static void cleanup() {
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
 * @brief funzione che viene chiamata alla fine del master (dopo che sono stati elaborati tutti i file o all'arrivo di un segnale != SIGUSR1) 
            manda la terminazione ai thread, successivamente fa la join e chiude la connessione con il server e aspetta che termini
 * 
 * @param workers array di thread worker
 * @param fd_skt socket di comunicazione con il server
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
    int status;
    wait(&status);
    exit(EXIT_SUCCESS);

}
/**
 * @brief inserimento ordinato nella coda di risultati del collector
 * 
 * @param t puntatore alla testa della coda
 * @param val risultato da inserire
 */
static void insert(Queue_t** t, resType_t val){
    Queue_t* newres = (Queue_t*)malloc(sizeof(Queue_t));
    if(newres == NULL){
        perror("malloc newres");
        exit(EXIT_FAILURE);
    }
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


static void printQueue(Queue_t** t){
    Queue_t* curr = *t;
    while(curr != NULL){
        printf("%ld %s\n", curr->res.result, curr->res.filename);
        curr = curr->next;
    }
    fflush(stdout);
}

static void freeQueue(Queue_t** t){
    if(*t == NULL) return;
    while(*t != NULL){
        Queue_t *tmp = *t;
        *t = (*t)->next;
        free(tmp->res.filename);
        free(tmp); 
    }
}


/**
 * @brief funzione eseguita dai thread
 * 
 * @param args socket per la comunicazione con il collector
 */
static void *worker(void* args){
    fflush(stdout);
    struct stat statbuf;
    size_t size;
    long res = 0;
    int n_long;
    int* fd_skt = (int*) args;
    
    while(1){
        //pop dalla coda dei task dell'elemento su cui operare
        char* el = pop(taskqueue);
        if(el == NULL){
            printf("pop\n");
            pthread_exit(NULL);
        }
        //"fine" è il messaggio che sono finiti i file quindi esco
        if(strcmp(el, "fine") == 0){
            break;
        }        
        //controllo se il file è regolare
        if(stat(el, &statbuf) == 0){
            size = statbuf.st_size;
            if(!S_ISREG(statbuf.st_mode)){
                pthread_exit(NULL);
            }
        }

        FILE* file; 
        //size è la lunghezza del file in byte quindi ci sono size/8 long (long 8 byte)  
        n_long = size/8;
        long buff[n_long];
        if((file = fopen(el, "r")) == NULL){
            perror("fopen");
            pthread_exit(NULL);
        }
        //leggo tutto il file e lo metto nel buffer
        if(fread(buff, sizeof(long), n_long, file) == 0){
            printf("%s\n", el);
            perror("fread");
            fclose(file);
            pthread_exit(NULL);
        }

        //operazione i*file[i]
        for(int i = 0; i < n_long; i++){
            res += i*buff[i];
        }
        
        /* mando il risultato al collector tramite la socket nel formato risultato, lunghezza nome file, nome file*/
        int namelen = strlen(el);

        LOCK(&lock);
        if(writen((long)*fd_skt, &res, sizeof(long)) == -1){
            UNLOCK(&lock);
            perror("write result");
            pthread_exit(NULL);
        }

        if(writen((long)*fd_skt, &namelen, sizeof(int)) == -1){
            UNLOCK(&lock);
            perror("write result");
            pthread_exit(NULL);
        }

        if(writen((long)*fd_skt, (void*)el, sizeof(char)*namelen) == -1){
            UNLOCK(&lock);
            perror("write result");
            pthread_exit(NULL);
        }
        UNLOCK(&lock);
        fclose(file);
        res = 0;
        //libero il puntatore all'elemento una volta finito di lavorarci
        if(el!= NULL){
            free(el);
        } 
        
    }
    pthread_exit(NULL);
}

/**
 * @brief funzione che processa la directory passata come argomento
 * 
 * @param dirpath path della directory
 */
static void processdir(const char* dirpath){
    DIR* d = opendir(dirpath);
    struct dirent* file;
    if(d == NULL){
        perror("apertura dir");
        return;
    }
    
    //controlla ogni elemento della directory
    while((errno = 0, file = readdir(d)) != NULL){
        if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
            continue;
        }

        //aggiorna il path dell'elemento che diventa path_corrente/nomefile
        char path[255];
        if((strlen(file->d_name) + strlen(dirpath) + 1) < 255){
            strncpy(path, dirpath, 255);
            if (path[strlen(path) - 1] != '/') {
                    strncat(path, "/", 255 - 1);
                }
            strncat(path, file->d_name, 255-(strlen(dirpath)+1));
            strncat(path, "\0", 1);
        }else{
            printf("stringa path troppo lunga");
            return;
        }

        //se l'elemento della directory è un'altra directory richiamo la funzione ricorsivamente con il nuovo path
        if (file->d_type == DT_DIR) {
            processdir(path);
        } else {
            //altrimenti è un file quindi alloco un puntatore per il path del file e lo metto nella coda
            char* pushFile = (char*)malloc((strlen(path)+1));
            if(pushFile == NULL){
                perror("malloc pushFile");
                exit(EXIT_FAILURE);
            }
            strncpy(pushFile, path, strlen(path)+1);
            push(taskqueue, (void*)pushFile);
            sleep(delay);
        }
    }
    closedir(d);
}

static int isRegular(const char filename[]){
    struct stat statbuf;

    if((stat(filename, &statbuf)) == 0){
        if(S_ISREG(statbuf.st_mode)){    
            return 1;
        }
    }
    return 0;
}

static int isDir(const char filename[]){
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
    
    unlink(SOCKNAME);

    //inizializzo l'indirizzo a "farm.sck"
    strncpy(addr.sun_path, SOCKNAME, UNIX_PATH_MAX);
    addr.sun_family = AF_UNIX;
    atexit(cleanup);

    int fd_skt,
        qLen = 8,
        opt;
    
    char* dir = NULL;
    //creo la maschera per i segnali da gestire
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
    EF(pthread_sigmask(SIG_SETMASK, &oldmask, NULL), "ripristinare maschera"); //ripristino la maschera

    //prendo le opzioni passate da riga di comando con getopt
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
    int pid;
    EF(pid = fork(), "fork");
    if(pid != 0){
        /*--------MASTERWORKER--------*/
        //blocco SIGUSR1
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
        //inizializzo la socket e mi connetto al server
        EF((fd_skt = socket(AF_UNIX, SOCK_STREAM, 0)), "socket client");
        while(connect(fd_skt, (struct sockaddr*) &addr, sizeof(addr)) == -1){
            if(errno == ENOENT || errno == ECONNREFUSED)
                sleep(1);
            else{ 
                perror("socket connect");
                exit(EXIT_FAILURE);
            }
        }
        //inizializzo i thread
        pthread_t workers[nThreads];
        for(int i = 0; i < nThreads; i++){
            fflush(stdout);
            if(pthread_create(&workers[i], NULL, &worker, &fd_skt) == -1){
                perror("thread create");
                exit(EXIT_FAILURE);
            }
            
        }
        //se è stata passata una directory come argomento la processo
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
        //quando getopt ritorna -1, optdint punta al primo elemento che non è un'opzione quindi se optind != 0 
        //significa che ci sono dei file da inserire nella coda
        if(optind != 0){
            for(int i = optind; i < argc; i++){
                //ad ogni iterazione controlla se è arrivato un segnale
                if(sign) master_exit(workers, &fd_skt);
                //se è il nome di un file regolare alloca un puntatore e lo inserisce nella coda
                if(isRegular(argv[i])){
                    char* file = (char*)malloc(strlen(argv[i])+1);
                    if(file == NULL){
                        perror("malloc nome file");
                        exit(EXIT_FAILURE);
                    }
                    strncpy(file, argv[i], strlen(argv[i])+1);
                    EF(push(taskqueue, (void*)file), "push");
                    sleep(delay);
                }else{
                    perror("file non regolare");
                }
            }
        }

        master_exit(workers, &fd_skt);
        
    }
    else{
        /*----------COLLECTOR----------*/
        //maschero tutti i segnali tranne SIGUSR1
        sigset_t c_mask;
        sigemptyset(&c_mask);
        sigaddset(&c_mask, SIGINT); 
        sigaddset(&c_mask, SIGHUP);
        sigaddset(&c_mask, SIGQUIT);
        sigaddset(&c_mask, SIGTERM);
        EF(pthread_sigmask(SIG_BLOCK, &c_mask, NULL), "maschera collector");

        Queue_t* codaRes = NULL;
        resType_t result;
        int fd_c, notused;
        long res;
        char* filename;
        int namelen;
        int serv_sock;
        
        //inizializzo la socket bindo l'indirizzo mi metto in ascolto e accetto la connessione con il client
        EF(serv_sock = socket(AF_UNIX, SOCK_STREAM, 0), "socket");
        EF(notused = bind(serv_sock, (struct sockaddr *)&addr, sizeof(addr)), "bind server");
        EF((notused = listen(serv_sock, SOMAXCONN)), "listen");
        EF((fd_c = accept(serv_sock, NULL, 0)), "accept");

        //lettura dalla socket
        while(1){
            //ad ogni iterazione controllo usr1
            if(usr1){
                printQueue(&codaRes);
                usr1 = 0;
            }
            //legge prima il long contenente il risultato
            EF(readn(fd_c, &res, sizeof(long)), "read result");
            //significa che ha finito
            if(res == -1) break;

            EF(readn(fd_c, &namelen, sizeof(int)), "read name lenght");
            filename = malloc(sizeof(char)*(namelen + 1));
            if(filename == NULL){
                perror("malloc nome file");
                exit(EXIT_FAILURE);
            }
            EF((readn(fd_c, (void*)filename, namelen*sizeof(char))), "read file name");
            filename[namelen] = '\0';
            //inserisce il risulato nella coda
            result.filename = filename;
            result.result = res;
            insert(&codaRes, result);
        }
        printQueue(&codaRes);    
        fflush(stdout);
        shutdown(fd_c, SHUT_RDWR);
        EF(close(fd_c), "server close fd_c");
        EF(close(serv_sock), "server close serv_sock");
        freeQueue(&codaRes);
        exit(EXIT_SUCCESS);
    }
    exit(EXIT_SUCCESS);
}
