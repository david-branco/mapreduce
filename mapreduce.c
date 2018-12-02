#include<sys/types.h>
#include <signal.h>
#include<sys/wait.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<unistd.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<wordexp.h>

#define MAXL 1000
#define MAXCOM 1000

typedef struct sPares {
	char *chave;
	char *valor;
	struct sPares *next;
}*Pares,NodoPares;

typedef struct sPares2Red {
	char *chave;
	char *valores[MAXL];
	struct sPares2Red *next;
}*Pares2Red, NodoPares2Red;


void mata_zombies()
{
	int stat ;

	signal( SIGCHLD, mata_zombies ) ;
	wait( &stat ) ;
}


char** getArgs(const char* s)
{
    static wordexp_t words;
    static int initialized = 0;
    
    if(s && !initialized)
    {
        wordexp(s, &words, 0);
        initialized = 1;
    }
    else if(s)
    {
        // Reutilizar a memória que já tinha sido alocada previamente
        wordexp(s, &words, WRDE_REUSE);
    }
    else
    {
        // Libertar a memória quando s == NULL
        wordfree(&words);
        //initialized == 0;
        return NULL;
    }
    
    return words.we_wordv;
}

Pares init() {
	Pares paraux;
	paraux=(Pares) malloc(sizeof(NodoPares));		
	paraux->chave=NULL;
	paraux->valor=NULL;
	paraux->next=NULL;
return paraux;
}

Pares2Red init2red() {
	Pares2Red paraux;
	paraux=(Pares2Red) malloc(sizeof(NodoPares2Red));		
	paraux->chave=NULL;
	paraux->valores[0]=NULL;
	paraux->next=NULL;
return paraux;
}	

Pares insere_pares (char* ch,char* val, Pares input){

	if (input==NULL){

	Pares paraux=init();
	
	paraux->chave = strdup(ch);
	paraux->valor = strdup(val);
	
	input = paraux;
	}
	else input->next = insere_pares (ch,val,input->next);
	

	return(input);
}
						

Pares2Red insere_pares2red (char* ch,char* val,Pares2Red pr){
	
	int i;
	if (pr==NULL){

	Pares2Red paraux=init2red();
	
	paraux->chave = strdup(ch);
	paraux->valores[0]=strdup(val);
	pr=paraux;
	}
	else if(pr->chave==NULL) {
		pr->chave = strdup(ch);
		pr->valores[0]=strdup(val);
	}
	else if(strcmp(pr->chave,ch)==0) {
		i=0;
		while(pr->valores[i]!=NULL && i<MAXL) i++;
		pr->valores[i]=strdup(val);
	}
	else pr->next=insere_pares2red(ch,val,pr->next);

	return pr;
}


Pares2Red group(Pares p) {
	
	Pares2Red pr=NULL;

	if(p==NULL) pr=init2red();
		else 
			while(p) {
				pr=insere_pares2red(p->chave,p->valor,pr);
				p=p->next; 
			}
	return pr;
}		

	

void showPares(Pares p) {
	if(p==NULL) printf("TA VAZIA\n");
		else {
		printf("TEM QQ COISA\n");
		while(p!=NULL) {
			printf("Chave: %s - Valor: %s\n",p->chave,p->valor);
			p=p->next;
		}
		}
}

void showPares2Red(Pares2Red pr) {
	int i;
	if(pr==NULL) printf("TA VAZIA\n");
		else {
		printf("TEM QQ COISA\n");
		while(pr!=NULL) {
			printf("Chave: %s\n",pr->chave);
			i=0;
			while(pr->valores[i]!=NULL) {
				printf("Valor: %s\n",pr->valores[i]);
				i++;
			}
		pr=pr->next;
		}
		}
}


void input() {
	int fd = open("texto.txt",O_WRONLY | O_TRUNC | O_CREAT,0666);
	char c;
	while(read(0,&c,1))
		write(fd,&c,1);
	close(fd);
}


Pares ler_linhas() {
	
	char *buff,buff2[MAXL];
	int f2 = open("texto.txt",O_RDONLY,0666);
	Pares p=NULL;
	
	read(f2,buff2,MAXL);
	buff=strtok(buff2,"\n");
	while(buff!=NULL) {
		if(strtok(NULL,"\n")==NULL) break;
		p=insere_pares(buff,buff,p);	
		buff=strtok(NULL,"\n");
	}

close(f2);
return p;
}

void map(Pares p, char * cmd) {
 	
 int i,j,pid;
 int *filhos;
 
 filhos = malloc(MAXCOM * sizeof(int));
 
	int fd = open("pares.txt",O_WRONLY | O_TRUNC | O_CREAT, 0666);
	
	for(j=0,p;p!=NULL;j++,p=p->next) {
	
	pid=fork();

	if(pid==0) {
		dup2(fd, 1);
		close(fd);
   		execlp(cmd,cmd,p->valor,NULL);
		exit(1);
    	}
	else filhos[j]=pid;
		
	}
 
 //for(i=0;i<j;i++) { int st; wait(&st); }
  
 for(i=0;i<j;i++) {
 int st;
 while(-1==waitpid(filhos[i],&st,0));
	if(!WIFEXITED(st)) {
		printf("Erro a espera do pid\n");
		exit(1);
	}
 }
 free(filhos);
 
}


Pares ler_pares() {
	
	char *ch,*val,buff2[MAXL];
	int f2 = open("pares.txt",O_RDONLY,0666);
	Pares p=NULL;
	
	read(f2,buff2,MAXL);
	ch=strtok(buff2,"\t");
	val=strtok(NULL,"\n");
	while(ch!=NULL && val!=NULL) {
		p=insere_pares(ch,val,p);	
		ch=strtok(NULL,"\t");
		val=strtok(NULL,"\n");
	}
close(f2);
return p;
}


void reduce(Pares2Red pr, char *cmd) {
	
 int i,pi2[2],pi[2],pid,pid2,t,j;

 char buff[MAXL],buff2[MAXL];;
 char **cmds=NULL;
 cmds=getArgs(cmd); 
 
 signal(SIGCHLD,mata_zombies) ;

for(j=0,pr;pr!=NULL;j++,pr=pr->next) {

	pipe(pi);
	
	pid=fork();
	if(pid==0) {
			pipe(pi2);
			pid2=fork();
			if(pid2==0) {
				close(pi2[1]);
				dup2(pi2[0],0);
				dup2(pi[1],1);
				close(pi2[0]);
				execvp(cmds[0],cmds);
			}
			else {
				close(pi2[0]);
				dup2(pi2[1],1);	
				for(i=0;pr->valores[i]!=NULL;i++) {		
					strcat(pr->valores[i],"\n");
					write(pi2[1],pr->valores[i],sizeof(char)*strlen(pr->valores[i]));
				}
				close(pi2[1]);
				exit(1);		
			}
    	}
  	else {
		close(pi[1]);
		t=read(pi[0],buff,MAXL);
		buff[t-1]='\0';
		sprintf(buff2,"%s %s\n",pr->chave,buff);
		close(pi[0]);
		write(1,buff2,strlen(buff2));
	}

for(i=0;i<j;i++) { int st; wait(&st); }
 
 }
}


int main(int argc, char* argv[]){
	
	Pares p=NULL;	
	Pares2Red pr=NULL;	
	
	
	input();	
	p=ler_linhas();
	map(p,argv[1]);
	p=ler_pares();
	//showPares(p);
	pr=group(p);
	//showPares2Red(pr);
	reduce(pr,argv[2]);
	free(p);
	free(pr);
	remove("texto.txt");
	remove("pares.txt");	

return 1;	
}
