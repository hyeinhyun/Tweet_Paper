#include <rocksdb/c.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#define DATABASE "Rock_bag_man(TEST).db"
#include <assert.h>
#include <sys/types.h>
typedef struct _kvs_tweet_t
{
  char id[2048];
  char content[4096];

} kvs_tweet_t;



double Insert_Data(const rocksdb_options_t* options, char* key,char* value)
{
  rocksdb_t *db;
  rocksdb_writeoptions_t *woptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  memset(&emp.id,'\0',sizeof(emp.id));
  memset(&emp.content,'\0',sizeof(emp.content));
  double operating_time;

 strcpy(emp.id,key);
 strcpy(emp.content,value);

 // gettimeofday( &startTime, NULL );

  /* DB OPEN */
 db = rocksdb_open(options, DATABASE, &err);
 assert(!err);
//gettimeofday( &startTime, NULL );


  woptions = rocksdb_writeoptions_create();
gettimeofday( &startTime, NULL );
  rocksdb_put(db, woptions,&emp.id, strlen(emp.id)+1, &emp.content, strlen(emp.content)+1, &err);
gettimeofday(&endTime,NULL);
assert(!err);
rocksdb_close(db);
 // gettimeofday( &endTime, NULL );
  //assert(!err);
  rocksdb_writeoptions_destroy(woptions);


    //printf("  Record: '%s' -> '%s'\n",emp.id, emp.content);

    operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
    -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

    //printf("%f\n",operating_time);

//printf("ellapsed time %f second\n", operating_time);
//rocksdb_close(db);
return operating_time;


}

double Insert_BatchData(const rocksdb_options_t* options,char* file_name){
  rocksdb_t *db;
  const rocksdb_writeoptions_t *woptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;


  int  num, len;
  char *buffer;
  int size;
  int count=0;
  int total=0;
  int pos=0;

  memset(&emp.id,'\0',sizeof(emp.id));
  memset(&emp.content,'\0',sizeof(emp.content));
  gettimeofday( &startTime, NULL );

  /* DB OPEN */
  db = rocksdb_open(options, DATABASE, &err);
  assert(!err);

  woptions = rocksdb_writeoptions_create();

  //char buffer[4096];
  FILE *fp = fopen(file_name, "r");
  if(fp == NULL) {
      printf("file open error! \n");
      exit(0);
  }
 // printf("fopen success! \n");

  fseek(fp, 0, SEEK_END);

  size = ftell(fp);

  buffer=malloc(size+1);
  memset(buffer, 0, size + 1);

  fseek(fp, 0, SEEK_SET);
  count = fread(buffer, size, 1, fp);

  //printf("%s\n size: %d, count: %d\n", buffer, size, count);

  //// put one by one ////



  for (int i =0; i < size+1; i++) {
    switch(buffer[i]) {

	case '[': {
        char *kbegin = &buffer[i];
        char *kend = strchr(kbegin, ']');
        int kstringLength = kend - kbegin;

        memcpy(&emp.id, &buffer[i+1], kstringLength - 1);
        //printf("key: %s\n", emp.id);
      break;
      }
      case '{': {
        char *vbegin = &buffer[i];
        char *vend = strchr(vbegin, '}');
        int vstringLength = vend - vbegin;
        memcpy(&emp.content, &buffer[i+1], vstringLength - 1);
        //printf("value: %s\n", emp.content);

        rocksdb_put(db, woptions,&emp.id, strlen(emp.id)+1, &emp.content, strlen(emp.content)+1, &err);
        //printf(" Record: '%s' -> '%s'\n", emp.id, emp.content);
	memset(&emp.id,'\0',sizeof(emp.id));
	//printf("%s",emp.id);
	memset(&emp.content,'\0',sizeof(emp.content));

      break;
      }
    }
  }

  gettimeofday( &endTime, NULL );

  operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
  -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

  //printf("%f\n",operating_time);
rocksdb_close(db);
free(buffer);
return operating_time;




}

double Search_Data(const rocksdb_options_t* options,char* key){
  rocksdb_t *db;
    const rocksdb_readoptions_t *roptions;
    char *err = NULL;
    kvs_tweet_t emp;
    struct timeval startTime, endTime, gepTime;
    size_t read_len;
    char *read;
    char value[2048];
    read[4096];
    read_len=sizeof(read);
    double operating_time;
     //printf("\n");



   // memset(&emp.id,'\0',strlen(emp.id));
//memset(&emp.content,0,sizeof(emp.content));

   // printf(read);

    //
    // printf("Enter Key to get :");
    // printf("\n");
    // scanf("%s",&emp.id);
    strcpy(emp.id,key);
   // gettimeofday( &startTime, NULL );

    /* DB OPEN */
    db = rocksdb_open(options, DATABASE, &err);
    assert(!err);
//gettimeofday( &startTime, NULL );

    roptions = rocksdb_readoptions_create();
gettimeofday( &startTime, NULL );
    read=rocksdb_get(db, roptions,&emp.id,strlen(emp.id)+1,&read_len, &err);
   gettimeofday(&endTime,NULL);
    read[read_len] = '\0';
    free(read);
 //  gettimeofday(&endTime,NULL);
    assert(!err);
    //assert(!err);
rocksdb_close(db);
rocksdb_readoptions_destroy(roptions);
   // gettimeofday( &endTime, NULL );
  //  assert(!err);

   //   printf("Key:%s-Value: %s\n",emp.id, read);
      //printf("\n");

      //printf("\n");
    //memset(&emp.id,0,strlen(emp.id));
    //memset(&emp.content,0,sizeof(emp.content));






    operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
    -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

    //printf("%f\n",operating_time);
  //  rocksdb_close(db);
    return operating_time;


}
double Search_BatchData(const rocksdb_options_t* options){
  rocksdb_t *db;
  rocksdb_iterator_t *iter;
  rocksdb_readoptions_t *roptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;


  gettimeofday( &startTime, NULL );

  /* DB OPEN */
  db = rocksdb_open(options, DATABASE, &err);
  assert(!err);
  //int count=0;


  roptions = rocksdb_readoptions_create();
  iter = rocksdb_create_iterator(db, roptions);
  rocksdb_iter_seek_to_first(iter);
  int i;
  for(i=0;rocksdb_iter_valid(iter)!=0;rocksdb_iter_next(iter)){
	  //count=count+1;
	  char * str;
	  size_t len;
    assert(!err);
	  str=rocksdb_iter_key(iter,&len);
//	 printf("%s\n",str);
	  memset(str,0,len);
	  str=rocksdb_iter_value(iter,&len);
   // printf("%s\n",str);
	  //printf(str);
	  //memset(str,0,len);

	  i++;
	 }

 // for (i=0; rocksdb_iter_valid(iter)!=0; rocksdb_iter_next(iter))
   //   {
     //     size_t key_len, value_len;
       //   assert(!err);
	  //count=count+1;

	 //   char * key_ptr;
	   // key_ptr[2048];
           // key_ptr = rocksdb_iter_key(iter,&key_len);
	   // key_ptr[key_len] = '\0';
	   // char* value_ptr;
	   // value_ptr[4096];
           // value_ptr = rocksdb_iter_value(iter,&value_len);
	   // value_ptr[value_len]='\0';

           // printf("%s : %s\n", key_ptr, value_ptr);
 	    //memset(key_ptr,'\0',
	   // i++;


  // }
  //printf("%d\n",count);
      rocksdb_iter_destroy(iter);

      gettimeofday( &endTime, NULL );

      operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
      -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

   //   printf("%f\n",operating_time);
    rocksdb_close(db);
    return operating_time;



}

double Delete_Data(const rocksdb_options_t* options,char* key){
  rocksdb_t *db;
  rocksdb_writeoptions_t *woptions;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;

  //
  // printf("KEY DELETE:\n");
  // scanf("%s",&emp.id);
  strcpy(emp.id,key);
  //gettimeofday( &startTime, NULL );

  /* DB OPEN */
  db = rocksdb_open(options, DATABASE, &err);
  assert(!err);
//gettimeofday( &startTime, NULL );

    woptions = rocksdb_writeoptions_create();
 // gettimeofday( &endTime, NULL );
 gettimeofday( &startTime, NULL );
  rocksdb_delete(db,woptions,&emp.id,strlen(emp.id)+1,&err);
gettimeofday( &endTime, NULL );
  memset(&emp.id,0,sizeof(emp.id));
  memset(&emp.content,0,sizeof(emp.content));
  //gettimeofday(&endTime,NULL);
assert(!err);
rocksdb_writeoptions_destroy(woptions);
rocksdb_close(db);

  //gettimeofday( &endTime, NULL );

  //assert(!err);

  //printf("delete success\n");


      operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
      -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

      //printf("%f\n",operating_time);
//rocksdb_close(db);
return operating_time;


  }

double Delete_BatchData(const rocksdb_options_t* options){
  rocksdb_t *db;
  char *err = NULL;
  kvs_tweet_t emp;
  struct timeval startTime, endTime, gepTime;
  double operating_time;


  gettimeofday( &startTime, NULL );




  rocksdb_destroy_db(options,DATABASE,&err);

  gettimeofday( &endTime, NULL );

  assert(!err);


 // printf("destroy success\n");

  operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
  -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

 // printf("%f\n",operating_time);
  return operating_time;


}




//#3


//#4




//#5



int main() {
	//printf("here");
//	system("./cache.sh");
	//printf("here");
    rocksdb_options_t *options;
    char *err = NULL;
    //char *read;
    int user_choice;
    int count;
    double operating_time;
    char file_batch[100];
    char file_single[100];



    struct timeval startTime, endTime, gepTime;

    /******************************************/

    /*Create*/
    gettimeofday( &startTime, NULL );
    options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    gettimeofday( &endTime, NULL );

    operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
    -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

    //printf("%f\n",operating_time);

    //

    system("./cache.sh");

    double single_put=0;
    char s_file[1024];
    char k[2048];
    double first1=0;

    double first =0;
    char v[2048];
    char *pStr;
    char *line_p;

    long a=1000000000000000000;

   // printf("single put \n");
   // printf("Enter single filename: ");
    //scanf(" %[^\n]", &s_file);
    FILE *sfp = fopen("new_single_avg_medium.txt", "r");
    if(sfp == NULL) {
 printf("Fail! \n");
 exit(0);
    }
   // printf("sucess! \n");
    //printf("before for \n");
    for(int i=0;i<2; i++){
   // printf("Start \n");

    sprintf(k,"%ld", a+i );
    pStr=fgets(v, sizeof(v), sfp);
    pStr[strlen(pStr)-1]='\0';

    single_put=single_put+Insert_Data(options,k,pStr);
         // pStr[strlen(pStr)-1]='\0';
           if(i==0){
       printf("%f\n", single_put);
       first1=single_put;
    } else if(i==1) {
       printf("%f\n", (single_put-first1));
    }
    memset(k,0,sizeof(k));
 //   printf("Success %d\n",i);
    }

    fclose(sfp);
    system("./cache.sh");

    double single_get=0;
    char kk[2048];
    double first2=0;


       //printf("single get \n");
       for(int i=0;i<2; i++){
        //printf("%d\n",i);
    //	      printf("for for for for\n");
      //  a=a+1;
        sprintf(kk, "%ld", a+i);
    //	      printf("sprint all\n");
        //printf("%f\n",Search_Data(options,kk));
        single_get=single_get+Search_Data(options,kk);
        //printf("single get %d\n",i);
        if(i==0) {
           //printf("%s \n","jiji");
     printf("%f \n", single_get);
    // printf("helllllllllo");
     first2=single_get;
    // printf("heeeeeeeeeeel");
  } else if(i==1) {
     printf("%f\n", (single_get-first2));
        }
       // printf("End %d\n",i);
       }

       system("./cache.sh");
       double single_delete=0;
       char kkk[2048];
       double first3=0;

       //printf("single delete\n ");
        for(int i=0; i<2; i++){
        //a=a+1;
        sprintf(kkk, "%ld",a+i);
        single_delete=single_delete+Delete_Data(options,kkk);
        if(i==0) {
      printf("%f\n", single_delete);
      first3=single_delete;
    } else if(i==1) {
      printf("%f\n", (single_delete-first3));
        }

       }



//system("./cache.sh");

}
