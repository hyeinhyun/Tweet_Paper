#include <rocksdb/c.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#define DATABASE "Rock_bag_man(4).db"
#include <assert.h>
typedef struct _kvs_tweet_t
{
  char id[2048];
  char content[4096];

} kvs_tweet_t;





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
  //printf("fopen success! \n");

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
	 //printf("%s\n",str);
	  memset(str,0,len);
	  str=rocksdb_iter_value(iter,&len);
   // printf("%s\n",str);
	  //printf(str);
	  memset(str,0,len);

	  i++;
	 }

 
      rocksdb_iter_destroy(iter);

      gettimeofday( &endTime, NULL );

      operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
      -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

      //printf("%f\n",operating_time);
    rocksdb_close(db);
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


  //printf("destroy success\n");

  operating_time = (double)(endTime.tv_sec)+(double)(endTime.tv_usec)/1000000.0
  -(double)(startTime.tv_sec)-(double)(startTime.tv_usec)/1000000.0;

  //printf("%f\n",operating_time);
  return operating_time;


}




//#3


//#4




//#5



int main() {

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

    char b_file[1024];
    printf("%f\n",operating_time);
    printf("enter batch filename:");
    scanf(" %[^\n]", &b_file);

    double batch_put=0;
    double batch_get=0;
    double batch_delete=0;


    //
    for(int i=0;i<100; i++){

    batch_put=batch_put+Insert_BatchData(options,b_file);
    batch_get=batch_get+Search_BatchData(options);

    //batch_delete
    //double batch_delete=0;
    batch_delete=batch_delete+Delete_BatchData(options);
    system("./cache.sh");
      }
      printf("Average of put_batchdata is : %f\n",batch_put/100);
      printf("Average of get_batchdata is : %f\n",batch_get/100);
      printf("Average of delete_batchdata is : %f\n",batch_delete/100);




}
